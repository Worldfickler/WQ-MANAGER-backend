"""API integration tests against an isolated SQLite database; no production writes."""
import os
import unittest
from datetime import datetime, timedelta
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import httpx
from fastapi import FastAPI, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.datastructures import Headers

os.environ.setdefault("SQL_ECHO", "False")

from app.api.v1.endpoints.submit_wait import router
from app.core.database import Base, get_db
from app.core.security import create_access_token
from app.models.submit_wait import (
    SubmitWaitFlower, SubmitWaitGroup, SubmitWaitImage, SubmitWaitMember,
    SubmitWaitPost, SubmitWaitVote, SubmitWaitWatch, SubmitWaitSticker, SubmitWaitVoteSticker,
)
from app.models.user import SystemUser
from app.services import minio_storage_service as storage
from app.services.submit_wait_service import build_image_url
from app.services.submit_wait_sticker_service import build_sticker_url


class SubmitWaitAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        tables = [model.__table__ for model in (SystemUser, SubmitWaitGroup, SubmitWaitMember,
                  SubmitWaitPost, SubmitWaitImage, SubmitWaitVote, SubmitWaitFlower, SubmitWaitWatch,
                  SubmitWaitSticker, SubmitWaitVoteSticker)]
        async with self.engine.begin() as connection:
            await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=tables))
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.sessions() as db:
            db.add_all([SystemUser(id=index, wq_id=f"TEST{index}", username=f"Test Researcher {index}",
                                  is_active=index != 4, delete_flag=False) for index in range(1, 5)])
            await db.commit()

        async def isolated_db():
            async with self.sessions() as db:
                try:
                    yield db
                    await db.commit()
                except Exception:
                    await db.rollback()
                    raise

        app = FastAPI()
        app.include_router(router, prefix="/submit-wait")
        app.dependency_overrides[get_db] = isolated_db
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        self.tokens = {index: create_access_token({"sub": f"TEST{index}"}) for index in range(1, 5)}

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.engine.dispose()

    async def request(self, method, path, user=1, **kwargs):
        headers = {"Authorization": f"Bearer {self.tokens[user]}"} if user else {}
        return await self.client.request(method, f"/submit-wait{path}", headers=headers, **kwargs)

    async def post(self, **overrides):
        response = await self.request("POST", "/posts", json={"title": "低换手 Alpha", "content": "Sharpe 2.1\n需要再验证子区间吗？", **overrides})
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()

    async def vote(self, post_id, user=2, decision="submit", reason="子区间比较稳定，建议提交"):
        return await self.request("POST", f"/posts/{post_id}/votes", user, json={"decision": decision, "reason": reason})

    async def sticker_fixture(self):
        async with self.sessions() as db:
            stickers = [SubmitWaitSticker(name=name, object_name=f"submit-wait/stickers/{index}.jpg",
                                          width=322, height=184, sort_order=index, active=index != 2)
                        for index, name in enumerate(("忍不住了", "赶紧交它一个", "已停用"))]
            db.add_all(stickers)
            await db.commit()
            return [sticker.id for sticker in stickers]

    async def test_sticker_catalog_requires_auth_and_only_lists_active_assets(self):
        ids = await self.sticker_fixture()
        self.assertEqual((await self.request("GET", "/stickers", user=None)).status_code, 403)
        self.assertEqual((await self.request("GET", "/stickers", user=4)).status_code, 401)
        items = (await self.request("GET", "/stickers", 2)).json()
        self.assertEqual([item["id"] for item in items], ids[:2])
        self.assertEqual(items[0]["name"], "忍不住了")
        self.assertEqual(items[0]["width"], 322)
        self.assertNotIn("object_name", items[0])
        self.assertTrue(items[0]["url"].startswith(f"/api/v1/submit-wait/stickers/{ids[0]}/image?ticket="))

    async def test_vote_stickers_persist_order_and_follow_result_permissions(self):
        ids = await self.sticker_fixture()
        post = await self.post()
        path = f"/posts/{post['id']}/votes"
        response = await self.request("POST", path, 2, json={"decision": "hold", "reason": "补充验证", "sticker_ids": ids[1::-1]})
        self.assertEqual(response.status_code, 201, response.text)
        self.assertEqual(response.json()["review_count"], 1)
        self.assertEqual((await self.request("GET", path, 3)).status_code, 403)
        for user in (1, 2):
            item = (await self.request("GET", path, user)).json()["items"][0]
            self.assertEqual(item["reason"], "补充验证")
            self.assertEqual([sticker["id"] for sticker in item["stickers"]], ids[1::-1])
        await self.request("POST", f"/posts/{post['id']}/watch", 3)
        self.assertEqual(len((await self.request("GET", path, 3)).json()["items"][0]["stickers"]), 2)
        self.assertEqual((await self.vote(post["id"], user=3)).status_code, 403)
        other = await self.post()
        await self.vote(other["id"])
        self.assertEqual((await self.request("GET", f"/posts/{other['id']}/votes")).json()["items"][0]["stickers"], [])

    async def test_invalid_stickers_do_not_create_vote_and_text_reason_is_still_required(self):
        ids = await self.sticker_fixture()
        post = await self.post()
        path = f"/posts/{post['id']}/votes"
        for chosen, expected in [([0], 422), ([-1], 422), ([ids[0], ids[0]], 422),
                                 ([1, 2, 3, 4], 422), ([99999], 400), ([ids[2]], 400)]:
            result = await self.request("POST", path, 2, json={"decision": "submit", "reason": "测试", "sticker_ids": chosen})
            self.assertEqual(result.status_code, expected, result.text)
        result = await self.request("POST", path, 2, json={"decision": "submit", "reason": "  ", "sticker_ids": [ids[0]]})
        self.assertEqual(result.status_code, 422)
        self.assertEqual((await self.request("GET", f"/posts/{post['id']}")).json()["review_count"], 0)
        self.assertEqual((await self.vote(post["id"])).status_code, 201)

    async def test_sticker_image_stream_and_ticket_guards(self):
        ids = await self.sticker_fixture()
        path = f"/submit-wait/stickers/{ids[0]}/image"
        for ticket in ("invalid", self.tokens[1],
                       parse_qs(urlsplit(build_image_url(ids[0], 1)).query)["ticket"][0],
                       parse_qs(urlsplit(build_sticker_url(ids[1], 1)).query)["ticket"][0],
                       create_access_token({"scope": "submit-wait-sticker", "sub": f"submit-wait-sticker:{ids[0]}",
                                            "sticker_id": ids[0], "viewer_id": 1}, expires_delta=timedelta(seconds=-1))):
            self.assertEqual((await self.client.get(path, params={"ticket": ticket})).status_code, 403)
        self.assertEqual((await self.client.get(build_sticker_url(ids[0], 4).removeprefix("/api/v1"))).status_code, 403)
        self.assertEqual((await self.client.get(build_sticker_url(99999, 1).removeprefix("/api/v1"))).status_code, 404)
        fake_login = create_access_token({"sub": "TEST1", "scope": "submit-wait-sticker"})
        self.assertEqual((await self.client.get("/submit-wait/posts", headers={"Authorization": f"Bearer {fake_login}"})).status_code, 401)
        source = MagicMock()
        source.headers = {"content-type": "image/jpeg"}
        source.stream.return_value = [b"\xff\xd8\xffsticker"]
        with patch.object(storage, "open_submit_wait_image", new=AsyncMock(return_value=source)) as read:
            result = await self.client.get(build_sticker_url(ids[0], 1).removeprefix("/api/v1"))
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.content, b"\xff\xd8\xffsticker")
        self.assertEqual(result.headers["content-type"], "image/jpeg")
        self.assertEqual(result.headers["cache-control"], "private, max-age=60")
        read.assert_awaited_once_with("submit-wait/stickers/0.jpg")
        source.close.assert_called_once()
        source.release_conn.assert_called_once()

    async def test_disabled_stickers_remain_in_historical_votes(self):
        ids = await self.sticker_fixture()
        post = await self.post()
        await self.request("POST", f"/posts/{post['id']}/votes", 2, json={"decision": "submit", "reason": "验证完成", "sticker_ids": [ids[0]]})
        async with self.sessions() as db:
            sticker = await db.get(SubmitWaitSticker, ids[0])
            sticker.active = False
            await db.commit()
        items = (await self.request("GET", "/stickers")).json()
        self.assertNotIn(ids[0], [item["id"] for item in items])
        vote = (await self.request("GET", f"/posts/{post['id']}/votes")).json()["items"][0]
        self.assertEqual(vote["stickers"][0]["id"], ids[0])

    async def test_requires_real_authentication(self):
        self.assertEqual((await self.request("GET", "/posts", user=None)).status_code, 403)
        self.assertEqual((await self.request("GET", "/posts", user=4)).status_code, 401)
        response = await self.client.get("/submit-wait/posts", headers={"Authorization": "Bearer invalid"})
        self.assertEqual(response.status_code, 401)

    async def test_anonymous_text_and_images_round_trip_without_identity_leak(self):
        async with self.sessions() as db:
            image = SubmitWaitImage(uploader_id=1, object_name="submit-wait/random.png")
            db.add(image); await db.commit()
            image_id = image.id
        post = await self.post(image_ids=[image_id])
        self.assertTrue(post["is_owner"])
        response = (await self.request("GET", f"/posts/{post['id']}", user=2)).json()
        self.assertEqual(response["author_name"], "匿名研究员")
        self.assertNotIn("author_id", response)
        self.assertNotIn("TEST1", str(response))
        self.assertNotIn("Test Researcher 1", str(response))
        self.assertEqual(response["content"], "Sharpe 2.1\n需要再验证子区间吗？")
        self.assertEqual(response["images"][0]["id"], image_id)
        self.assertTrue(response["created_at"].endswith("Z"))

    async def test_named_author_is_visible(self):
        post = await self.post(anonymous=False)
        self.assertEqual(post["author_name"], "Test Researcher 1")

    async def test_results_are_hidden_in_detail_list_and_votes_until_voting(self):
        post = await self.post()
        self.assertEqual((await self.vote(post["id"])).status_code, 201)
        detail = (await self.request("GET", f"/posts/{post['id']}", user=3)).json()
        feed = (await self.request("GET", "/posts", user=3)).json()["items"][0]
        for item in (detail, feed):
            self.assertFalse(item["results_visible"])
            self.assertIsNone(item["submit_count"])
            self.assertIsNone(item["hold_count"])
            self.assertEqual(item["review_count"], 1)
        self.assertEqual((await self.request("GET", f"/posts/{post['id']}/votes", user=3)).status_code, 403)
        reviewed = (await self.request("GET", f"/posts/{post['id']}", user=2)).json()
        self.assertTrue(reviewed["results_visible"])
        self.assertEqual(reviewed["submit_count"], 1)
        self.assertEqual(reviewed["my_decision"], "submit")
        self.assertEqual((await self.request("GET", f"/posts/{post['id']}/votes")).status_code, 200)

    async def test_no_self_vote_duplicate_vote_or_blank_reason(self):
        post = await self.post()
        self.assertEqual((await self.vote(post["id"], user=1)).status_code, 403)
        self.assertEqual((await self.vote(post["id"], reason="   ")).status_code, 422)
        self.assertEqual((await self.vote(post["id"], decision="invalid")).status_code, 422)
        self.assertEqual((await self.vote(post["id"])).status_code, 201)
        self.assertEqual((await self.vote(post["id"], decision="hold")).status_code, 409)
        count = (await self.request("GET", f"/posts/{post['id']}")).json()["review_count"]
        self.assertEqual(count, 1)

    async def test_watching_unlocks_results_and_permanently_blocks_review_of_that_post(self):
        post = await self.post()
        path = f"/posts/{post['id']}"
        await self.vote(post["id"])
        self.assertEqual((await self.request("GET", path + "/votes", 3)).status_code, 403)
        for _ in range(2):
            watched = await self.request("POST", path + "/watch", 3)
            self.assertEqual(watched.status_code, 200, watched.text)
            self.assertTrue(watched.json()["has_watched"])
            self.assertTrue(watched.json()["results_visible"])
            self.assertEqual(watched.json()["watch_count"], 1)
            self.assertEqual(watched.json()["review_count"], 1)
            self.assertIsNone(watched.json()["my_decision"])
        opinions = (await self.request("GET", path + "/votes", 3)).json()["items"]
        self.assertEqual(opinions[0]["reason"], "子区间比较稳定，建议提交")
        # A separate request/session must not let the observer vote, even via direct API.
        self.assertEqual((await self.vote(post["id"], user=3)).status_code, 403)
        detail = (await self.request("GET", path, 3)).json()
        self.assertTrue(detail["has_watched"])
        self.assertEqual(detail["submit_count"], 1)
        self.assertEqual((await self.request("PUT", path + f"/votes/{opinions[0]['id']}/flower", 3)).status_code, 200)
        other = await self.post()
        self.assertEqual((await self.vote(other["id"], user=3)).status_code, 201)

    async def test_watch_scope_and_stats_keep_observers_separate_from_reviewers(self):
        post = await self.post()
        await self.request("POST", f"/posts/{post['id']}/watch", 2)
        watched = (await self.request("GET", "/posts", 2, params={"scope": "watched"})).json()
        self.assertEqual([item["id"] for item in watched["items"]], [post["id"]])
        self.assertEqual(watched["items"][0]["watch_count"], 1)
        self.assertEqual(watched["items"][0]["review_count"], 0)
        self.assertEqual((await self.request("GET", "/posts", 2, params={"scope": "reviewed"})).json()["total"], 0)
        self.assertEqual((await self.request("GET", "/stats", 2)).json()["reviewed"], 0)
        self.assertEqual((await self.request("GET", "/posts", 3, params={"scope": "watched"})).json()["total"], 0)
        still_sealed = (await self.request("GET", f"/posts/{post['id']}", 3)).json()
        self.assertFalse(still_sealed["results_visible"])
        await self.request("DELETE", f"/posts/{post['id']}")
        self.assertEqual((await self.request("GET", "/posts", 2, params={"scope": "watched"})).json()["total"], 0)
        self.assertEqual((await self.request("POST", f"/posts/{post['id']}/watch", 3)).status_code, 404)

    async def test_author_and_reviewer_watch_is_noop_and_closed_posts_can_be_watched(self):
        post = await self.post()
        path = f"/posts/{post['id']}"
        await self.vote(post["id"])
        for user in (1, 2):
            result = (await self.request("POST", path + "/watch", user)).json()
            self.assertFalse(result["has_watched"])
            self.assertEqual(result["watch_count"], 0)
            self.assertTrue(result["results_visible"])
        await self.request("PATCH", path + "/outcome", json={"status": "held"})
        result = await self.request("POST", path + "/watch", 3)
        self.assertEqual(result.status_code, 200)
        self.assertTrue(result.json()["has_watched"])
        self.assertEqual((await self.request("GET", path + "/votes", 3)).status_code, 200)
        self.assertEqual((await self.vote(post["id"], user=3)).status_code, 409)

    async def test_square_orders_by_reviews_before_paginating_and_ignores_watch_count(self):
        popular = await self.post(title="Popular")
        tied_old = await self.post(title="Tied old")
        tied_new = await self.post(title="Tied new")
        unseen = await self.post(title="No reviews")
        async with self.sessions() as db:
            for index, post in enumerate((popular, tied_old, tied_new, unseen)):
                model = await db.get(SubmitWaitPost, post["id"])
                model.created_at = datetime(2026, 1, index + 1)
                for user in range(10, 10 + [3, 1, 1, 0][index]):
                    db.add(SubmitWaitVote(post_id=model.id, user_id=user, decision="submit", reason="排序测试"))
            # Many watchers must not make an unreviewed post rank above reviewed posts.
            db.add_all([SubmitWaitWatch(post_id=unseen["id"], user_id=user) for user in range(10, 20)])
            await db.commit()
        expected = [popular["id"], tied_new["id"], tied_old["id"], unseen["id"]]
        for page in (1, 2):
            result = (await self.request("GET", "/posts", 2, params={"page_size": 2, "page": page})).json()
            self.assertEqual(result["total"], 4)
            self.assertEqual([item["id"] for item in result["items"]], expected[(page - 1) * 2:page * 2])
        mine = (await self.request("GET", "/posts", params={"scope": "mine"})).json()
        self.assertEqual([item["id"] for item in mine["items"]], [unseen["id"], tied_new["id"], tied_old["id"], popular["id"]])
        filtered = (await self.request("GET", "/posts", 2, params={"search": "Tied", "blocked": "new"})).json()
        self.assertEqual([item["id"] for item in filtered["items"]], [tied_old["id"]])

    async def test_filters_search_pagination_and_personal_keyword_blocks(self):
        await self.post(title="Alpha A", content="EUR turnover")
        await self.post(title="Alpha B", content="USA sharpe")
        await self.post(title="Alpha 100%", content="USA fitness")
        blocked = (await self.request("GET", "/posts", params=[("blocked", "USA")])).json()
        self.assertEqual(blocked["total"], 1)
        mine = (await self.request("GET", "/posts", params=[("scope", "mine"), ("blocked", "USA")])).json()
        self.assertEqual(mine["total"], 3)
        literal = (await self.request("GET", "/posts", params={"search": "100%"})).json()
        self.assertEqual(literal["total"], 1)
        paged = (await self.request("GET", "/posts", params={"page_size": 1, "page": 2})).json()
        self.assertEqual(len(paged["items"]), 1)
        self.assertEqual(paged["total"], 3)
        self.assertEqual((await self.request("GET", "/posts", params={"page_size": 1000})).status_code, 422)

    async def test_random_draw_only_returns_eligible_posts_and_does_not_unlock_opinions(self):
        own = (await self.request("POST", "/posts", 2, json={"title": "自己的申请", "content": "示例"})).json()
        reviewed = await self.post()
        watched = await self.post()
        closed = await self.post()
        removed = await self.post()
        eligible = await self.post(title="可评审的申请")
        await self.vote(reviewed["id"])
        await self.request("POST", f"/posts/{watched['id']}/watch", 2)
        await self.request("PATCH", f"/posts/{closed['id']}/outcome", json={"status": "held"})
        await self.request("DELETE", f"/posts/{removed['id']}")
        await self.vote(eligible["id"], user=3)
        for _ in range(3):
            result = await self.request("GET", "/posts/random", 2)
            self.assertEqual(result.status_code, 200, result.text)
            item = result.json()
            self.assertEqual(item["id"], eligible["id"])
            self.assertNotEqual(item["id"], own["id"])
            self.assertEqual(item["review_count"], 1)
            self.assertFalse(item["results_visible"])
            self.assertFalse(item["has_watched"])
            self.assertEqual(item["watch_count"], 0)
            self.assertIsNone(item["my_decision"])
            self.assertIsNone(item["submit_count"])
        self.assertEqual((await self.request("GET", f"/posts/{eligible['id']}/votes", 2)).status_code, 403)
        self.assertEqual((await self.vote(eligible["id"])).status_code, 201)
        self.assertIsNone((await self.request("GET", "/posts/random", 2)).json())

    async def test_random_draw_samples_beyond_first_page_and_avoids_previous_when_possible(self):
        async with self.sessions() as db:
            models = [SubmitWaitPost(author_id=1, title=f"抽卡 {index}", content="示例") for index in range(15)]
            db.add_all(models)
            await db.commit()
            ids = [post.id for post in models]
        with patch("app.services.submit_wait_service.randbelow", return_value=14) as random_index:
            result = (await self.request("GET", "/posts/random", 2)).json()
            self.assertEqual(result["id"], ids[-1])
            random_index.assert_called_once_with(15)
        with patch("app.services.submit_wait_service.randbelow", return_value=0) as random_index:
            result = (await self.request("GET", "/posts/random", 2, params={"exclude_id": ids[0]})).json()
            self.assertEqual(result["id"], ids[1])
            random_index.assert_called_once_with(14)
        # With a single eligible post, drawing again still returns that post.
        result = (await self.request("GET", "/posts/random", 2, params={"blocked": [f"抽卡 {i}" for i in range(1, 15)], "exclude_id": ids[0]})).json()
        self.assertEqual(result["id"], ids[0])

    async def test_random_draw_enforces_group_access_blocks_and_authentication(self):
        self.assertEqual((await self.request("GET", "/posts/random", user=None)).status_code, 403)
        self.assertEqual((await self.request("GET", "/posts/random", user=4)).status_code, 401)
        self.assertIsNone((await self.request("GET", "/posts/random", 2)).json())
        public = await self.post(title="USA", content="Sharpe")
        group = (await self.request("POST", "/groups", json={"name": "私密小组"})).json()
        private = await self.post(group_id=group["id"])
        self.assertEqual((await self.request("GET", "/posts/random", 2)).json()["id"], public["id"])
        self.assertEqual((await self.request("GET", "/posts/random", 2, params={"group_id": group["id"]})).status_code, 404)
        self.assertIsNone((await self.request("GET", "/posts/random", 2, params={"blocked": "USA"})).json())
        await self.request("POST", "/groups/join", 2, json={"invite_code": group["invite_code"]})
        self.assertEqual((await self.request("GET", "/posts/random", 2, params={"group_id": group["id"]})).json()["id"], private["id"])
        self.assertEqual((await self.request("GET", "/posts/random", 2, params={"group_id": 0})).status_code, 422)
        self.assertEqual((await self.request("GET", "/posts/random", 2, params={"blocked": "a" * 31})).status_code, 400)

    async def test_only_author_can_close_or_withdraw_and_closed_posts_cannot_be_voted(self):
        post = await self.post()
        path = f"/posts/{post['id']}"
        self.assertEqual((await self.request("PATCH", path + "/outcome", 2, json={"status": "held"})).status_code, 403)
        self.assertEqual((await self.request("DELETE", path, 2)).status_code, 403)
        response = await self.request("PATCH", path + "/outcome", json={"status": "held", "note": "先补充验证"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["outcome_note"], "先补充验证")
        self.assertTrue(response.json()["closed_at"].endswith("Z"))
        self.assertEqual((await self.vote(post["id"])).status_code, 409)
        self.assertEqual((await self.request("PATCH", path + "/outcome", json={"status": "submitted"})).status_code, 409)
        self.assertEqual((await self.request("DELETE", path)).status_code, 200)
        self.assertEqual((await self.request("GET", path)).status_code, 404)
        self.assertEqual((await self.request("GET", "/posts")).json()["total"], 0)

    async def test_group_membership_protects_every_post_operation(self):
        group = (await self.request("POST", "/groups", json={"name": "研究小组"})).json()
        post = await self.post(group_id=group["id"])
        path = f"/posts/{post['id']}"
        self.assertEqual((await self.request("GET", "/posts")).json()["total"], 0)
        for method, suffix, kwargs in [
            ("GET", "", {}), ("GET", "/votes", {}), ("POST", "/watch", {}),
            ("POST", "/votes", {"json": {"decision": "hold", "reason": "测试"}}),
            ("PATCH", "/outcome", {"json": {"status": "held"}}), ("DELETE", "", {}),
        ]:
            self.assertEqual((await self.request(method, path + suffix, 2, **kwargs)).status_code, 404)
        self.assertEqual((await self.request("GET", "/posts", 2, params={"group_id": group["id"]})).status_code, 404)
        self.assertEqual((await self.request("POST", "/posts", 2, json={"title": "a", "content": "b", "group_id": group["id"]})).status_code, 404)
        for _ in range(2):
            response = await self.request("POST", "/groups/join", 2, json={"invite_code": group["invite_code"].lower()})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()[0]["member_count"], 2)
        self.assertEqual((await self.request("GET", path, 2)).status_code, 200)
        self.assertEqual((await self.request("POST", path + "/watch", 3)).status_code, 404)
        await self.request("POST", "/groups/join", 3, json={"invite_code": group["invite_code"]})
        self.assertEqual((await self.request("POST", path + "/watch", 3)).status_code, 200)
        self.assertEqual((await self.request("GET", "/posts", 3, params={"scope": "watched"})).json()["total"], 1)
        self.assertEqual((await self.vote(post["id"], user=3)).status_code, 403)
        self.assertEqual((await self.vote(post["id"])).status_code, 201)
        mine = (await self.request("GET", "/posts", params={"scope": "mine"})).json()
        self.assertEqual(mine["total"], 1)
        reviewed = (await self.request("GET", "/posts", 2, params={"scope": "reviewed"})).json()
        self.assertEqual(reviewed["total"], 1)

    async def test_image_ownership_reuse_and_invalid_requests(self):
        async with self.sessions() as db:
            image = SubmitWaitImage(uploader_id=2, object_name="submit-wait/other.png")
            db.add(image); await db.commit(); image_id = image.id
        for image_ids in ([image_id], [99999], [image_id, image_id], [-1], list(range(1, 11))):
            response = await self.request("POST", "/posts", json={"title": "test", "content": "test", "image_ids": image_ids})
            self.assertIn(response.status_code, (400, 422))
        response = await self.request("POST", "/posts", 2, json={"title": "test", "content": "test", "image_ids": [image_id]})
        self.assertEqual(response.status_code, 201)
        response = await self.request("POST", "/posts", 2, json={"title": "test", "content": "test", "image_ids": [image_id]})
        self.assertEqual(response.status_code, 400)
        self.assertEqual((await self.request("POST", "/posts", json={"title": "  ", "content": "a"})).status_code, 422)
        self.assertEqual((await self.request("POST", "/groups", json={"name": "  "})).status_code, 422)

    async def test_flower_is_idempotent_and_requires_unlocked_results(self):
        post = await self.post()
        await self.vote(post["id"])
        vote = (await self.request("GET", f"/posts/{post['id']}/votes")).json()["items"][0]
        path = f"/posts/{post['id']}/votes/{vote['id']}/flower"
        self.assertEqual((await self.request("PUT", path, 3)).status_code, 403)
        self.assertEqual((await self.request("PUT", path, 2)).status_code, 400)
        for _ in range(2):
            result = await self.request("PUT", path)
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()["flower_count"], 1)
        other = await self.post()
        self.assertEqual((await self.request("PUT", f"/posts/{other['id']}/votes/{vote['id']}/flower")).status_code, 404)

    async def test_stats_and_reviewed_scope_ignore_withdrawn_posts(self):
        post = await self.post()
        await self.vote(post["id"])
        await self.request("PATCH", f"/posts/{post['id']}/outcome", json={"status": "submitted"})
        self.assertEqual((await self.request("GET", "/stats")).json(), {"posts": 1, "reviewed": 0, "submitted": 1, "held": 0})
        self.assertEqual((await self.request("GET", "/stats", 2)).json()["reviewed"], 1)
        await self.request("DELETE", f"/posts/{post['id']}")
        self.assertEqual((await self.request("GET", "/stats", 2)).json()["reviewed"], 0)
        self.assertEqual((await self.request("GET", "/posts", 2, params={"scope": "reviewed"})).json()["total"], 0)

    async def test_upload_endpoint_persists_asset_for_current_user(self):
        uploaded = storage.UploadedImage("screenshot.png", "submit-wait/random.png", "image/png", 12, "https://images.test/random.png")
        with patch.object(storage, "upload_submit_wait_images", new=AsyncMock(return_value=[uploaded])):
            response = await self.request("POST", "/images", 2, files=[("files", ("screenshot.png", b"image", "image/png"))])
        self.assertEqual(response.status_code, 201)
        image_id = response.json()[0]["id"]
        async with self.sessions() as db:
            image = await db.get(SubmitWaitImage, image_id)
            self.assertEqual(image.uploader_id, 2)
            self.assertIsNone(image.post_id)

    async def image_fixture(self, post_id=None):
        async with self.sessions() as db:
            image = SubmitWaitImage(uploader_id=1, object_name="submit-wait/private.png", post_id=post_id)
            db.add(image)
            await db.commit()
            return image.id

    async def test_private_image_stream_uses_scoped_ticket_and_releases_connection(self):
        image_id = await self.image_fixture()
        path = build_image_url(image_id, 1).removeprefix("/api/v1")
        source = MagicMock()
        source.headers = {"content-type": "image/png"}
        source.stream.return_value = [b"\x89PNG\r\n\x1a\n123"]
        with patch.object(storage, "open_submit_wait_image", new=AsyncMock(return_value=source)) as read:
            response = await self.client.get(path)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"\x89PNG\r\n\x1a\n123")
        self.assertEqual(response.headers["content-type"], "image/png")
        self.assertEqual(response.headers["cache-control"], "private, max-age=60")
        read.assert_awaited_once_with("submit-wait/private.png")
        source.close.assert_called_once()
        source.release_conn.assert_called_once()

    async def test_image_tickets_reject_wrong_scope_expiry_image_and_other_uploaders(self):
        image_id = await self.image_fixture()
        path = f"/submit-wait/images/{image_id}"
        self.assertEqual((await self.client.get(path, params={"ticket": "invalid"})).status_code, 403)
        self.assertEqual((await self.client.get(path, params={"ticket": self.tokens[1]})).status_code, 403)
        wrong_image = parse_qs(urlsplit(build_image_url(image_id + 1, 1)).query)["ticket"][0]
        self.assertEqual((await self.client.get(path, params={"ticket": wrong_image})).status_code, 403)
        expired = create_access_token({"sub": f"submit-wait-image:{image_id}", "scope": "submit-wait-image",
                                       "viewer_id": 1, "image_id": image_id}, expires_delta=timedelta(seconds=-1))
        self.assertEqual((await self.client.get(path, params={"ticket": expired})).status_code, 403)
        self.assertEqual((await self.client.get(build_image_url(image_id, 2).removeprefix("/api/v1"))).status_code, 404)
        self.assertEqual((await self.client.get(build_image_url(image_id, 4).removeprefix("/api/v1"))).status_code, 403)
        image_ticket = parse_qs(urlsplit(build_image_url(image_id, 1)).query)["ticket"][0]
        response = await self.client.get("/submit-wait/posts", headers={"Authorization": f"Bearer {image_ticket}"})
        self.assertEqual(response.status_code, 401)

    async def test_image_links_recheck_group_access_and_withdrawals(self):
        group = (await self.request("POST", "/groups", json={"name": "私有小组"})).json()
        post = await self.post(group_id=group["id"])
        image_id = await self.image_fixture(post["id"])
        self.assertEqual((await self.client.get(build_image_url(image_id, 2).removeprefix("/api/v1"))).status_code, 404)
        author_url = build_image_url(image_id, 1).removeprefix("/api/v1")
        await self.request("DELETE", f"/posts/{post['id']}")
        self.assertEqual((await self.client.get(author_url)).status_code, 404)


class SubmitWaitImageTests(unittest.IsolatedAsyncioTestCase):
    def test_mime_spoofing_svg_empty_and_size_limits(self):
        for content, mime in [(b"<script>alert(1)</script>", "image/png"), (b"<svg/>", "image/svg+xml"),
                              (b"", "image/png"), (b"\x89PNG\r\n\x1a\n", "image/jpeg"),
                              (b"\x89PNG\r\n\x1a\n" + b"x" * storage.MAX_IMAGE_SIZE, "image/png")]:
            with self.assertRaises(HTTPException):
                storage.validate_submit_wait_image(content, mime)
        self.assertEqual(storage.validate_submit_wait_image(b"\x89PNG\r\n\x1a\n123", "image/png"), ".png")

    async def test_batch_failure_cleans_up_uploaded_objects_and_closes_files(self):
        files = [UploadFile(BytesIO(content), filename="private-user-id.png", headers=Headers({"content-type": "image/png"}))
                 for content in (b"\x89PNG\r\n\x1a\n123", b"not an image")]
        with patch.object(storage, "_get_client") as get_client, patch.object(storage, "_ensure_bucket"), patch.object(storage, "_put_object") as put:
            with self.assertRaises(HTTPException):
                await storage.upload_submit_wait_images(files)
            get_client.return_value.remove_object.assert_called_once()
            self.assertNotIn("private-user-id", put.call_args.args[2])
            self.assertTrue(put.call_args.args[2].startswith("submit-wait/"))
        self.assertTrue(all(file.file.closed for file in files))


if __name__ == "__main__":
    unittest.main()
