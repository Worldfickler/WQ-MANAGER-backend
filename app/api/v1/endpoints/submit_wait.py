from typing import Literal

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import decode_access_token, get_current_user
from app.models.submit_wait import SubmitWaitFlower as Flower, SubmitWaitImage as Image
from app.models.submit_wait import SubmitWaitPost as Post, SubmitWaitVote as Vote, SubmitWaitWatch as Watch
from app.models.submit_wait import SubmitWaitSticker as Sticker, SubmitWaitVoteSticker as VoteSticker
from app.models.user import SystemUser
from app.schemas.submit_wait import (
    GroupCreate, GroupItem, GroupJoin, ImageItem, MyStats, OutcomeUpdate,
    PostCreate, PostItem, PostPage, VoteCreate, VotePage, StickerItem,
)
from app.services import minio_storage_service as storage
from app.services import submit_wait_service as service
from app.services import submit_wait_sticker_service as sticker_service

router = APIRouter()


def image_response(source) -> StreamingResponse:
    def stream():
        try:
            yield from source.stream(64 * 1024)
        finally:
            source.close()
            source.release_conn()

    return StreamingResponse(stream(), media_type=source.headers.get("content-type", "application/octet-stream"),
                             headers={"Cache-Control": "private, max-age=60", "X-Content-Type-Options": "nosniff"})


@router.get("/stickers", response_model=list[StickerItem])
async def list_stickers(db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    return await sticker_service.list_stickers(db, user.id)


@router.get("/stickers/{sticker_id}/image")
async def get_sticker_image(sticker_id: int, ticket: str = Query(..., max_length=2048),
                            db: AsyncSession = Depends(get_db)):
    payload = decode_access_token(ticket)
    if (payload is None or payload.get("scope") != "submit-wait-sticker"
            or payload.get("sub") != f"submit-wait-sticker:{sticker_id}"
            or payload.get("sticker_id") != sticker_id or not payload.get("exp")):
        raise HTTPException(403, "表情访问凭证无效或已过期，请刷新表情")
    viewer = await db.scalar(select(SystemUser).where(
        SystemUser.id == payload.get("viewer_id"), SystemUser.is_active.is_(True),
        SystemUser.delete_flag.is_(False),
    ))
    if viewer is None:
        raise HTTPException(403, "表情访问权限已失效")
    sticker = await db.get(Sticker, sticker_id)
    if sticker is None:
        raise HTTPException(404, "表情不存在")
    return image_response(await storage.open_submit_wait_image(sticker.object_name))


@router.get("/stats", response_model=MyStats)
async def get_stats(db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    counts = dict((await db.execute(select(Post.status, func.count(Post.id)).where(
        Post.author_id == user.id, Post.deleted.is_(False)).group_by(Post.status))).all())
    reviewed = await db.scalar(select(func.count(Vote.id)).join(Post, Post.id == Vote.post_id)
                               .where(Vote.user_id == user.id, Post.deleted.is_(False)))
    return {"posts": sum(counts.values()), "reviewed": reviewed,
            "submitted": counts.get("submitted", 0), "held": counts.get("held", 0)}


@router.get("/groups", response_model=list[GroupItem])
async def list_groups(db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    return await service.group_items(db, user.id)


@router.post("/groups", response_model=GroupItem, status_code=201)
async def create_group(payload: GroupCreate, db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    return await service.create_group(db, user.id, payload.name)


@router.post("/groups/join", response_model=list[GroupItem])
async def join_group(payload: GroupJoin, db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    await service.join_group(db, user.id, payload.invite_code)
    return await service.group_items(db, user.id)


@router.post("/images", response_model=list[ImageItem], status_code=201)
async def upload_images(files: list[UploadFile] = File(...), db: AsyncSession = Depends(get_db),
                        user: SystemUser = Depends(get_current_user)):
    uploaded = await storage.upload_submit_wait_images(files)
    result = []
    for item in uploaded:
        image = Image(uploader_id=user.id, object_name=item.object_name)
        db.add(image)
        await db.flush()
        result.append({"id": image.id, "url": service.build_image_url(image.id, user.id)})
    return result


@router.get("/images/{image_id}")
async def get_image(image_id: int, ticket: str = Query(..., max_length=2048),
                    db: AsyncSession = Depends(get_db)):
    payload = decode_access_token(ticket)
    if (payload is None or payload.get("scope") != "submit-wait-image"
            or payload.get("sub") != f"submit-wait-image:{image_id}"
            or payload.get("image_id") != image_id or not payload.get("exp")):
        raise HTTPException(403, "图片访问凭证无效或已过期，请刷新申请")
    viewer = await db.scalar(select(SystemUser).where(
        SystemUser.id == payload.get("viewer_id"), SystemUser.is_active.is_(True),
        SystemUser.delete_flag.is_(False),
    ))
    if viewer is None:
        raise HTTPException(403, "图片访问权限已失效")
    image = await db.get(Image, image_id)
    if image is None:
        raise HTTPException(404, "图片不存在")
    if image.post_id is not None:
        await service.get_post(db, image.post_id, viewer.id)
    elif image.uploader_id != viewer.id:
        raise HTTPException(404, "图片不存在")
    source = await storage.open_submit_wait_image(image.object_name)

    return image_response(source)


@router.get("/posts", response_model=PostPage)
async def list_posts(
    scope: Literal["all", "mine", "reviewed", "watched"] = "all",
    group_id: int | None = Query(None, gt=0),
    status: Literal["open", "submitted", "held"] | None = None,
    search: str = Query("", max_length=120),
    blocked: list[str] = Query(default=[], max_length=20),
    page: int = Query(1, ge=1), page_size: int = Query(12, ge=1, le=50),
    db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user),
):
    if any(len(word) > 30 for word in blocked):
        raise HTTPException(400, "每个屏蔽词不能超过 30 个字符")
    return await service.list_posts(db, user.id, scope, group_id, status, search, blocked, page, page_size)


@router.get("/posts/random", response_model=PostItem | None)
async def random_post(
    group_id: int | None = Query(None, gt=0),
    blocked: list[str] = Query(default=[], max_length=20),
    exclude_id: int | None = Query(None, gt=0),
    db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user),
):
    if any(len(word) > 30 for word in blocked):
        raise HTTPException(400, "每个屏蔽词不能超过 30 个字符")
    return await service.random_post(db, user.id, group_id, blocked, exclude_id)


@router.post("/posts", response_model=PostItem, status_code=201)
async def create_post(payload: PostCreate, db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    post = await service.create_post(db, user.id, payload)
    return (await service.serialize_posts(db, [post], user.id))[0]


@router.get("/posts/{post_id}", response_model=PostItem)
async def get_post(post_id: int, db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    post = await service.get_post(db, post_id, user.id)
    return (await service.serialize_posts(db, [post], user.id))[0]


@router.post("/posts/{post_id}/votes", response_model=PostItem, status_code=201)
async def vote(post_id: int, payload: VoteCreate, db: AsyncSession = Depends(get_db),
               user: SystemUser = Depends(get_current_user)):
    post = await service.get_post(db, post_id, user.id, lock=True)
    if post.author_id == user.id:
        raise HTTPException(403, "不能评审自己的 Alpha")
    if post.status != "open":
        raise HTTPException(409, "作者已反馈最终决定，评审已结束")
    if await service.my_vote(db, post.id, user.id) is not None:
        raise HTTPException(409, "你已评审过这条申请")
    if await service.my_watch(db, post.id, user.id) is not None:
        raise HTTPException(403, "你已选择围观，不能再评审这条申请")
    if payload.sticker_ids:
        available = (await db.scalars(select(Sticker.id).where(
            Sticker.id.in_(payload.sticker_ids), Sticker.active.is_(True)).with_for_update())).all()
        if len(available) != len(payload.sticker_ids):
            raise HTTPException(400, "所选表情不存在或已停用，请刷新表情后重新选择")
    record = Vote(post_id=post.id, user_id=user.id, decision=payload.decision, reason=payload.reason)
    db.add(record)
    try:
        await db.flush()
    except IntegrityError as exc:
        raise HTTPException(409, "你已评审过这条申请") from exc
    db.add_all([VoteSticker(vote_id=record.id, sticker_id=sticker_id, position=index)
                for index, sticker_id in enumerate(payload.sticker_ids)])
    await db.flush()
    return (await service.serialize_posts(db, [post], user.id))[0]


@router.post("/posts/{post_id}/watch", response_model=PostItem)
async def watch_post(post_id: int, db: AsyncSession = Depends(get_db),
                     user: SystemUser = Depends(get_current_user)):
    # Watching and voting lock the same post before checking either choice.
    # This keeps a user from committing both actions in concurrent requests.
    post = await service.get_post(db, post_id, user.id, lock=True)
    if (post.author_id != user.id and await service.my_vote(db, post.id, user.id) is None
            and await service.my_watch(db, post.id, user.id) is None):
        db.add(Watch(post_id=post.id, user_id=user.id))
        await db.flush()
    return (await service.serialize_posts(db, [post], user.id))[0]


@router.get("/posts/{post_id}/votes", response_model=VotePage)
async def list_votes(post_id: int, page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=50),
                     db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    post = await service.get_post(db, post_id, user.id)
    await service.require_results(db, post, user.id)
    total = await db.scalar(select(func.count(Vote.id)).where(Vote.post_id == post.id))
    votes = (await db.scalars(select(Vote).where(Vote.post_id == post.id).order_by(Vote.created_at.desc(), Vote.id.desc())
                             .offset((page - 1) * page_size).limit(page_size))).all()
    ids = [vote.id for vote in votes]
    flowers = dict((await db.execute(select(Flower.vote_id, func.count(Flower.id))
                                    .where(Flower.vote_id.in_(ids)).group_by(Flower.vote_id))).all())
    mine = set((await db.scalars(select(Flower.vote_id).where(Flower.vote_id.in_(ids), Flower.user_id == user.id))).all())
    stickers = await sticker_service.for_votes(db, ids, user.id)
    return {"items": [{"id": vote.id, "reviewer_name": f"研究员 {vote.id:04d}",
                       "decision": vote.decision, "reason": vote.reason,
                       "created_at": service.utc_timestamp(vote.created_at), "is_mine": vote.user_id == user.id,
                       "flower_count": flowers.get(vote.id, 0), "flowered": vote.id in mine,
                       "stickers": stickers.get(vote.id, [])} for vote in votes],
            "total": total, "page": page, "page_size": page_size}


@router.put("/posts/{post_id}/votes/{vote_id}/flower")
async def send_flower(post_id: int, vote_id: int, db: AsyncSession = Depends(get_db),
                      user: SystemUser = Depends(get_current_user)):
    post = await service.get_post(db, post_id, user.id, lock=True)
    await service.require_results(db, post, user.id)
    vote_record = await db.scalar(select(Vote).where(Vote.id == vote_id, Vote.post_id == post_id))
    if vote_record is None:
        raise HTTPException(404, "评审意见不存在")
    if vote_record.user_id == user.id:
        raise HTTPException(400, "不能给自己的意见送花")
    existing = await db.scalar(select(Flower.id).where(Flower.vote_id == vote_id, Flower.user_id == user.id).with_for_update())
    if existing is None:
        db.add(Flower(vote_id=vote_id, user_id=user.id))
        await db.flush()
    count = await db.scalar(select(func.count(Flower.id)).where(Flower.vote_id == vote_id))
    return {"flower_count": count, "flowered": True}


@router.patch("/posts/{post_id}/outcome", response_model=PostItem)
async def update_outcome(post_id: int, payload: OutcomeUpdate, db: AsyncSession = Depends(get_db),
                         user: SystemUser = Depends(get_current_user)):
    post = await service.get_post(db, post_id, user.id, lock=True)
    if post.author_id != user.id:
        raise HTTPException(403, "只有作者可以反馈最终决定")
    if post.status != "open":
        raise HTTPException(409, "已反馈最终决定")
    post.status = payload.status
    post.outcome_note = payload.note
    post.closed_at = service.utc_now()
    await db.flush()
    return (await service.serialize_posts(db, [post], user.id))[0]


@router.delete("/posts/{post_id}")
async def delete_post(post_id: int, db: AsyncSession = Depends(get_db), user: SystemUser = Depends(get_current_user)):
    post = await service.get_post(db, post_id, user.id, lock=True)
    if post.author_id != user.id:
        raise HTTPException(403, "只有作者可以撤回申请")
    post.deleted = True
    await db.flush()
    return {"success": True}
