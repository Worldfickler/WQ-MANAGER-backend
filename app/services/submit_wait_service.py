from datetime import datetime, timedelta, timezone
from secrets import randbelow, token_hex

from fastapi import HTTPException
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.security import create_access_token
from app.models.submit_wait import (
    SubmitWaitFlower as Flower, SubmitWaitGroup as Group,
    SubmitWaitImage as Image, SubmitWaitMember as Member,
    SubmitWaitPost as Post, SubmitWaitVote as Vote, SubmitWaitWatch as Watch,
)
from app.models.user import SystemUser
from app.schemas.submit_wait import PostCreate


def build_image_url(image_id: int, viewer_id: int) -> str:
    """A scoped image ticket lets img tags load private MinIO objects over the app origin."""
    ticket = create_access_token(
        {"sub": f"submit-wait-image:{image_id}", "scope": "submit-wait-image",
         "image_id": image_id, "viewer_id": viewer_id},
        expires_delta=timedelta(hours=1),
    )
    return f"{settings.API_PREFIX}/submit-wait/images/{image_id}?ticket={ticket}"


async def require_member(db: AsyncSession, group_id: int, user_id: int) -> None:
    member = await db.scalar(select(Member.id).where(Member.group_id == group_id, Member.user_id == user_id))
    if member is None:
        raise HTTPException(404, "小组不存在或你尚未加入")


async def get_post(db: AsyncSession, post_id: int, user_id: int, *, lock: bool = False) -> Post:
    query = select(Post).where(Post.id == post_id, Post.deleted.is_(False))
    if lock:
        query = query.with_for_update()
    post = await db.scalar(query.execution_options(populate_existing=True))
    if post is None:
        raise HTTPException(404, "申请不存在或已被撤回")
    if post.group_id is not None:
        await require_member(db, post.group_id, user_id)
    return post


async def my_vote(db: AsyncSession, post_id: int, user_id: int) -> Vote | None:
    return await db.scalar(select(Vote).where(Vote.post_id == post_id, Vote.user_id == user_id).with_for_update())


async def my_watch(db: AsyncSession, post_id: int, user_id: int) -> Watch | None:
    return await db.scalar(select(Watch).where(Watch.post_id == post_id, Watch.user_id == user_id).with_for_update())


async def require_results(db: AsyncSession, post: Post, user_id: int) -> None:
    if (post.author_id != user_id and await my_vote(db, post.id, user_id) is None
            and await my_watch(db, post.id, user_id) is None):
        raise HTTPException(403, "请先完成评审，或选择围观后查看其他人的意见")


def utc_now() -> datetime:
    # MySQL stores naive UTC; response timestamps carry an explicit UTC offset.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_timestamp(value: datetime | None) -> datetime | None:
    return value.replace(tzinfo=timezone.utc) if value is not None else None


async def serialize_posts(db: AsyncSession, posts: list[Post], user_id: int) -> list[dict]:
    if not posts:
        return []
    ids = [post.id for post in posts]
    author_ids = {post.author_id for post in posts if not post.anonymous}
    authors = {}
    if author_ids:
        authors = {user.id: user.username or user.wq_id for user in
                   (await db.scalars(select(SystemUser).where(SystemUser.id.in_(author_ids)))).all()}
    groups = {group.id: group.name for group in
              (await db.scalars(select(Group).where(Group.id.in_({p.group_id for p in posts if p.group_id})))).all()}
    images: dict[int, list[dict]] = {}
    for image in (await db.scalars(select(Image).where(Image.post_id.in_(ids)).order_by(Image.id))).all():
        images.setdefault(image.post_id, []).append({"id": image.id, "url": build_image_url(image.id, user_id)})
    counts: dict[int, dict[str, int]] = {}
    for post_id, decision, count in (await db.execute(
        select(Vote.post_id, Vote.decision, func.count(Vote.id))
        .where(Vote.post_id.in_(ids)).group_by(Vote.post_id, Vote.decision)
    )).all():
        counts.setdefault(post_id, {})[decision] = count
    own_votes = {vote.post_id: vote.decision for vote in (await db.scalars(
        select(Vote).where(Vote.post_id.in_(ids), Vote.user_id == user_id)
    )).all()}
    watch_counts = dict((await db.execute(select(Watch.post_id, func.count(Watch.id))
                                         .where(Watch.post_id.in_(ids)).group_by(Watch.post_id))).all())
    own_watches = set((await db.scalars(select(Watch.post_id)
                                      .where(Watch.post_id.in_(ids), Watch.user_id == user_id))).all())
    items = []
    for post in posts:
        visible = post.author_id == user_id or post.id in own_votes or post.id in own_watches
        votes = counts.get(post.id, {})
        items.append({
            "id": post.id, "title": post.title, "content": post.content,
            "anonymous": post.anonymous,
            "author_name": "匿名研究员" if post.anonymous else authors.get(post.author_id, "研究员"),
            "is_owner": post.author_id == user_id,
            "group_id": post.group_id, "group_name": groups.get(post.group_id),
            "status": post.status, "outcome_note": post.outcome_note,
            "created_at": utc_timestamp(post.created_at), "closed_at": utc_timestamp(post.closed_at),
            "images": images.get(post.id, []), "review_count": sum(votes.values()),
            "watch_count": watch_counts.get(post.id, 0), "has_watched": post.id in own_watches,
            "results_visible": visible,
            "submit_count": votes.get("submit", 0) if visible else None,
            "hold_count": votes.get("hold", 0) if visible else None,
            "my_decision": own_votes.get(post.id),
        })
    return items


async def list_posts(
    db: AsyncSession, user_id: int, scope: str, group_id: int | None,
    status: str | None, search: str, blocked: list[str], page: int, page_size: int,
) -> dict:
    conditions = [Post.deleted.is_(False)]
    if group_id is not None:
        await require_member(db, group_id, user_id)
        conditions.append(Post.group_id == group_id)
    elif scope == "all":
        conditions.append(Post.group_id.is_(None))
    else:
        member_groups = select(Member.group_id).where(Member.user_id == user_id)
        conditions.append(or_(Post.group_id.is_(None), Post.group_id.in_(member_groups)))
    if scope == "mine":
        conditions.append(Post.author_id == user_id)
    elif scope == "reviewed":
        conditions.append(Post.id.in_(select(Vote.post_id).where(Vote.user_id == user_id)))
    elif scope == "watched":
        conditions.append(Post.id.in_(select(Watch.post_id).where(Watch.user_id == user_id)))
    if status:
        conditions.append(Post.status == status)
    if search.strip():
        conditions.append(or_(Post.title.contains(search.strip(), autoescape=True), Post.content.contains(search.strip(), autoescape=True)))
    if scope == "all":
        for word in blocked:
            if word.strip():
                conditions.append(~or_(Post.title.contains(word.strip(), autoescape=True), Post.content.contains(word.strip(), autoescape=True)))
    total = await db.scalar(select(func.count(Post.id)).where(*conditions))
    query = select(Post).where(*conditions)
    if scope == "all":
        review_counts = (select(Vote.post_id, func.count(Vote.id).label("review_count"))
                         .group_by(Vote.post_id).subquery())
        query = query.outerjoin(review_counts, review_counts.c.post_id == Post.id)
        query = query.order_by(func.coalesce(review_counts.c.review_count, 0).desc())
    query = query.order_by(Post.created_at.desc(), Post.id.desc())
    posts = (await db.scalars(query.offset((page - 1) * page_size).limit(page_size))).all()
    return {"items": await serialize_posts(db, list(posts), user_id), "total": total, "page": page, "page_size": page_size}


async def random_post(
    db: AsyncSession, user_id: int, group_id: int | None,
    blocked: list[str], exclude_id: int | None,
) -> dict | None:
    conditions = [Post.deleted.is_(False), Post.status == "open", Post.author_id != user_id,
                  ~Post.id.in_(select(Vote.post_id).where(Vote.user_id == user_id)),
                  ~Post.id.in_(select(Watch.post_id).where(Watch.user_id == user_id))]
    if group_id is not None:
        await require_member(db, group_id, user_id)
        conditions.append(Post.group_id == group_id)
    else:
        conditions.append(Post.group_id.is_(None))
    for word in blocked:
        if word.strip():
            conditions.append(~or_(Post.title.contains(word.strip(), autoescape=True),
                                   Post.content.contains(word.strip(), autoescape=True)))
    if exclude_id is not None:
        alternatives = [*conditions, Post.id != exclude_id]
        total = await db.scalar(select(func.count(Post.id)).where(*alternatives))
        if total:
            conditions = alternatives
        else:
            total = await db.scalar(select(func.count(Post.id)).where(*conditions))
    else:
        total = await db.scalar(select(func.count(Post.id)).where(*conditions))
    if not total:
        return None
    # Sample the entire eligible pool, independent of feed ranking or pagination.
    post = await db.scalar(select(Post).where(*conditions).order_by(Post.id).offset(randbelow(total)).limit(1))
    return (await serialize_posts(db, [post], user_id))[0] if post is not None else None


async def create_post(db: AsyncSession, user_id: int, payload: PostCreate) -> Post:
    if payload.group_id is not None:
        await require_member(db, payload.group_id, user_id)
    images = []
    if payload.image_ids:
        images = list((await db.scalars(select(Image).where(Image.id.in_(payload.image_ids)).with_for_update())).all())
        if len(images) != len(payload.image_ids) or any(image.uploader_id != user_id or image.post_id is not None for image in images):
            raise HTTPException(400, "图片无效、已被使用或不属于当前用户，请重新上传")
    post = Post(author_id=user_id, title=payload.title, content=payload.content,
                anonymous=payload.anonymous, group_id=payload.group_id)
    db.add(post)
    await db.flush()
    for image in images:
        image.post_id = post.id
    await db.flush()
    await db.refresh(post)
    return post


async def group_items(db: AsyncSession, user_id: int) -> list[dict]:
    groups = (await db.scalars(select(Group).join(Member, Member.group_id == Group.id)
                              .where(Member.user_id == user_id).order_by(Group.created_at.desc(), Group.id.desc()))).all()
    counts = dict((await db.execute(select(Member.group_id, func.count(Member.id))
                                   .where(Member.group_id.in_([group.id for group in groups]))
                                   .group_by(Member.group_id))).all())
    return [{"id": group.id, "name": group.name, "invite_code": group.invite_code,
             "member_count": counts.get(group.id, 0)} for group in groups]


async def create_group(db: AsyncSession, user_id: int, name: str) -> dict:
    group = Group(name=name, invite_code=token_hex(8).upper(), owner_id=user_id)
    db.add(group)
    await db.flush()
    db.add(Member(group_id=group.id, user_id=user_id))
    await db.flush()
    return {"id": group.id, "name": group.name, "invite_code": group.invite_code, "member_count": 1}


async def join_group(db: AsyncSession, user_id: int, invite_code: str) -> None:
    group = await db.scalar(select(Group).where(Group.invite_code == invite_code.upper()).with_for_update())
    if group is None:
        raise HTTPException(404, "邀请码无效")
    if await db.scalar(select(Member.id).where(Member.group_id == group.id, Member.user_id == user_id).with_for_update()) is None:
        db.add(Member(group_id=group.id, user_id=user_id))
        await db.flush()
