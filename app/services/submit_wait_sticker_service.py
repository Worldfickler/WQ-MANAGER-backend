from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.security import create_access_token
from app.models.submit_wait import SubmitWaitSticker as Sticker, SubmitWaitVoteSticker as VoteSticker


def build_sticker_url(sticker_id: int, viewer_id: int) -> str:
    ticket = create_access_token(
        {"sub": f"submit-wait-sticker:{sticker_id}", "scope": "submit-wait-sticker",
         "sticker_id": sticker_id, "viewer_id": viewer_id},
        expires_delta=timedelta(hours=1),
    )
    return f"{settings.API_PREFIX}/submit-wait/stickers/{sticker_id}/image?ticket={ticket}"


def serialize_sticker(sticker: Sticker, viewer_id: int) -> dict:
    return {"id": sticker.id, "name": sticker.name, "width": sticker.width, "height": sticker.height,
            "url": build_sticker_url(sticker.id, viewer_id)}


async def list_stickers(db: AsyncSession, viewer_id: int) -> list[dict]:
    stickers = (await db.scalars(select(Sticker).where(Sticker.active.is_(True))
                                .order_by(Sticker.sort_order, Sticker.id))).all()
    return [serialize_sticker(sticker, viewer_id) for sticker in stickers]


async def for_votes(db: AsyncSession, vote_ids: list[int], viewer_id: int) -> dict[int, list[dict]]:
    if not vote_ids:
        return {}
    rows = (await db.execute(select(VoteSticker, Sticker).join(Sticker, Sticker.id == VoteSticker.sticker_id)
                            .where(VoteSticker.vote_id.in_(vote_ids))
                            .order_by(VoteSticker.vote_id, VoteSticker.position, VoteSticker.id))).all()
    result: dict[int, list[dict]] = {}
    for link, sticker in rows:
        result.setdefault(link.vote_id, []).append(serialize_sticker(sticker, viewer_id))
    return result
