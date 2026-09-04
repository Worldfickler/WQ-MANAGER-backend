from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.sponsor import SponsorDonation

__all__ = ["get_donor_ids"]


async def get_donor_ids(db: AsyncSession) -> list[str]:
    """只读取可展示的 WQ_ID，不向接口层传递赞助金额。"""
    result = await db.execute(
        select(SponsorDonation.wq_id)
        .where(SponsorDonation.amount > 0)
        .order_by(SponsorDonation.id.asc())
    )
    return [wq_id for wq_id in result.scalars().all() if wq_id]
