from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.leaderboard import LeaderboardConsultantUser
from app.models.user import SystemUser

__all__ = ["authenticate_user", "create_user_from_consultant"]


async def authenticate_user(
    db: AsyncSession,
    wq_id: str,
) -> tuple[bool, Optional[SystemUser], str]:
    """Validate user exists and is active."""
    normalized = wq_id.strip().upper()

    result = await db.execute(
        select(SystemUser).where(
            SystemUser.wq_id == normalized,
            SystemUser.delete_flag == False,
            SystemUser.is_active == True,
        )
    )
    user = result.scalars().first()
    if not user:
        return False, None, "WQ_ID not found or inactive"

    return True, user, "Login successful"


async def create_user_from_consultant(
    db: AsyncSession,
    wq_id: str,
) -> Optional[SystemUser]:
    """Create a user from consultant leaderboard data if missing."""
    normalized = wq_id.strip().upper()

    existing = await db.execute(
        select(SystemUser).where(SystemUser.wq_id == normalized)
    )
    user = existing.scalars().first()
    if user:
        return user

    consultant = await db.execute(
        select(LeaderboardConsultantUser).where(
            LeaderboardConsultantUser.user == normalized,
            LeaderboardConsultantUser.delete_flag == False,
        )
    )
    consultant_user = consultant.scalars().first()
    if not consultant_user:
        return None

    new_user = SystemUser(
        wq_id=normalized,
        username=normalized,
        is_active=True,
    )
    db.add(new_user)
    await db.flush()
    await db.refresh(new_user)
    return new_user
