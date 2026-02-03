from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.feedback import UserFeedback
from app.models.user import SystemUser
from app.schemas.feedback import FeedbackCreate

__all__ = ["create_feedback"]


async def create_feedback(
    db: AsyncSession,
    payload: FeedbackCreate,
    current_user: SystemUser,
) -> UserFeedback:
    content = payload.content.strip()
    feedback = UserFeedback(
        user_id=current_user.id,
        wq_id=current_user.wq_id,
        username=current_user.username,
        content=content,
        feedback_type=payload.feedback_type,
        page=payload.page,
        contact=payload.contact,
        status="new",
    )
    db.add(feedback)
    await db.flush()
    await db.refresh(feedback)
    return feedback
