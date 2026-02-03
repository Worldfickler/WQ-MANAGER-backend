from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.user import SystemUser
from app.schemas.feedback import FeedbackCreate, FeedbackResponse
from app.services import feedback_service

router = APIRouter()


@router.post("", response_model=FeedbackResponse)
async def submit_feedback(
    payload: FeedbackCreate,
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    try:
        if not payload.content.strip():
            raise HTTPException(status_code=400, detail="反馈内容不能为空")
        await feedback_service.create_feedback(db, payload, current_user)
        return FeedbackResponse(success=True, message="反馈已提交")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail="反馈提交失败") from exc
