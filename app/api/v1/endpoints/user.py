from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.core.cache import cache_response
from app.models.user import SystemUser
from app.services import user_service

router = APIRouter()


@router.get("/profile/history")
@cache_response("user:profile-history")
async def get_user_history(
    request: Request,
    limit_days: int = Query(30, description="Days to look back", ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    history = await user_service.get_user_history(
        db=db,
        wq_id=current_user.wq_id,
        limit_days=limit_days,
    )

    result = [
        {
            "record_date": record.record_date.isoformat() if record.record_date else None,
            "weight_factor": record.weight_factor,
            "value_factor": record.value_factor,
            "submissions_count": record.submissions_count,
            "mean_prod_correlation": record.mean_prod_correlation,
            "mean_self_correlation": record.mean_self_correlation,
            "super_alpha_submissions_count": record.super_alpha_submissions_count,
            "super_alpha_mean_prod_correlation": record.super_alpha_mean_prod_correlation,
            "super_alpha_mean_self_correlation": record.super_alpha_mean_self_correlation,
            "university": record.university,
            "country": record.country,
        }
        for record in history
    ]

    return {
        "wq_id": current_user.wq_id,
        "username": current_user.username,
        "data": result,
    }


@router.get("/profile/statistics")
@cache_response("user:profile-statistics")
async def get_user_statistics(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    stats = await user_service.get_user_statistics(db=db, wq_id=current_user.wq_id)

    if not stats:
        return {"message": "No data available"}

    return {"wq_id": current_user.wq_id, "username": current_user.username, **stats}
