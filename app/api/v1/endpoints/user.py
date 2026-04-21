from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.core.cache import cache_response
from app.models.user import SystemUser
from app.schemas.user import (
    UserPageAuthSetRequest,
    UserPageAuthSetResponse,
    UserPageAuthStatusResponse,
    UserPageAuthVerifyRequest,
    UserPageAuthVerifyResponse,
)
from app.services import user_service

router = APIRouter()


@router.get("/profile/history")
@cache_response("user:profile-history")
async def get_user_history(
    request: Request,
    limit_days: int = Query(3650, description="Days to look back", ge=1, le=3650),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    history = await user_service.get_user_history(
        db=db,
        wq_id=current_user.wq_id,
        limit_days=limit_days,
    )

    trends = {"value_factor_trend": [], "combined_trend": []}
    if history:
        trends = await user_service.get_user_metric_trends_by_event(
            db=db,
            wq_id=current_user.wq_id,
            start_date=history[0].record_date,
            end_date=history[-1].record_date,
        )

    result = [
        {
            "record_date": record.record_date.isoformat() if record.record_date else None,
            "weight_factor": record.weight_factor,
            "value_factor": record.value_factor,
            "daily_osmosis_rank": record.daily_osmosis_rank,
            "combined_alpha_performance": None,
            "combined_power_pool_alpha_performance": None,
            "combined_selected_alpha_performance": None,
            "combined_osmosis_performance": None,
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
        "value_factor_trend": trends["value_factor_trend"],
        "combined_trend": trends["combined_trend"],
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


@router.get("/page-auth/{page_key}", response_model=UserPageAuthStatusResponse)
async def get_page_auth_status(
    page_key: str,
    current_user: SystemUser = Depends(get_current_user),
):
    try:
        return await user_service.get_page_auth_status(current_user=current_user, page_key=page_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/page-auth/{page_key}/set", response_model=UserPageAuthSetResponse)
async def set_page_auth_code(
    page_key: str,
    payload: UserPageAuthSetRequest,
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    try:
        return await user_service.set_page_auth_code(
            db=db,
            current_user=current_user,
            page_key=page_key,
            auth_code=payload.auth_code,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/page-auth/{page_key}/verify", response_model=UserPageAuthVerifyResponse)
async def verify_page_auth_code(
    page_key: str,
    payload: UserPageAuthVerifyRequest,
    current_user: SystemUser = Depends(get_current_user),
):
    try:
        return await user_service.verify_page_auth_code(
            current_user=current_user,
            page_key=page_key,
            auth_code=payload.auth_code,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
