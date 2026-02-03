from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.core.cache import cache_response
from app.models.user import SystemUser
from app.schemas.dashboard import (
    CountryHistoryData,
    CountryRankingData,
    PaginatedResponse,
    UniversityRankingData,
    UserCorrelationRankingData,
    UserSubmissionsRankingData,
    UserWeightChangeRankingData,
    UserWeightRankingData,
)
from app.services import dashboard_service

router = APIRouter()


@router.get("/country-rankings", response_model=PaginatedResponse[CountryRankingData])
@cache_response("dashboard:country-rankings", vary_by_user=False)
async def get_country_rankings(
    request: Request,
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(50, description="Items per page", ge=1, le=100),
    quarter: str = Query("", description="Quarter in format YYYY-Q1"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data, total = await db.run_sync(
        lambda sync_db: dashboard_service.get_country_rankings(sync_db, page, page_size, quarter)
    )
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.get("/university-rankings", response_model=PaginatedResponse[UniversityRankingData])
@cache_response("dashboard:university-rankings", vary_by_user=False)
async def get_university_rankings(
    request: Request,
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(50, description="Items per page", ge=1, le=100),
    quarter: str = Query("", description="Quarter in format YYYY-Q1"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data, total = await db.run_sync(
        lambda sync_db: dashboard_service.get_university_rankings(sync_db, page, page_size, quarter)
    )
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.get("/top-users-by-weight", response_model=PaginatedResponse[UserWeightRankingData])
@cache_response("dashboard:top-users-by-weight", vary_by_user=False)
async def get_top_users_by_weight(
    request: Request,
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(50, description="Items per page", ge=1, le=100),
    country: Optional[str] = Query(None, description="Filter by country"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data, total = await db.run_sync(
        lambda sync_db: dashboard_service.get_top_users_by_weight(sync_db, page, page_size, country)
    )
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.get("/top-users-by-weight-change", response_model=PaginatedResponse[UserWeightChangeRankingData])
@cache_response("dashboard:top-users-by-weight-change", vary_by_user=False)
async def get_top_users_by_weight_change(
    request: Request,
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(50, description="Items per page", ge=1, le=100),
    quarter: str = Query("", description="Quarter in format YYYY-Q1"),
    order: str = Query("desc", description="Sort order", regex="^(desc|asc)$"),
    country: Optional[str] = Query(None, description="Filter by country"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data, total = await db.run_sync(
        lambda sync_db: dashboard_service.get_top_users_by_weight_change(
            sync_db, page, page_size, quarter, order, country
        )
    )
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.get("/top-users-by-submissions", response_model=PaginatedResponse[UserSubmissionsRankingData])
@cache_response("dashboard:top-users-by-submissions", vary_by_user=False)
async def get_top_users_by_submissions(
    request: Request,
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(50, description="Items per page", ge=1, le=100),
    country: Optional[str] = Query(None, description="Filter by country"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data, total = await db.run_sync(
        lambda sync_db: dashboard_service.get_top_users_by_submissions(sync_db, page, page_size, country)
    )
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.get("/top-users-by-correlation", response_model=PaginatedResponse[UserCorrelationRankingData])
@cache_response("dashboard:top-users-by-correlation", vary_by_user=False)
async def get_top_users_by_correlation(
    request: Request,
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(50, description="Items per page", ge=1, le=100),
    correlation_type: str = Query("prod", description="Correlation type", regex="^(prod|self)$"),
    country: Optional[str] = Query(None, description="Filter by country"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data, total = await db.run_sync(
        lambda sync_db: dashboard_service.get_top_users_by_correlation(
            sync_db, page, page_size, correlation_type, country
        )
    )
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.get("/country-history/{country}", response_model=PaginatedResponse[CountryHistoryData])
@cache_response("dashboard:country-history", vary_by_user=False)
async def get_country_history(
    request: Request,
    country: str,
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(20, description="Items per page", ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data, total = await db.run_sync(
        lambda sync_db: dashboard_service.get_country_history(sync_db, country, page, page_size)
    )
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }
