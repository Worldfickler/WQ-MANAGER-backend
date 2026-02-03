from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.core.cache import cache_response
from app.models.user import SystemUser
from app.schemas.leaderboard import (
    CountrySubmissionTimeSeriesResponse,
    CountryWeightData,
    CountryWeightTimeSeriesResponse,
    GeniusUserWeightChangeResponse,
    GeniusCountryTimeSeriesResponse,
    GeniusWeightTimeSeriesResponse,
    GeniusLevelWeightChangeResponse,
    SummaryStatistics,
    UserWeightTimeSeriesResponse,
    UserWeightData,
)
from app.services import leaderboard_service

router = APIRouter()


@router.get("/country-weight-timeseries", response_model=List[CountryWeightTimeSeriesResponse])
@cache_response("leaderboard:country-weight-timeseries", vary_by_user=False)
async def get_country_weight_timeseries(
    request: Request,
    countries: Optional[str] = Query(
        None,
        description="Comma-separated country codes (e.g., 'CN,US,IN'). If not specified, returns all countries.",
    ),
    limit_days: int = Query(30, description="Number of recent days to fetch", ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    country_list = countries.split(",") if countries else None
    country_data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_country_weight_time_series(
            db=sync_db,
            countries=country_list,
            limit_days=limit_days,
        )
    )

    return [
        CountryWeightTimeSeriesResponse(
            country=country,
            dates=data["dates"],
            weights=data["weights"],
        )
        for country, data in country_data.items()
    ]


@router.get("/available-countries", response_model=List[str])
@cache_response("leaderboard:available-countries", vary_by_user=False)
async def get_available_countries(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    return await db.run_sync(lambda sync_db: leaderboard_service.get_available_countries(sync_db))


@router.get("/genius-available-countries", response_model=List[str])
@cache_response("leaderboard:genius-available-countries", vary_by_user=False)
async def get_genius_available_countries(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    return await db.run_sync(lambda sync_db: leaderboard_service.get_genius_available_countries(sync_db))


@router.get("/genius-available-levels", response_model=List[str])
@cache_response("leaderboard:genius-available-levels", vary_by_user=False)
async def get_genius_available_levels(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    return await db.run_sync(lambda sync_db: leaderboard_service.get_genius_available_levels(sync_db))


@router.get("/country-submission-timeseries", response_model=List[CountrySubmissionTimeSeriesResponse])
@cache_response("leaderboard:country-submission-timeseries", vary_by_user=False)
async def get_country_submission_timeseries(
    request: Request,
    countries: Optional[str] = Query(
        None,
        description="Comma-separated country codes (e.g., 'CN,US,IN'). If not specified, returns all countries.",
    ),
    limit_days: int = Query(30, description="Number of recent days to fetch", ge=1, le=365),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    country_list = countries.split(",") if countries else None
    country_data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_country_submission_time_series(
            db=sync_db,
            countries=country_list,
            limit_days=limit_days,
            start_date=start_date,
            end_date=end_date,
        )
    )

    return [
        CountrySubmissionTimeSeriesResponse(
            country=country,
            dates=data["dates"],
            submissions_count=data["submissions_count"],
            super_alpha_submissions_count=data["super_alpha_submissions_count"],
            submissions_change=data["submissions_change"],
            super_alpha_submissions_change=data["super_alpha_submissions_change"],
        )
        for country, data in country_data.items()
    ]


@router.get("/country-leaderboard", response_model=List[CountryWeightData])
@cache_response("leaderboard:country-leaderboard", vary_by_user=False)
async def get_country_leaderboard(
    request: Request,
    limit: int = Query(10, description="Maximum number of countries to return", ge=1, le=100),
    days: int = Query(7, description="Days to look back for change calculation", ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    return await db.run_sync(lambda sync_db: leaderboard_service.get_country_leaderboard(sync_db, limit, days))


@router.get("/user-leaderboard", response_model=List[UserWeightData])
@cache_response("leaderboard:user-leaderboard", vary_by_user=False)
async def get_user_leaderboard(
    request: Request,
    limit: int = Query(6, description="Maximum number of users to return", ge=1, le=100),
    days: int = Query(7, description="Days to look back for change calculation", ge=1, le=365),
    order: str = Query(
        "desc",
        description="Sort order: 'desc' for positive change first, 'asc' for negative change first",
        regex="^(desc|asc)$",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    return await db.run_sync(
        lambda sync_db: leaderboard_service.get_user_leaderboard(sync_db, limit, days, order)
    )


@router.get("/summary-statistics", response_model=SummaryStatistics)
@cache_response("leaderboard:summary-statistics", vary_by_user=False)
async def get_summary_statistics(
    request: Request,
    days: int = Query(7, description="Days to look back for change calculation", ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    stats = await db.run_sync(lambda sync_db: leaderboard_service.get_summary_statistics(sync_db, days))
    return SummaryStatistics(**stats)


@router.get("/genius-country-timeseries", response_model=List[GeniusCountryTimeSeriesResponse])
@cache_response("leaderboard:genius-country-timeseries", vary_by_user=False)
async def get_genius_country_timeseries(
    request: Request,
    countries: Optional[str] = Query(
        None,
        description="Comma-separated country codes (e.g., 'CN,US,IN'). If not specified, returns all countries.",
    ),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    country_list = countries.split(",") if countries else None
    country_data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_genius_country_time_series(
            db=sync_db,
            countries=country_list,
            start_date=start_date,
            end_date=end_date,
        )
    )

    return [
        GeniusCountryTimeSeriesResponse(
            country=country,
            dates=data["dates"],
            alpha_count_change=data["alpha_count_change"],
        )
        for country, data in country_data.items()
    ]


@router.get("/genius-weight-timeseries", response_model=List[GeniusWeightTimeSeriesResponse])
@cache_response("leaderboard:genius-weight-timeseries", vary_by_user=False)
async def get_genius_weight_timeseries(
    request: Request,
    levels: Optional[str] = Query(
        None,
        description="Comma-separated genius levels (e.g., 'EXPERT,GOLD')",
    ),
    countries: Optional[str] = Query(
        None,
        description="Comma-separated country codes (e.g., 'CN,US,IN'). If not specified, returns all countries.",
    ),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    level_list = [level.strip() for level in levels.split(",") if level.strip()] if levels else None
    country_list = [country.strip() for country in countries.split(",") if country.strip()] if countries else None
    series_map = await db.run_sync(
        lambda sync_db: leaderboard_service.get_genius_weight_sum_time_series(
            db=sync_db,
            genius_levels=level_list,
            countries=country_list,
            start_date=start_date,
            end_date=end_date,
        )
    )

    return [
        GeniusWeightTimeSeriesResponse(
            genius_level=data["genius_level"],
            country=data["country"],
            dates=data["dates"],
            weights=data["weights"],
        )
        for data in series_map.values()
    ]


@router.get("/genius-user-weight-changes", response_model=List[GeniusUserWeightChangeResponse])
@cache_response("leaderboard:genius-user-weight-changes", vary_by_user=False)
async def get_genius_user_weight_changes(
    request: Request,
    levels: Optional[str] = Query(
        None,
        description="Comma-separated genius levels (e.g., 'EXPERT,GOLD')",
    ),
    countries: Optional[str] = Query(
        None,
        description="Comma-separated country codes (e.g., 'CN,US,IN'). If not specified, returns all countries.",
    ),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    order: str = Query(
        "desc",
        description="Sort order: 'desc' for positive change first, 'asc' for negative change first",
        regex="^(desc|asc)$",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    level_list = [level.strip() for level in levels.split(",") if level.strip()] if levels else None
    country_list = [country.strip() for country in countries.split(",") if country.strip()] if countries else None
    results = await db.run_sync(
        lambda sync_db: leaderboard_service.get_genius_user_weight_changes(
            db=sync_db,
            genius_levels=level_list,
            countries=country_list,
            start_date=start_date,
            end_date=end_date,
            order=order,
        )
    )

    return [GeniusUserWeightChangeResponse(**item) for item in results]


@router.get("/genius-user-weight-timeseries", response_model=UserWeightTimeSeriesResponse)
@cache_response("leaderboard:genius-user-weight-timeseries", vary_by_user=False)
async def get_genius_user_weight_timeseries(
    request: Request,
    user: str = Query(..., description="User ID (WQ_ID)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_user_weight_time_series(
            db=sync_db,
            user=user,
            start_date=start_date,
            end_date=end_date,
        )
    )
    return UserWeightTimeSeriesResponse(**data)


@router.get("/genius-level-weight-changes", response_model=List[GeniusLevelWeightChangeResponse])
@cache_response("leaderboard:genius-level-weight-changes", vary_by_user=False)
async def get_genius_level_weight_changes(
    request: Request,
    days: int = Query(7, description="Days to look back for change calculation", ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    results = await db.run_sync(
        lambda sync_db: leaderboard_service.get_genius_level_weight_changes(
            db=sync_db,
            days=days,
        )
    )
    return [GeniusLevelWeightChangeResponse(**item) for item in results]
