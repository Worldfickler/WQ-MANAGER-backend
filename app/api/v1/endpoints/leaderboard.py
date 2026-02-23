from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
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
    UserDailyOsmosisTimeSeriesResponse,
    UserWeightData,
    CombinedAnalysisResponse,
    CombinedUserChangePageResponse,
    ConsultantMergedPageResponse,
    ValueFactorAnalysisResponse,
    ValueFactorUserChangePageResponse,
    UserMetricTrendResponse,
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


@router.get("/consultant-user-daily-osmosis-timeseries", response_model=UserDailyOsmosisTimeSeriesResponse)
@cache_response("leaderboard:consultant-user-daily-osmosis-timeseries", vary_by_user=False)
async def get_consultant_user_daily_osmosis_timeseries(
    request: Request,
    user: str = Query(..., description="User ID (WQ_ID)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_user_daily_osmosis_time_series(
            db=sync_db,
            user=user,
            start_date=start_date,
            end_date=end_date,
        )
    )
    return UserDailyOsmosisTimeSeriesResponse(**data)


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


@router.get("/combined-analysis", response_model=CombinedAnalysisResponse)
@cache_response("leaderboard:combined-analysis", vary_by_user=False)
async def get_combined_analysis(
    request: Request,
    countries: Optional[str] = Query(
        None,
        description="Comma-separated country codes (e.g., 'CN,US,IN')",
    ),
    levels: Optional[str] = Query(
        None,
        description="Comma-separated genius levels (e.g., 'EXPERT,GOLD')",
    ),
    exclude_alpha_both_zero: bool = Query(
        False,
        description="Exclude rows where base and target combined_alpha_performance are both 0",
    ),
    exclude_power_pool_both_zero: bool = Query(
        False,
        description="Exclude rows where base and target combined_power_pool_alpha_performance are both 0",
    ),
    exclude_selected_both_zero: bool = Query(
        False,
        description="Exclude rows where base and target combined_selected_alpha_performance are both 0",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    country_list = [country.strip() for country in countries.split(",") if country.strip()] if countries else None
    level_list = [level.strip() for level in levels.split(",") if level.strip()] if levels else None
    data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_combined_analysis(
            sync_db,
            countries=country_list,
            genius_levels=level_list,
            exclude_alpha_both_zero=exclude_alpha_both_zero,
            exclude_power_pool_both_zero=exclude_power_pool_both_zero,
            exclude_selected_both_zero=exclude_selected_both_zero,
        )
    )
    return CombinedAnalysisResponse(**data)


@router.get("/combined-user-changes", response_model=CombinedUserChangePageResponse)
@cache_response("leaderboard:combined-user-changes", vary_by_user=False)
async def get_combined_user_changes(
    request: Request,
    sort_by: str = Query(
        "alpha_change",
        description=(
            "Sort field: alpha_change|power_pool_change|selected_change|"
            "base_alpha|target_alpha|base_power_pool|target_power_pool|base_selected|target_selected"
        ),
        regex="^(alpha_change|power_pool_change|selected_change|base_alpha|target_alpha|base_power_pool|target_power_pool|base_selected|target_selected)$",
    ),
    sort_order: str = Query(
        "desc",
        description="Sort order: desc | asc",
        regex="^(desc|asc)$",
    ),
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(20, description="Items per page", ge=1, le=100),
    countries: Optional[str] = Query(None, description="Comma-separated countries"),
    levels: Optional[str] = Query(None, description="Comma-separated genius levels"),
    exclude_alpha_both_zero: bool = Query(
        False,
        description="Exclude rows where base and target combined_alpha_performance are both 0",
    ),
    exclude_power_pool_both_zero: bool = Query(
        False,
        description="Exclude rows where base and target combined_power_pool_alpha_performance are both 0",
    ),
    exclude_selected_both_zero: bool = Query(
        False,
        description="Exclude rows where base and target combined_selected_alpha_performance are both 0",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    country_list = [country.strip() for country in countries.split(",") if country.strip()] if countries else None
    level_list = [level.strip() for level in levels.split(",") if level.strip()] if levels else None
    data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_combined_user_changes(
            sync_db,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
            countries=country_list,
            genius_levels=level_list,
            exclude_alpha_both_zero=exclude_alpha_both_zero,
            exclude_power_pool_both_zero=exclude_power_pool_both_zero,
            exclude_selected_both_zero=exclude_selected_both_zero,
        )
    )
    return CombinedUserChangePageResponse(**data)


@router.get("/consultant-merged-page", response_model=ConsultantMergedPageResponse)
@cache_response("leaderboard:consultant-merged-page", vary_by_user=False)
async def get_consultant_merged_page(
    request: Request,
    record_date: Optional[str] = Query(None, description="Record date (YYYY-MM-DD)"),
    countries: Optional[str] = Query(None, description="Comma-separated countries"),
    levels: Optional[str] = Query(None, description="Comma-separated genius levels"),
    user_keyword: Optional[str] = Query(None, description="Search by user id"),
    sort_by: str = Query(
        "user",
        description=(
            "Sort field: user|country|university|genius_level|best_level|"
            "weight_factor|value_factor|daily_osmosis_rank|data_fields_used|submissions_count|"
            "mean_prod_correlation|mean_self_correlation|super_alpha_submissions_count|"
            "super_alpha_mean_prod_correlation|super_alpha_mean_self_correlation|"
            "alpha_count|pyramid_count|combined_alpha_performance|"
            "combined_power_pool_alpha_performance|combined_selected_alpha_performance|"
            "operator_count|operator_avg|field_count|field_avg|community_activity|"
            "max_simulation_streak|record_coverage"
        ),
        regex="^(user|country|university|genius_level|best_level|weight_factor|value_factor|daily_osmosis_rank|data_fields_used|submissions_count|mean_prod_correlation|mean_self_correlation|super_alpha_submissions_count|super_alpha_mean_prod_correlation|super_alpha_mean_self_correlation|alpha_count|pyramid_count|combined_alpha_performance|combined_power_pool_alpha_performance|combined_selected_alpha_performance|operator_count|operator_avg|field_count|field_avg|community_activity|max_simulation_streak|record_coverage)$",
    ),
    sort_order: str = Query(
        "asc",
        description="Sort order: desc | asc",
        regex="^(desc|asc)$",
    ),
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(20, description="Items per page", ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    parsed_date = None
    if record_date:
        try:
            parsed_date = datetime.strptime(record_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="record_date must be in YYYY-MM-DD format")

    country_list = [country.strip() for country in countries.split(",") if country.strip()] if countries else None
    level_list = [level.strip() for level in levels.split(",") if level.strip()] if levels else None

    data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_consultant_merged_page(
            sync_db,
            record_date=parsed_date,
            countries=country_list,
            genius_levels=level_list,
            user_keyword=user_keyword,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
        )
    )
    return ConsultantMergedPageResponse(**data)


@router.get("/value-factor-analysis", response_model=ValueFactorAnalysisResponse)
@cache_response("leaderboard:value-factor-analysis", vary_by_user=False)
async def get_value_factor_analysis(
    request: Request,
    exclude_both_half: bool = Query(
        False,
        description="Exclude rows where base and target value_factor are both 0.5",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_value_factor_analysis(
            sync_db,
            exclude_both_half=exclude_both_half,
        )
    )
    return ValueFactorAnalysisResponse(**data)


@router.get("/value-factor-user-changes", response_model=ValueFactorUserChangePageResponse)
@cache_response("leaderboard:value-factor-user-changes", vary_by_user=False)
async def get_value_factor_user_changes(
    request: Request,
    order: Optional[str] = Query(
        None,
        description="Deprecated: sort order. Use sort_order instead.",
        regex="^(desc|asc)$",
    ),
    sort_by: str = Query(
        "change",
        description="Sort field: change | base_value_factor | target_value_factor",
        regex="^(change|base_value_factor|target_value_factor)$",
    ),
    sort_order: str = Query(
        "desc",
        description="Sort order: desc | asc",
        regex="^(desc|asc)$",
    ),
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(20, description="Items per page", ge=1, le=100),
    country: Optional[str] = Query(None, description="Deprecated: filter by single country"),
    countries: Optional[str] = Query(None, description="Comma-separated countries"),
    genius_levels: Optional[str] = Query(
        None,
        description="Comma-separated genius levels (e.g., 'EXPERT,GOLD')",
    ),
    exclude_both_half: bool = Query(
        False,
        description="Exclude rows where base and target value_factor are both 0.5",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    effective_sort_order = order or sort_order
    level_list = [level.strip() for level in genius_levels.split(",") if level.strip()] if genius_levels else None
    country_list = [item.strip() for item in countries.split(",") if item.strip()] if countries else None
    if not country_list and country:
        country_list = [country]
    data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_value_factor_user_changes(
            sync_db,
            sort_by=sort_by,
            sort_order=effective_sort_order,
            page=page,
            page_size=page_size,
            countries=country_list,
            genius_levels=level_list,
            exclude_both_half=exclude_both_half,
        )
    )
    return ValueFactorUserChangePageResponse(**data)


@router.get("/user-metric-trends", response_model=UserMetricTrendResponse)
@cache_response("leaderboard:user-metric-trends", vary_by_user=False)
async def get_user_metric_trends(
    request: Request,
    user: str = Query(..., description="WQ ID"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    data = await db.run_sync(
        lambda sync_db: leaderboard_service.get_user_metric_trends_by_event(
            sync_db,
            user=user,
        )
    )
    return UserMetricTrendResponse(
        user=user,
        value_factor_trend=data.get("value_factor_trend", []),
        combined_trend=data.get("combined_trend", []),
    )
