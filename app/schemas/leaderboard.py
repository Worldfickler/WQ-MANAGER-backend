from pydantic import BaseModel
from datetime import date
from typing import Optional


class SummaryStatistics(BaseModel):
    total_users: int
    user_change: int
    total_alpha: int
    alpha_change: int
    total_weight: float
    weight_change: float
    total_records: int
    latest_record_date: Optional[str] = None


class CountryWeightData(BaseModel):
    record_date: date
    country: str
    weight_factor: Optional[float]
    user: Optional[int]
    value_factor: Optional[float]
    submissions_count: Optional[int]
    weight_change: Optional[float] = None
    weight_change_percent: Optional[float] = None

    class Config:
        from_attributes = True


class CountryWeightTimeSeriesResponse(BaseModel):
    country: str
    dates: list[str]
    weights: list[float]


class CountrySubmissionTimeSeriesResponse(BaseModel):
    """鍥藉鎻愪氦鏁伴噺鏃堕棿搴忓垪鍝嶅簲"""
    country: str
    dates: list[str]
    submissions_count: list[int]
    super_alpha_submissions_count: list[int]
    submissions_change: list[int]
    super_alpha_submissions_change: list[int]

class GeniusCountryTimeSeriesResponse(BaseModel):
    """Genius鍥藉鏃堕棿搴忓垪鍝嶅簲"""
    country: str
    dates: list[str]
    alpha_count_change: list[int]

class GeniusWeightTimeSeriesResponse(BaseModel):
    genius_level: str
    country: str
    dates: list[str]
    weights: list[float]


class GeniusUserWeightChangeResponse(BaseModel):
    user: str
    genius_level: Optional[str]
    country: Optional[str]
    start_weight: float
    end_weight: float
    weight_change: float
    weight_change_percent: Optional[float] = None
    rank: int
    percentile: float


class UserWeightTimeSeriesResponse(BaseModel):
    user: str
    dates: list[str]
    weights: list[float]


class UserDailyOsmosisTimeSeriesResponse(BaseModel):
    user: str
    dates: list[str]
    daily_osmosis_ranks: list[float]


class GeniusLevelWeightChangeResponse(BaseModel):
    genius_level: str
    total_users: int
    total_weight: float
    weight_change: float
    weight_change_percent: Optional[float] = None

class UserWeightData(BaseModel):
    record_date: date
    user: str
    weight_factor: Optional[float]
    value_factor: Optional[float]
    submissions_count: Optional[int]
    university: Optional[str] = None
    country: Optional[str] = None
    weight_change: Optional[float] = None
    weight_change_percent: Optional[float] = None

    class Config:
        from_attributes = True


class ValueFactorSummary(BaseModel):
    users_on_target_date: int
    users_on_base_date: int
    comparable_users: int
    new_users: int
    missing_users: int
    increased_users: int
    decreased_users: int
    unchanged_users: int
    avg_target_value_factor: float
    avg_base_value_factor: float
    avg_change: float
    median_change: float
    max_increase: float
    max_decrease: float


class ValueFactorDimensionItem(BaseModel):
    dimension: str
    comparable_users: int
    avg_target_value_factor: float
    avg_base_value_factor: float
    avg_change: float
    median_change: float
    increased_users: int
    decreased_users: int
    unchanged_users: int


class ValueFactorUserChangeItem(BaseModel):
    user: str
    country: Optional[str] = None
    university: Optional[str] = None
    genius_level: Optional[str] = None
    base_value_factor: float
    target_value_factor: float
    change: float


class ValueFactorDistribution(BaseModel):
    labels: list[str]
    counts: list[int]


class ValueFactorUserChangePageResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[ValueFactorUserChangeItem]


class ValueFactorAnalysisResponse(BaseModel):
    base_record_date: str
    target_record_date: str
    summary: ValueFactorSummary
    by_country: list[ValueFactorDimensionItem]
    by_university: list[ValueFactorDimensionItem]
    top_gainers: list[ValueFactorUserChangeItem]
    top_decliners: list[ValueFactorUserChangeItem]
    distribution: ValueFactorDistribution


class CombinedSummary(BaseModel):
    users_on_target_date: int
    users_on_base_date: int
    comparable_users: int
    new_users: int
    missing_users: int


class CombinedMetricSummary(BaseModel):
    metric: str
    display_name: str
    avg_target: float
    avg_base: float
    avg_change: float
    median_change: float
    max_increase: float
    max_decrease: float
    increased_users: int
    decreased_users: int
    unchanged_users: int


class CombinedDistribution(BaseModel):
    labels: list[str]
    counts: list[int]


class CombinedAnalysisResponse(BaseModel):
    base_record_date: str
    target_record_date: str
    summary: CombinedSummary
    metric_summaries: list[CombinedMetricSummary]
    distributions: dict[str, CombinedDistribution]


class CombinedUserChangeItem(BaseModel):
    user: str
    country: Optional[str] = None
    genius_level: Optional[str] = None
    base_alpha: float
    target_alpha: float
    alpha_change: float
    base_power_pool: float
    target_power_pool: float
    power_pool_change: float
    base_selected: float
    target_selected: float
    selected_change: float


class CombinedUserChangePageResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[CombinedUserChangeItem]


class ConsultantMergedSummary(BaseModel):
    total_users: int
    consultant_users: int
    genius_users: int
    matched_users: int
    country_count: int
    genius_level_count: int


class ConsultantMergedRow(BaseModel):
    user: str
    country: Optional[str] = None
    consultant_country: Optional[str] = None
    genius_country: Optional[str] = None
    university: Optional[str] = None

    has_consultant_record: bool
    has_genius_record: bool

    weight_factor: Optional[float] = None
    value_factor: Optional[float] = None
    daily_osmosis_rank: Optional[float] = None
    data_fields_used: Optional[int] = None
    submissions_count: Optional[int] = None
    mean_prod_correlation: Optional[float] = None
    mean_self_correlation: Optional[float] = None
    super_alpha_submissions_count: Optional[int] = None
    super_alpha_mean_prod_correlation: Optional[float] = None
    super_alpha_mean_self_correlation: Optional[float] = None

    genius_rank: Optional[int] = None
    genius_level: Optional[str] = None
    best_level: Optional[str] = None
    alpha_count: Optional[int] = None
    pyramid_count: Optional[int] = None
    combined_alpha_performance: Optional[float] = None
    combined_power_pool_alpha_performance: Optional[float] = None
    combined_selected_alpha_performance: Optional[float] = None
    operator_count: Optional[int] = None
    operator_avg: Optional[float] = None
    field_count: Optional[int] = None
    field_avg: Optional[float] = None
    community_activity: Optional[float] = None
    max_simulation_streak: Optional[int] = None


class ConsultantMergedPageResponse(BaseModel):
    record_date: Optional[str] = None
    available_record_dates: list[str]
    summary: ConsultantMergedSummary
    total: int
    page: int
    page_size: int
    items: list[ConsultantMergedRow]


class ValueFactorTrendPoint(BaseModel):
    update_date: str
    date_range: str
    value_factor: Optional[float] = None


class CombinedTrendPoint(BaseModel):
    update_date: str
    date_range: str
    combined_alpha_performance: Optional[float] = None
    combined_power_pool_alpha_performance: Optional[float] = None
    combined_selected_alpha_performance: Optional[float] = None


class UserMetricTrendResponse(BaseModel):
    user: str
    value_factor_trend: list[ValueFactorTrendPoint]
    combined_trend: list[CombinedTrendPoint]

