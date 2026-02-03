from pydantic import BaseModel
from typing import Optional, List, Generic, TypeVar


class CountryRankingData(BaseModel):
    """国家排名数据"""
    country: str
    user_count: int
    weight_factor: float
    value_factor: Optional[float] = None
    submissions_count: int
    super_alpha_submissions_count: int
    total_submissions: int
    mean_prod_correlation: Optional[float] = None
    mean_self_correlation: Optional[float] = None
    super_alpha_mean_prod_correlation: Optional[float] = None
    super_alpha_mean_self_correlation: Optional[float] = None
    # 变化值
    weight_change: Optional[float] = None
    value_change: Optional[float] = None
    submissions_change: Optional[int] = None
    super_alpha_submissions_change: Optional[int] = None
    total_submissions_change: Optional[int] = None
    prod_corr_change: Optional[float] = None
    self_corr_change: Optional[float] = None


class CountryHistoryData(BaseModel):
    """国家历史数据"""
    record_date: str
    user_count: int
    weight_factor: float
    value_factor: Optional[float] = None
    submissions_count: int
    super_alpha_submissions_count: int
    total_submissions: int
    mean_prod_correlation: Optional[float] = None
    mean_self_correlation: Optional[float] = None
    super_alpha_mean_prod_correlation: Optional[float] = None
    super_alpha_mean_self_correlation: Optional[float] = None


class UniversityRankingData(BaseModel):
    """大学排名数据"""
    university: str
    user_count: int
    avg_weight: float
    max_weight: float
    total_submissions: int


class UserWeightRankingData(BaseModel):
    """用户权重排名数据"""
    rank: int
    user: str
    weight_factor: float
    value_factor: Optional[float] = None
    total_submissions: int
    country: Optional[str] = None
    university: Optional[str] = None


class UserWeightChangeRankingData(BaseModel):
    """用户权重变化排名数据"""
    rank: int
    user: str
    current_weight: float
    weight_change: float
    country: Optional[str] = None
    university: Optional[str] = None


class UserSubmissionsRankingData(BaseModel):
    """用户提交数排名数据"""
    rank: int
    user: str
    weight_factor: Optional[float] = None
    regular_submissions: int
    super_alpha_submissions: int
    total_submissions: int
    country: Optional[str] = None
    university: Optional[str] = None


class UserCorrelationRankingData(BaseModel):
    """用户相关性排名数据"""
    rank: int
    user: str
    weight_factor: Optional[float] = None
    regular_correlation: Optional[float] = None
    super_alpha_correlation: Optional[float] = None
    avg_correlation: float
    country: Optional[str] = None
    university: Optional[str] = None


# 分页响应模型
T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    """分页响应"""
    data: List[T]
    total: int
    page: int
    page_size: int
    total_pages: int
