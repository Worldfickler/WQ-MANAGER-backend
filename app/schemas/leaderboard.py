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

