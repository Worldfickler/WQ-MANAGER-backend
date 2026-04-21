from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, Field


class BasePaymentUploadRequest(BaseModel):
    record_date: date = Field(..., description="记录日期（YYYY-MM-DD）")
    anonymity: Literal[0, 1] = Field(1, description="是否匿名（0-匿名，1-不匿名）")
    regular_payment: float = Field(..., description="regular收益")
    super_payment: float = Field(..., description="super收益")
    regular_count: Optional[int] = Field(None, description="regular数量")
    super_count: Optional[int] = Field(None, description="super数量")
    picture: Optional[str] = Field(None, description="图片url")
    pictures: Optional[list[str]] = Field(None, description="图片url列表（支持多图）")
    value_factor: Optional[float] = Field(None, description="value factor值")
    daily_osmosis_rank: Optional[float] = Field(None, description="osmosis每日分数")


class BasePaymentRecordResponse(BaseModel):
    record_date: str
    wq_id: str
    display_wq_id: str
    anonymity: int
    regular_payment: Optional[float] = None
    super_payment: Optional[float] = None
    total_payment: Optional[float] = None
    regular_count: Optional[int] = None
    super_count: Optional[int] = None
    picture: Optional[str] = None
    pictures: list[str] = Field(default_factory=list)
    picture_urls: list[str] = Field(default_factory=list)
    value_factor: Optional[float] = None
    daily_osmosis_rank: Optional[float] = None
    is_owner: bool = False


class BasePaymentUploadResponse(BaseModel):
    success: bool
    message: str
    data: BasePaymentRecordResponse


class BasePaymentConsultantDefaultsResponse(BaseModel):
    record_date: str
    value_factor: Optional[float] = None
    daily_osmosis_rank: Optional[float] = None


class BasePaymentMyStatusResponse(BaseModel):
    has_uploaded_for_date: bool
    record_date: str
    data: Optional[BasePaymentRecordResponse] = None
    consultant_defaults: Optional[BasePaymentConsultantDefaultsResponse] = None


class BasePaymentLeaderboardItem(BaseModel):
    rank: int
    record_date: str
    wq_id: str
    display_wq_id: str
    anonymity: int
    regular_payment: Optional[float] = None
    super_payment: Optional[float] = None
    total_payment: Optional[float] = None
    regular_count: Optional[int] = None
    super_count: Optional[int] = None
    picture: Optional[str] = None
    pictures: list[str] = Field(default_factory=list)
    picture_urls: list[str] = Field(default_factory=list)
    value_factor: Optional[float] = None
    daily_osmosis_rank: Optional[float] = None
    is_owner: bool = False


class BasePaymentLeaderboardResponse(BaseModel):
    record_date: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    sort_by: Literal[
        "total_payment",
        "regular_payment",
        "super_payment",
        "regular_count",
        "super_count",
        "value_factor",
        "daily_osmosis_rank",
    ] = "total_payment"
    sort_order: Literal["desc", "asc"] = "desc"
    total: int
    page: int
    page_size: int
    items: list[BasePaymentLeaderboardItem]


class BasePaymentDashboardTopItem(BaseModel):
    rank: int
    display_wq_id: str
    total_payment: Optional[float] = None
    regular_payment: Optional[float] = None
    super_payment: Optional[float] = None
    value_factor: Optional[float] = None
    daily_osmosis_rank: Optional[float] = None


class BasePaymentDashboardOverview(BaseModel):
    participant_count: int
    total_payment_sum: float
    regular_payment_sum: float
    super_payment_sum: float
    regular_share_pct: float
    super_share_pct: float
    average_total_payment: Optional[float] = None
    max_total_payment: Optional[float] = None
    min_total_payment: Optional[float] = None
    positive_count: int
    negative_count: int
    flat_count: int
    positive_rate_pct: float
    anonymity_count: int
    anonymity_rate_pct: float
    picture_count: int
    picture_rate_pct: float
    average_regular_count: Optional[float] = None
    average_super_count: Optional[float] = None
    average_value_factor: Optional[float] = None
    average_daily_osmosis_rank: Optional[float] = None


class BasePaymentDashboardResponse(BaseModel):
    record_date: str
    overview: BasePaymentDashboardOverview
    top_performers: list[BasePaymentDashboardTopItem]


class BasePaymentImageUploadItem(BaseModel):
    file_name: str
    object_name: str
    content_type: str
    size: int
    url: str


class BasePaymentImageUploadResponse(BaseModel):
    success: bool
    message: str
    items: list[BasePaymentImageUploadItem]
