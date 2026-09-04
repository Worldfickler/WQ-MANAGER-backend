from pydantic import BaseModel, Field


class SponsorDonorListResponse(BaseModel):
    donors: list[str] = Field(default_factory=list, description="赞助者 WQ_ID 列表")
