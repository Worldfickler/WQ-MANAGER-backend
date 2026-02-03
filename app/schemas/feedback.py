from typing import Optional, Literal

from pydantic import BaseModel, Field


class FeedbackCreate(BaseModel):
    content: str = Field(..., min_length=1, max_length=2000, description="反馈内容")
    feedback_type: Literal["bug", "optimize", "request"] = Field(
        "bug",
        description="反馈类型: bug/optimize/request",
    )
    page: Optional[str] = Field(None, max_length=200, description="页面路径")
    contact: Optional[str] = Field(None, max_length=200, description="联系方式")


class FeedbackResponse(BaseModel):
    success: bool
    message: str
