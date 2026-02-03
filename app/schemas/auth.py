from pydantic import BaseModel, Field
from typing import Optional


class LoginRequest(BaseModel):
    """登录请求"""
    wq_id: str = Field(..., min_length=1, max_length=32, description="WorldQuant用户ID")


class LoginResponse(BaseModel):
    """登录响应"""
    success: bool
    message: str
    wq_id: Optional[str] = None
    username: Optional[str] = None


class UserResponse(BaseModel):
    """用户信息响应"""
    id: int
    wq_id: str
    username: Optional[str] = None
    email: Optional[str] = None
    is_active: bool

    class Config:
        from_attributes = True
