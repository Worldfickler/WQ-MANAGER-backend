from pydantic import BaseModel, Field


class UserPageAuthStatusResponse(BaseModel):
    page_key: str
    is_set: bool


class UserPageAuthSetRequest(BaseModel):
    auth_code: str = Field(..., description="页面访问口令")


class UserPageAuthSetResponse(BaseModel):
    success: bool
    message: str
    page_key: str


class UserPageAuthVerifyRequest(BaseModel):
    auth_code: str = Field(..., description="页面访问口令")


class UserPageAuthVerifyResponse(BaseModel):
    page_key: str
    verified: bool
    message: str
    access_grant_token: str | None = None
    expires_at: str | None = None
