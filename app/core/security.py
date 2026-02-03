from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.models.user import SystemUser

security = HTTPBearer()


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    """Decode JWT access token."""
    try:
        return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> SystemUser:
    """Resolve current user from JWT."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    payload = decode_access_token(credentials.credentials)
    if payload is None:
        raise credentials_exception

    wq_id = payload.get("sub")
    if not wq_id:
        raise credentials_exception

    result = await db.execute(
        select(SystemUser).where(
            SystemUser.wq_id == wq_id,
            SystemUser.delete_flag == False,
            SystemUser.is_active == True,
        )
    )
    user = result.scalars().first()
    if user is None:
        raise credentials_exception

    return user


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)),
    db: AsyncSession = Depends(get_db),
) -> Optional[SystemUser]:
    """Optional auth dependency."""
    if credentials is None:
        return None

    try:
        payload = decode_access_token(credentials.credentials)
        if payload is None:
            return None

        wq_id = payload.get("sub")
        if not wq_id:
            return None

        result = await db.execute(
            select(SystemUser).where(
                SystemUser.wq_id == wq_id,
                SystemUser.delete_flag == False,
                SystemUser.is_active == True,
            )
        )
        return result.scalars().first()
    except Exception:
        return None
