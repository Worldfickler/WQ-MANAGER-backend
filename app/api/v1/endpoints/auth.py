from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import create_access_token, get_current_user
from app.models.user import SystemUser
from app.schemas.auth import LoginRequest, UserResponse
from app.services import auth_service

router = APIRouter()


@router.post("/login")
async def login(request: LoginRequest, db: AsyncSession = Depends(get_db)):
    try:
        success, user, message = await auth_service.authenticate_user(db, request.wq_id)
        if not success or user is None:
            return {"success": False, "message": message}

        access_token = create_access_token(data={"sub": user.wq_id, "user_id": user.id})
        return {
            "success": True,
            "message": message,
            "access_token": access_token,
            "token_type": "bearer",
            "wq_id": user.wq_id,
            "username": user.username,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Login failed") from exc


@router.get("/user/me", response_model=UserResponse)
async def get_current_user_info(current_user: SystemUser = Depends(get_current_user)):
    return current_user
