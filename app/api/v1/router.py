from fastapi import APIRouter

from app.api.v1.endpoints import auth, dashboard, leaderboard, user, feedback

api_router = APIRouter()

api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(user.router, prefix="/user", tags=["user"])
api_router.include_router(leaderboard.router, prefix="/leaderboard", tags=["leaderboard"])
api_router.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
api_router.include_router(feedback.router, prefix="/feedback", tags=["feedback"])
