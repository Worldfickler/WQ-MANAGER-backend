from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.user import SystemUser
from app.schemas.sponsor import SponsorDonorListResponse
from app.services import sponsor_service

router = APIRouter()


@router.get("/donors", response_model=SponsorDonorListResponse)
async def get_sponsor_donors(
    db: AsyncSession = Depends(get_db),
    _current_user: SystemUser = Depends(get_current_user),
):
    donors = await sponsor_service.get_donor_ids(db)
    return SponsorDonorListResponse(donors=donors)
