from fastapi import APIRouter

from app.schemas.scheduling import SchedulingSlotsResponse
from app.services.scheduling_service import SchedulingService

router = APIRouter(prefix="/scheduling", tags=["scheduling"])


@router.get("/slots", response_model=SchedulingSlotsResponse)
def get_slots() -> SchedulingSlotsResponse:
    service = SchedulingService()
    return service.list_slots()
