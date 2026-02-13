from fastapi import APIRouter

from app.core.config import get_settings
from app.schemas.health import HealthResponse
from app.services.health_service import HealthService

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    settings = get_settings()
    service = HealthService(settings)
    return service.get_status()
