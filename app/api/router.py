from fastapi import APIRouter

from app.api.routes.auth import router as auth_router
from app.api.routes.health import router as health_router
from app.api.routes.integrations import router as integrations_router
from app.api.routes.scheduling import router as scheduling_router
from app.api.routes.transcriptions import router as transcriptions_router

api_router = APIRouter()
v1_router = APIRouter(prefix="/v1")

api_router.include_router(health_router)

# Transitional unversioned routes used by current frontend.
api_router.include_router(auth_router)
api_router.include_router(integrations_router)
api_router.include_router(scheduling_router)
api_router.include_router(transcriptions_router)

# Versioned routes for long-term API evolution.
v1_router.include_router(auth_router)
v1_router.include_router(integrations_router)
v1_router.include_router(scheduling_router)
v1_router.include_router(transcriptions_router)
api_router.include_router(v1_router)
