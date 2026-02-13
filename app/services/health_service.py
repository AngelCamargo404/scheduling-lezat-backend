from datetime import UTC, datetime

from app.core.config import Settings
from app.schemas.health import HealthResponse


class HealthService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def get_status(self) -> HealthResponse:
        return HealthResponse(
            status="ok",
            service=self.settings.app_name,
            timestamp=datetime.now(UTC),
        )
