from datetime import UTC, datetime, timedelta

from app.schemas.scheduling import SchedulingSlot, SchedulingSlotsResponse


class SchedulingService:
    def list_slots(self) -> SchedulingSlotsResponse:
        now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        items = [
            SchedulingSlot(
                id="slot-1",
                starts_at=now + timedelta(hours=1),
                ends_at=now + timedelta(hours=2),
                available=True,
            ),
            SchedulingSlot(
                id="slot-2",
                starts_at=now + timedelta(hours=2),
                ends_at=now + timedelta(hours=3),
                available=False,
            ),
        ]
        return SchedulingSlotsResponse(items=items)
