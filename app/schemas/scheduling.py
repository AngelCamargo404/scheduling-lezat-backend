from datetime import datetime

from pydantic import BaseModel


class SchedulingSlot(BaseModel):
    id: str
    starts_at: datetime
    ends_at: datetime
    available: bool


class SchedulingSlotsResponse(BaseModel):
    items: list[SchedulingSlot]
