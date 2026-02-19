from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class TranscriptionProvider(StrEnum):
    fireflies = "fireflies"
    read_ai = "read_ai"


class TranscriptionWebhookResponse(BaseModel):
    status: str = "accepted"
    provider: TranscriptionProvider
    event_type: str | None = None
    meeting_id: str | None = None
    client_reference_id: str | None = None
    transcript_id: str | None = None
    meeting_platform: str | None = None
    is_google_meet: bool
    transcript_text_available: bool
    enrichment_status: str | None = None
    enrichment_error: str | None = None
    action_items_sync_status: str | None = None
    action_items_created_count: int | None = None
    stored_record_id: str | None = None
    received_at: datetime


class TranscriptionSentence(BaseModel):
    index: int | None = None
    speaker_name: str | None = None
    speaker_id: str | None = None
    text: str
    start_time: float | None = None
    end_time: float | None = None


class TranscriptionParticipant(BaseModel):
    name: str | None = None
    email: str | None = None
    external_id: str | None = None
    role: str | None = None


class TranscriptionRecord(BaseModel):
    id: str
    provider: TranscriptionProvider
    event_type: str | None = None
    meeting_id: str | None = None
    client_reference_id: str | None = None
    transcript_id: str | None = None
    meeting_platform: str | None = None
    is_google_meet: bool
    transcript_text_available: bool
    transcript_text: str | None = None
    transcript_sentences: list[TranscriptionSentence] = Field(default_factory=list)
    participants: list[TranscriptionParticipant] = Field(default_factory=list)
    participant_emails: list[str] = Field(default_factory=list)
    enrichment_status: str | None = None
    enrichment_error: str | None = None
    action_items_sync: dict[str, Any] | None = None
    fireflies_transcript: dict[str, Any] | None = None
    read_ai_transcript: dict[str, Any] | None = None
    raw_payload: dict[str, Any]
    received_at: datetime


class TranscriptionRecordsResponse(BaseModel):
    items: list[TranscriptionRecord]


class TranscriptionBackfillResponse(BaseModel):
    meeting_id: str
    updated_count: int
    record: TranscriptionRecord
