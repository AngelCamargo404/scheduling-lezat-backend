import hashlib
import hmac
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status

from app.core.config import Settings
from app.schemas.transcription import (
    TranscriptionBackfillResponse,
    TranscriptionProvider,
    TranscriptionRecord,
    TranscriptionRecordsResponse,
    TranscriptionSentence,
    TranscriptionWebhookResponse,
)
from app.services.action_item_creation_store import (
    ActionItemCreationStore,
    build_action_item_creation_record,
    create_action_item_creation_store,
)
from app.services.action_item_sync_service import (
    ActionItemSyncService,
    extract_sentences_for_action_items,
    sanitize_action_item_participants,
)
from app.services.fireflies_api_client import FirefliesApiClient, FirefliesApiError
from app.services.transcription_store import (
    TranscriptionStore,
    build_transcription_document,
    create_transcription_store,
)
from app.services.user_store import UserStore, create_user_store


class TranscriptionService:
    def __init__(
        self,
        settings: Settings,
        store: TranscriptionStore | None = None,
        fireflies_client: FirefliesApiClient | None = None,
        action_item_sync_service: ActionItemSyncService | None = None,
        action_item_creation_store: ActionItemCreationStore | None = None,
        user_store: UserStore | None = None,
    ) -> None:
        self.settings = settings
        self.store = store or create_transcription_store(
            store_name=settings.transcriptions_store,
            mongodb_uri=settings.mongodb_uri,
            mongodb_db_name=settings.mongodb_db_name,
            mongodb_collection_name=settings.mongodb_transcriptions_collection,
            mongodb_connect_timeout_ms=settings.mongodb_connect_timeout_ms,
        )
        self.fireflies_client = fireflies_client or self._create_fireflies_client()
        self.action_item_sync_service = action_item_sync_service or ActionItemSyncService(settings)
        self.action_item_creation_store = (
            action_item_creation_store
            or create_action_item_creation_store(
                store_name=settings.transcriptions_store,
                mongodb_uri=settings.mongodb_uri,
                mongodb_db_name=settings.mongodb_db_name,
                mongodb_collection_name=settings.mongodb_action_item_creations_collection,
                mongodb_connect_timeout_ms=settings.mongodb_connect_timeout_ms,
            )
        )
        self._has_custom_action_item_sync_service = action_item_sync_service is not None
        self._user_store = user_store
        self._user_store_lookup_failed = False

    def process_webhook(
        self,
        provider: TranscriptionProvider,
        payload: Mapping[str, Any],
        shared_secret: str | None,
        raw_body: bytes | None = None,
        signature: str | None = None,
    ) -> TranscriptionWebhookResponse:
        self._validate_auth(
            provider=provider,
            shared_secret=shared_secret,
            raw_body=raw_body,
            signature=signature,
        )

        meeting_platform = self._extract_first_string(
            payload,
            paths=(
                "meeting.platform",
                "meeting.meeting_platform",
                "meeting.source",
                "platform",
                "source",
            ),
        )
        meeting_url = self._extract_first_string(
            payload,
            paths=(
                "meeting.url",
                "meeting.join_url",
                "meeting.link",
                "join_url",
                "url",
            ),
        )
        meeting_id = self._extract_first_string(
            payload,
            paths=(
                "meeting.id",
                "meeting.meeting_id",
                "meeting.external_id",
                "meetingId",
                "meeting_id",
            ),
        )
        client_reference_id = self._extract_first_string(
            payload,
            paths=("clientReferenceId", "client_reference_id"),
        )
        transcript_id = self._extract_first_string(
            payload,
            paths=(
                "transcript.id",
                "transcript.transcript_id",
                "transcriptId",
                "transcript_id",
            ),
        )
        event_type = self._extract_first_string(
            payload,
            paths=("event", "event_type", "eventType", "type"),
        )
        transcript_text = self._extract_transcript_text(payload)
        enrichment_status = "not_required"
        enrichment_error: str | None = None
        fireflies_transcript: dict[str, Any] | None = None

        if provider == TranscriptionProvider.fireflies:
            (
                transcript_text,
                transcript_id,
                meeting_url,
                fireflies_transcript,
                enrichment_status,
                enrichment_error,
            ) = self._enrich_fireflies_transcript(
                meeting_id=meeting_id,
                transcript_id=transcript_id,
                transcript_text=transcript_text,
                meeting_url=meeting_url,
            )

        transcript_sentences = extract_sentences_for_action_items(fireflies_transcript)
        participant_emails = self._extract_participant_emails(fireflies_transcript)
        if not participant_emails:
            participant_emails = self._extract_participant_emails_from_payload(payload)
        action_items_sync = self._sync_action_items(
            meeting_id=meeting_id,
            transcript_text=transcript_text,
            transcript_sentences=transcript_sentences,
            participant_emails=participant_emails,
            resolve_user_settings=True,
        )

        if not meeting_platform:
            meeting_platform = self._infer_meeting_platform_from_url(meeting_url)
        is_google_meet = self._is_google_meet(meeting_platform, meeting_url)
        received_at = datetime.now(UTC)

        normalized_document = build_transcription_document(
            provider=provider.value,
            event_type=event_type,
            meeting_id=meeting_id,
            client_reference_id=client_reference_id,
            transcript_id=transcript_id,
            meeting_platform=meeting_platform,
            is_google_meet=is_google_meet,
            transcript_text_available=transcript_text is not None,
            transcript_text=transcript_text,
            enrichment_status=enrichment_status,
            enrichment_error=enrichment_error,
            action_items_sync=action_items_sync,
            fireflies_transcript=fireflies_transcript,
            raw_payload=payload,
        )

        stored_record_id = self._save_document(normalized_document)
        self._save_action_item_creation_records(
            source="webhook",
            provider=provider.value,
            meeting_id=meeting_id,
            transcript_id=transcript_id,
            client_reference_id=client_reference_id,
            transcription_record_id=stored_record_id,
            participant_emails=participant_emails,
            action_items_sync=action_items_sync,
        )
        return TranscriptionWebhookResponse(
            provider=provider,
            event_type=event_type,
            meeting_id=meeting_id,
            client_reference_id=client_reference_id,
            transcript_id=transcript_id,
            meeting_platform=meeting_platform,
            is_google_meet=is_google_meet,
            transcript_text_available=transcript_text is not None,
            enrichment_status=enrichment_status,
            enrichment_error=enrichment_error,
            action_items_sync_status=self._to_text(action_items_sync.get("status"))
            if action_items_sync
            else None,
            action_items_created_count=self._to_int(action_items_sync.get("created_count"))
            if action_items_sync
            else None,
            stored_record_id=stored_record_id,
            received_at=received_at,
        )

    def list_received(self, limit: int = 50) -> TranscriptionRecordsResponse:
        normalized_limit = min(max(limit, 1), 200)
        try:
            raw_items = self.store.list_recent(limit=normalized_limit)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to query transcription storage.",
            ) from exc

        return TranscriptionRecordsResponse(
            items=[self._map_record(record) for record in raw_items],
        )

    def get_received(self, record_id: str) -> TranscriptionRecord:
        try:
            record = self.store.get_by_id(record_id)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to query transcription storage.",
            ) from exc

        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transcription record not found.",
            )
        return self._map_record(record)

    def get_received_by_meeting_id(self, meeting_id: str) -> TranscriptionRecord:
        try:
            record = self.store.get_latest_by_meeting_id(meeting_id)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to query transcription storage.",
            ) from exc

        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transcription record not found for meeting_id.",
            )
        return self._map_record(record)

    def backfill_by_meeting_id(self, meeting_id: str) -> TranscriptionBackfillResponse:
        current_record = self._get_latest_record_by_meeting_id(meeting_id)
        provider = TranscriptionProvider(str(current_record.get("provider", "fireflies")))
        if provider != TranscriptionProvider.fireflies:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Backfill is only supported for Fireflies records.",
            )
        if not self.fireflies_client:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="FIREFLIES_API_KEY is not configured.",
            )

        transcript_text = self._to_text(current_record.get("transcript_text"))
        transcript_id = self._to_text(current_record.get("transcript_id"))
        meeting_url = self._extract_first_string(
            current_record,
            paths=("meeting_url", "meeting_platform_url", "raw_payload.meeting_link"),
        )
        (
            transcript_text,
            transcript_id,
            meeting_url,
            fireflies_transcript,
            enrichment_status,
            enrichment_error,
        ) = self._enrich_fireflies_transcript(
            meeting_id=meeting_id,
            transcript_id=transcript_id,
            transcript_text=transcript_text,
            meeting_url=meeting_url,
        )
        transcript_sentences = extract_sentences_for_action_items(fireflies_transcript)
        participant_emails = sanitize_action_item_participants(
            self._extract_participant_emails(fireflies_transcript),
        )
        action_items_sync = self._sync_action_items(
            meeting_id=meeting_id,
            transcript_text=transcript_text,
            transcript_sentences=transcript_sentences,
            participant_emails=participant_emails,
            resolve_user_settings=True,
        )
        meeting_platform = self._to_text(current_record.get("meeting_platform"))
        if not meeting_platform:
            meeting_platform = self._infer_meeting_platform_from_url(meeting_url)
        update_payload = {
            "transcript_id": transcript_id,
            "meeting_platform": meeting_platform,
            "is_google_meet": self._is_google_meet(meeting_platform, meeting_url),
            "transcript_text_available": transcript_text is not None,
            "transcript_text": transcript_text,
            "enrichment_status": enrichment_status,
            "enrichment_error": enrichment_error,
            "action_items_sync": action_items_sync,
            "fireflies_transcript": fireflies_transcript,
            "updated_at": datetime.now(UTC),
        }

        try:
            updated_count = self.store.update_by_meeting_id(
                meeting_id=meeting_id,
                updates=update_payload,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to update transcription records.",
            ) from exc

        if updated_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transcription record not found for meeting_id.",
            )

        refreshed_record = self._get_latest_record_by_meeting_id(meeting_id)
        self._save_action_item_creation_records(
            source="backfill",
            provider=provider.value,
            meeting_id=meeting_id,
            transcript_id=transcript_id,
            client_reference_id=self._to_text(current_record.get("client_reference_id")),
            transcription_record_id=self._to_text(refreshed_record.get("_id")),
            participant_emails=participant_emails,
            action_items_sync=action_items_sync,
        )
        return TranscriptionBackfillResponse(
            meeting_id=meeting_id,
            updated_count=updated_count,
            record=self._map_record(refreshed_record),
        )

    def _validate_auth(
        self,
        provider: TranscriptionProvider,
        shared_secret: str | None,
        raw_body: bytes | None,
        signature: str | None,
    ) -> None:
        expected_secret = ""
        if provider == TranscriptionProvider.fireflies:
            expected_secret = self.settings.fireflies_webhook_secret
        elif provider == TranscriptionProvider.read_ai:
            expected_secret = self.settings.read_ai_webhook_secret

        if not expected_secret:
            return

        if provider == TranscriptionProvider.fireflies:
            if raw_body and signature and self._is_valid_hmac_signature(
                payload=raw_body,
                signature=signature,
                secret=expected_secret,
            ):
                return
            if shared_secret == expected_secret:
                return
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid webhook signature.",
            )

        if shared_secret == expected_secret:
            return

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook secret.",
        )

    def _is_valid_hmac_signature(self, payload: bytes, signature: str, secret: str) -> bool:
        provided_signature = signature.strip()
        if not provided_signature:
            return False
        if provided_signature.startswith("sha256="):
            provided_signature = provided_signature.split("=", maxsplit=1)[1].strip()

        computed_signature = hmac.new(
            key=secret.encode("utf-8"),
            msg=payload,
            digestmod=hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(computed_signature, provided_signature)

    def _save_document(self, document: Mapping[str, Any]) -> str:
        try:
            return self.store.save(document)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to persist transcription.",
            ) from exc

    def _get_latest_record_by_meeting_id(self, meeting_id: str) -> Mapping[str, Any]:
        try:
            record = self.store.get_latest_by_meeting_id(meeting_id)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to query transcription storage.",
            ) from exc

        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transcription record not found for meeting_id.",
            )
        return record

    def _enrich_fireflies_transcript(
        self,
        meeting_id: str | None,
        transcript_id: str | None,
        transcript_text: str | None,
        meeting_url: str | None,
    ) -> tuple[str | None, str | None, str | None, dict[str, Any] | None, str, str | None]:
        if not meeting_id:
            return (
                transcript_text,
                transcript_id,
                meeting_url,
                None,
                "failed_missing_meeting_id",
                "Webhook payload missing meetingId.",
            )

        if not self.fireflies_client:
            return (
                transcript_text,
                transcript_id,
                meeting_url,
                None,
                "skipped_missing_api_key",
                "FIREFLIES_API_KEY is not configured.",
            )

        try:
            fireflies_transcript = self._fetch_fireflies_transcript(meeting_id)
        except FirefliesApiError as exc:
            return (
                transcript_text,
                transcript_id,
                meeting_url,
                None,
                "failed_fetch",
                str(exc),
            )

        text_from_fireflies = self._extract_transcript_text(fireflies_transcript)
        transcript_id_from_fireflies = self._to_text(fireflies_transcript.get("id"))
        meeting_url_from_fireflies = self._to_text(fireflies_transcript.get("meeting_link"))
        return (
            text_from_fireflies or transcript_text,
            transcript_id_from_fireflies or transcript_id,
            meeting_url_from_fireflies or meeting_url,
            fireflies_transcript,
            "completed",
            None,
        )

    def _fetch_fireflies_transcript(self, meeting_id: str) -> dict[str, Any]:
        if not self.fireflies_client:
            raise FirefliesApiError("Fireflies API client is not configured.")
        return self.fireflies_client.fetch_transcript_by_meeting_id(meeting_id)

    def _create_fireflies_client(self) -> FirefliesApiClient | None:
        if not self.settings.fireflies_api_key:
            return None
        return FirefliesApiClient(
            api_url=self.settings.fireflies_api_url,
            api_key=self.settings.fireflies_api_key,
            timeout_seconds=self.settings.fireflies_api_timeout_seconds,
            user_agent=self.settings.fireflies_api_user_agent,
        )

    def _map_record(self, record: Mapping[str, Any]) -> TranscriptionRecord:
        raw_fireflies_transcript = self._to_mapping(record.get("fireflies_transcript"))
        return TranscriptionRecord(
            id=str(record.get("_id", "")),
            provider=TranscriptionProvider(str(record.get("provider", "fireflies"))),
            event_type=self._to_text(record.get("event_type")),
            meeting_id=self._to_text(record.get("meeting_id")),
            client_reference_id=self._to_text(record.get("client_reference_id")),
            transcript_id=self._to_text(record.get("transcript_id")),
            meeting_platform=self._to_text(record.get("meeting_platform")),
            is_google_meet=bool(record.get("is_google_meet", False)),
            transcript_text_available=bool(record.get("transcript_text_available", False)),
            transcript_text=self._to_text(record.get("transcript_text")),
            transcript_sentences=self._extract_transcription_sentences(
                raw_fireflies_transcript,
            ),
            participant_emails=self._extract_participant_emails(raw_fireflies_transcript),
            enrichment_status=self._to_text(record.get("enrichment_status")),
            enrichment_error=self._to_text(record.get("enrichment_error")),
            action_items_sync=self._to_mapping(record.get("action_items_sync")),
            fireflies_transcript=raw_fireflies_transcript,
            raw_payload=dict(record.get("raw_payload", {})),
            received_at=self._to_datetime(record.get("received_at")),
        )

    def _sync_action_items(
        self,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, Any]],
        participant_emails: list[str],
        resolve_user_settings: bool = False,
    ) -> dict[str, Any]:
        if not self.action_item_sync_service:
            return {
                "status": "skipped_service_unavailable",
                "extracted_count": 0,
                "created_count": 0,
                "items": [],
                "error": "ActionItemSyncService is not available.",
                "synced_at": datetime.now(UTC),
            }
        sanitized_participants = sanitize_action_item_participants(participant_emails)
        effective_settings = self.settings
        sync_service = self.action_item_sync_service
        if resolve_user_settings and not self._has_custom_action_item_sync_service:
            effective_settings = self._resolve_settings_for_participants(sanitized_participants)
            sync_service = ActionItemSyncService(effective_settings)
        if not effective_settings.transcription_autosync_enabled:
            return {
                "status": "skipped_disabled_by_user",
                "extracted_count": 0,
                "created_count": 0,
                "google_calendar_status": "not_required_disabled_by_user",
                "google_calendar_created_count": 0,
                "google_calendar_error": None,
                "outlook_calendar_status": "not_required_disabled_by_user",
                "outlook_calendar_created_count": 0,
                "outlook_calendar_error": None,
                "items": [],
                "error": (
                    "TRANSCRIPTION_AUTOSYNC_ENABLED is disabled for this user."
                ),
                "synced_at": datetime.now(UTC),
            }

        try:
            return sync_service.sync(
                meeting_id=meeting_id,
                transcript_text=transcript_text,
                transcript_sentences=transcript_sentences,
                participant_emails=sanitized_participants,
            )
        except Exception as exc:
            return {
                "status": "failed_unexpected",
                "extracted_count": 0,
                "created_count": 0,
                "items": [],
                "error": str(exc),
                "synced_at": datetime.now(UTC),
            }

    def _resolve_settings_for_participants(self, participant_emails: list[str]) -> Settings:
        if not participant_emails:
            return self.settings
        user_store = self._get_user_store()
        if not user_store:
            return self.settings

        seen_user_ids: set[str] = set()
        best_user_values: dict[str, str] | None = None
        best_score = 0

        for participant_email in participant_emails:
            user_record = user_store.get_user_by_email(participant_email)
            if not user_record:
                continue
            user_id = self._to_text(user_record.get("_id"))
            if not user_id or user_id in seen_user_ids:
                continue
            seen_user_ids.add(user_id)

            user_values = user_store.get_user_settings_values(user_id)
            score = self._score_user_settings(user_values)
            if score <= best_score:
                continue
            best_score = score
            best_user_values = user_values

        if not best_user_values:
            return self.settings
        return self._merge_settings_with_user_values(best_user_values)

    def _score_user_settings(self, user_values: Mapping[str, str]) -> int:
        score = 0
        for env_var in (
            "NOTION_API_TOKEN",
            "NOTION_TASKS_DATABASE_ID",
            "GOOGLE_CALENDAR_API_TOKEN",
            "GOOGLE_CALENDAR_REFRESH_TOKEN",
            "OUTLOOK_CALENDAR_API_TOKEN",
            "TRANSCRIPTION_AUTOSYNC_ENABLED",
        ):
            raw_value = user_values.get(env_var, "")
            if raw_value.strip():
                score += 1
        return score

    def _merge_settings_with_user_values(self, user_values: Mapping[str, str]) -> Settings:
        overrides: dict[str, Any] = {}
        for env_var, raw_value in user_values.items():
            attr_name = env_var.lower()
            if not hasattr(self.settings, attr_name):
                continue
            base_value = getattr(self.settings, attr_name)
            if isinstance(base_value, list):
                overrides[attr_name] = [
                    value.strip()
                    for value in raw_value.split(",")
                    if value.strip()
                ]
                continue
            if isinstance(base_value, bool):
                lowered = raw_value.strip().lower()
                if lowered in {"1", "true", "yes", "on"}:
                    overrides[attr_name] = True
                elif lowered in {"0", "false", "no", "off"}:
                    overrides[attr_name] = False
                continue
            if isinstance(base_value, int):
                try:
                    overrides[attr_name] = int(raw_value)
                except ValueError:
                    continue
                continue
            if isinstance(base_value, float):
                try:
                    overrides[attr_name] = float(raw_value)
                except ValueError:
                    continue
                continue
            overrides[attr_name] = raw_value

        if not overrides:
            return self.settings

        merged_payload = self.settings.model_dump()
        merged_payload.update(overrides)
        try:
            return Settings.model_validate(merged_payload)
        except Exception:
            return self.settings

    def _get_user_store(self) -> UserStore | None:
        if self._user_store:
            return self._user_store
        if self._user_store_lookup_failed:
            return None
        try:
            self._user_store = create_user_store(self.settings)
        except Exception:
            self._user_store_lookup_failed = True
            return None
        return self._user_store

    def _save_action_item_creation_records(
        self,
        *,
        source: str,
        provider: str,
        meeting_id: str | None,
        transcript_id: str | None,
        client_reference_id: str | None,
        transcription_record_id: str | None,
        participant_emails: list[str],
        action_items_sync: Mapping[str, Any] | None,
    ) -> None:
        if not action_items_sync:
            return
        raw_items = action_items_sync.get("items")
        if not isinstance(raw_items, list):
            return
        synced_at = self._to_datetime(action_items_sync.get("synced_at"))
        creation_records: list[dict[str, Any]] = []
        for item_index, raw_item in enumerate(raw_items):
            if not isinstance(raw_item, Mapping):
                continue
            notion_status = self._to_text(raw_item.get("status"))
            if notion_status != "created":
                continue
            creation_records.append(
                build_action_item_creation_record(
                    source=source,
                    provider=provider,
                    meeting_id=meeting_id,
                    transcript_id=transcript_id,
                    client_reference_id=client_reference_id,
                    transcription_record_id=transcription_record_id,
                    action_item_index=item_index,
                    action_item=raw_item,
                    participant_emails=participant_emails,
                    synced_at=synced_at,
                ),
            )

        if not creation_records:
            return
        try:
            self.action_item_creation_store.save_many(creation_records)
        except Exception:
            # Log persistence failures should not block webhook ingestion.
            return

    def _to_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        return datetime.now(UTC)

    def _to_mapping(self, value: Any) -> dict[str, Any] | None:
        if isinstance(value, Mapping):
            return dict(value)
        return None

    def _extract_transcription_sentences(
        self,
        fireflies_transcript: Mapping[str, Any] | None,
    ) -> list[TranscriptionSentence]:
        if not fireflies_transcript:
            return []

        raw_sentences = fireflies_transcript.get("sentences")
        if not isinstance(raw_sentences, list):
            return []

        sentences: list[TranscriptionSentence] = []
        for raw_sentence in raw_sentences:
            if not isinstance(raw_sentence, Mapping):
                continue
            text = self._to_text(raw_sentence.get("text"))
            if not text:
                continue
            sentences.append(
                TranscriptionSentence(
                    index=self._to_int(raw_sentence.get("index")),
                    speaker_name=self._to_text(raw_sentence.get("speaker_name")),
                    speaker_id=self._to_text(raw_sentence.get("speaker_id")),
                    text=text,
                    start_time=self._to_float(raw_sentence.get("start_time")),
                    end_time=self._to_float(raw_sentence.get("end_time")),
                ),
            )
        return sentences

    def _extract_participant_emails(
        self,
        fireflies_transcript: Mapping[str, Any] | None,
    ) -> list[str]:
        if not fireflies_transcript:
            return []

        emails: set[str] = set()

        def add_email_candidate(value: Any) -> None:
            email = self._to_text(value)
            if not email:
                return
            normalized = email.lower()
            if "@" not in normalized:
                return
            emails.add(normalized)

        add_email_candidate(fireflies_transcript.get("organizer_email"))
        add_email_candidate(fireflies_transcript.get("host_email"))

        raw_user = fireflies_transcript.get("user")
        if isinstance(raw_user, Mapping):
            add_email_candidate(raw_user.get("email"))

        for key in ("participants", "fireflies_users"):
            values = fireflies_transcript.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                if isinstance(value, Mapping):
                    add_email_candidate(value.get("email"))
                    continue
                add_email_candidate(value)

        raw_meeting_attendees = fireflies_transcript.get("meeting_attendees")
        if isinstance(raw_meeting_attendees, list):
            for attendee in raw_meeting_attendees:
                if not isinstance(attendee, Mapping):
                    continue
                add_email_candidate(attendee.get("email"))

        return sorted(emails)

    def _extract_participant_emails_from_payload(
        self,
        payload: Mapping[str, Any],
    ) -> list[str]:
        emails: set[str] = set()

        def add_email_candidate(value: Any) -> None:
            email = self._to_text(value)
            if not email:
                return
            normalized = email.lower()
            if "@" not in normalized:
                return
            emails.add(normalized)

        for path in (
            "participant_emails",
            "participants",
            "attendees",
            "meeting.participants",
            "meeting.attendees",
            "meeting_attendees",
        ):
            values = self._extract_path(payload, path)
            if not isinstance(values, list):
                continue
            for value in values:
                if isinstance(value, Mapping):
                    add_email_candidate(value.get("email"))
                    continue
                add_email_candidate(value)

        return sorted(emails)

    def _infer_meeting_platform_from_url(self, meeting_url: str | None) -> str | None:
        if not meeting_url:
            return None
        lowered = meeting_url.lower()
        if "meet.google.com" in lowered:
            return "google_meet"
        return None

    def _extract_transcript_text(self, payload: Mapping[str, Any]) -> str | None:
        candidate_paths = (
            "transcript.text",
            "transcript.content",
            "transcript.full_text",
            "transcript",
            "summary.transcript",
            "summary.text",
            "data.transcript",
            "data.transcript.text",
            "meeting.transcript",
            "sentences",
            "paragraphs",
        )
        for path in candidate_paths:
            raw_value = self._extract_path(payload, path)
            text = self._to_text(raw_value)
            if text:
                return text
        return None

    def _is_google_meet(self, meeting_platform: str | None, meeting_url: str | None) -> bool:
        candidates = [meeting_platform, meeting_url]
        for value in candidates:
            if not value:
                continue
            lowered = value.lower()
            if "google" in lowered and "meet" in lowered:
                return True
            if "meet.google.com" in lowered:
                return True
        return False

    def _extract_first_string(
        self,
        payload: Mapping[str, Any],
        paths: tuple[str, ...],
    ) -> str | None:
        for path in paths:
            value = self._extract_path(payload, path)
            text = self._to_text(value)
            if text:
                return text
        return None

    def _extract_path(self, payload: Mapping[str, Any], path: str) -> Any:
        value: Any = payload
        for segment in path.split("."):
            if not isinstance(value, Mapping):
                return None
            if segment not in value:
                return None
            value = value[segment]
        return value

    def _to_text(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned or None
        if isinstance(value, list):
            parts: list[str] = []
            for item in value:
                text = self._to_text(item)
                if text:
                    parts.append(text)
            if not parts:
                return None
            return "\n".join(parts)
        if isinstance(value, Mapping):
            for key in ("text", "content", "transcript", "value"):
                text = self._to_text(value.get(key))
                if text:
                    return text
            return None
        return str(value)

    def _to_float(self, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int | float):
            return float(value)
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    def _to_int(self, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            return None
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            try:
                return int(cleaned)
            except ValueError:
                return None
        return None
