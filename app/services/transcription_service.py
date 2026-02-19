import hashlib
import json
import hmac
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status

from app.core.config import Settings
from app.schemas.transcription import (
    TranscriptionBackfillResponse,
    TranscriptionParticipant,
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
from app.services.read_ai_api_client import ReadAiApiClient, ReadAiApiError
from app.services.team_membership_service import TeamMembershipService
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
        read_ai_client: ReadAiApiClient | None = None,
        action_item_sync_service: ActionItemSyncService | None = None,
        action_item_creation_store: ActionItemCreationStore | None = None,
        user_store: UserStore | None = None,
        team_membership_service: TeamMembershipService | None = None,
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
        self.read_ai_client = read_ai_client or self._create_read_ai_client()
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
        self._team_membership_service = team_membership_service
        self._team_membership_service_lookup_failed = False

    def process_webhook(
        self,
        provider: TranscriptionProvider,
        payload: Mapping[str, Any],
        shared_secret: str | None,
        raw_body: bytes | None = None,
        signature: str | None = None,
        user_settings_user_id: str | None = None,
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
                "session_id",
                "uuid",
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
        ingestion_key = self._build_ingestion_key(
            provider=provider,
            payload=payload,
        )

        if ingestion_key:
            existing_record = self._get_record_by_ingestion_key(ingestion_key)
            if existing_record:
                existing_action_items_sync = self._to_mapping(existing_record.get("action_items_sync"))
                existing_status = (
                    self._to_text(existing_action_items_sync.get("status"))
                    if existing_action_items_sync
                    else None
                )
                return TranscriptionWebhookResponse(
                    provider=provider,
                    event_type=self._to_text(existing_record.get("event_type")) or event_type,
                    meeting_id=self._to_text(existing_record.get("meeting_id")) or meeting_id,
                    client_reference_id=self._to_text(existing_record.get("client_reference_id"))
                    or client_reference_id,
                    transcript_id=self._to_text(existing_record.get("transcript_id")) or transcript_id,
                    meeting_platform=self._to_text(existing_record.get("meeting_platform"))
                    or meeting_platform,
                    is_google_meet=bool(existing_record.get("is_google_meet", False)),
                    transcript_text_available=bool(
                        existing_record.get("transcript_text_available", False)
                    ),
                    enrichment_status=self._to_text(existing_record.get("enrichment_status"))
                    or "skipped_duplicate",
                    enrichment_error=self._to_text(existing_record.get("enrichment_error")),
                    action_items_sync_status=existing_status,
                    action_items_created_count=self._to_int(
                        existing_action_items_sync.get("created_count"),
                    )
                    if existing_action_items_sync
                    else None,
                    stored_record_id=self._to_text(existing_record.get("_id")),
                    received_at=self._to_datetime(existing_record.get("received_at")),
                )

        transcript_text = self._extract_transcript_text(payload)
        enrichment_status = "not_required"
        enrichment_error: str | None = None
        fireflies_transcript: dict[str, Any] | None = None
        read_ai_transcript: dict[str, Any] | None = None
        enrichment_fireflies_client = self._resolve_fireflies_client_for_payload(
            payload,
            user_settings_user_id=user_settings_user_id,
        )
        enrichment_read_ai_client = self._resolve_read_ai_client_for_payload(
            payload,
            user_settings_user_id=user_settings_user_id,
        )

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
                fireflies_client=enrichment_fireflies_client,
            )
        elif provider == TranscriptionProvider.read_ai:
             (
                transcript_text,
                transcript_id,
                meeting_url,
                read_ai_transcript,
                enrichment_status,
                enrichment_error,
            ) = self._enrich_read_ai_transcript(
                meeting_id=meeting_id,
                transcript_id=transcript_id,
                transcript_text=transcript_text,
                meeting_url=meeting_url,
                read_ai_client=enrichment_read_ai_client,
            )

        transcript_sentences: list[dict[str, Any]] = []
        participants: list[dict[str, Any]] = []
        participant_emails: list[str] = []

        if fireflies_transcript:
            transcript_sentences = extract_sentences_for_action_items(fireflies_transcript)
            participants = self._extract_fireflies_participants(fireflies_transcript)
        elif read_ai_transcript:
            read_ai_sentences_objs = self._extract_read_ai_sentences(read_ai_transcript)
            transcript_sentences = [
                {
                    "text": s.text,
                    "speaker_name": s.speaker_name,
                    "start_time": s.start_time,
                }
                for s in read_ai_sentences_objs
            ]
            participants = self._extract_read_ai_participants(read_ai_transcript)
        elif provider == TranscriptionProvider.read_ai:
            read_ai_sentences_objs = self._extract_read_ai_sentences(payload)
            transcript_sentences = [
                {
                    "text": s.text,
                    "speaker_name": s.speaker_name,
                    "start_time": s.start_time,
                }
                for s in read_ai_sentences_objs
            ]
            participants = self._extract_read_ai_participants(payload)

        if not participants:
            participants = self._extract_participants_from_payload(payload)
        if not participants:
            participants = self._extract_participants_from_sentences(transcript_sentences)

        participant_emails = self._extract_participant_emails_from_participants(participants)
        if not participant_emails:
            participant_emails = self._extract_participant_emails_from_payload(payload)
        if participant_emails and not participants:
            participants = self._build_participants_from_emails(participant_emails)
        participant_emails = sanitize_action_item_participants(participant_emails)

        action_items_sync = self._sync_action_items_with_team_routing(
            meeting_id=meeting_id,
            transcript_text=transcript_text,
            transcript_sentences=transcript_sentences,
            participant_emails=participant_emails,
            user_settings_user_id=user_settings_user_id,
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
            ingestion_key=ingestion_key,
            enrichment_status=enrichment_status,
            enrichment_error=enrichment_error,
            participants=participants,
            participant_emails=participant_emails,
            action_items_sync=action_items_sync,
            fireflies_transcript=fireflies_transcript,
            read_ai_transcript=read_ai_transcript,
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
        participants = self._extract_fireflies_participants(fireflies_transcript)
        participant_emails = self._extract_participant_emails_from_participants(participants)
        if not participant_emails:
            participant_emails = sanitize_action_item_participants(
                self._extract_participant_emails(fireflies_transcript),
            )
        if participant_emails and not participants:
            participants = self._build_participants_from_emails(participant_emails)
        action_items_sync = self._sync_action_items_with_team_routing(
            meeting_id=meeting_id,
            transcript_text=transcript_text,
            transcript_sentences=transcript_sentences,
            participant_emails=participant_emails,
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
            "participants": participants,
            "participant_emails": participant_emails,
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
        if provider == TranscriptionProvider.read_ai:
            return

        expected_secret = ""
        if provider == TranscriptionProvider.fireflies:
            expected_secret = self.settings.fireflies_webhook_secret

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

    def _get_record_by_ingestion_key(self, ingestion_key: str) -> Mapping[str, Any] | None:
        try:
            return self.store.get_by_ingestion_key(ingestion_key)
        except Exception:
            return None

    def _build_ingestion_key(
        self,
        *,
        provider: TranscriptionProvider,
        payload: Mapping[str, Any],
    ) -> str | None:
        meeting_id = self._extract_first_string(
            payload,
            paths=(
                "meeting.id",
                "meeting.meeting_id",
                "meeting.external_id",
                "meetingId",
                "meeting_id",
                "session_id",
                "uuid",
            ),
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
        client_reference_id = self._extract_first_string(
            payload,
            paths=("clientReferenceId", "client_reference_id"),
        )
        event_type = self._extract_first_string(
            payload,
            paths=("event", "event_type", "eventType", "type"),
        )
        key_parts: list[str] = [provider.value]
        if meeting_id:
            key_parts.append(f"meeting:{meeting_id.strip().lower()}")
        if transcript_id:
            key_parts.append(f"transcript:{transcript_id.strip().lower()}")
        elif client_reference_id:
            key_parts.append(f"client_reference:{client_reference_id.strip().lower()}")
        if event_type:
            key_parts.append(f"event:{event_type.strip().lower()}")
        if len(key_parts) >= 3:
            return "|".join(key_parts)

        try:
            normalized_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        except TypeError:
            normalized_payload = json.dumps(dict(payload), sort_keys=True, default=str)

        digest = hashlib.sha256(normalized_payload.encode("utf-8")).hexdigest()
        return f"{provider.value}:{digest}"

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
        fireflies_client: FirefliesApiClient | None = None,
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

        active_client = fireflies_client or self.fireflies_client
        if not active_client:
            return (
                transcript_text,
                transcript_id,
                meeting_url,
                None,
                "skipped_missing_api_key",
                "FIREFLIES_API_KEY is not configured.",
            )

        try:
            fireflies_transcript = self._fetch_fireflies_transcript(
                meeting_id,
                fireflies_client=active_client,
            )
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

    def _fetch_fireflies_transcript(
        self,
        meeting_id: str,
        fireflies_client: FirefliesApiClient | None = None,
    ) -> dict[str, Any]:
        active_client = fireflies_client or self.fireflies_client
        if not active_client:
            raise FirefliesApiError("Fireflies API client is not configured.")
        return active_client.fetch_transcript_by_meeting_id(meeting_id)

    def _create_fireflies_client(self) -> FirefliesApiClient | None:
        if not self.settings.fireflies_api_key:
            return None
        return FirefliesApiClient(
            api_url=self.settings.fireflies_api_url,
            api_key=self.settings.fireflies_api_key,
            timeout_seconds=self.settings.fireflies_api_timeout_seconds,
            user_agent=self.settings.fireflies_api_user_agent,
        )

    def _resolve_fireflies_client_for_payload(
        self,
        payload: Mapping[str, Any],
        user_settings_user_id: str | None = None,
    ) -> FirefliesApiClient | None:
        participant_emails = sanitize_action_item_participants(
            self._extract_participant_emails_from_payload(payload),
        )
        if not participant_emails:
            if not user_settings_user_id:
                return self.fireflies_client
            effective_settings = self._resolve_settings_for_participants(
                [],
                user_settings_user_id=user_settings_user_id,
            )
        else:
            effective_settings = self._resolve_settings_for_participants(
                participant_emails,
                user_settings_user_id=user_settings_user_id,
            )
        if not effective_settings.fireflies_api_key:
            return self.fireflies_client

        if effective_settings.fireflies_api_key == self.settings.fireflies_api_key:
            return self.fireflies_client

        return FirefliesApiClient(
            api_url=effective_settings.fireflies_api_url,
            api_key=effective_settings.fireflies_api_key,
            timeout_seconds=effective_settings.fireflies_api_timeout_seconds,
            user_agent=effective_settings.fireflies_api_user_agent,
        )

    def _resolve_read_ai_client_for_payload(
        self,
        payload: Mapping[str, Any],
        user_settings_user_id: str | None = None,
    ) -> ReadAiApiClient | None:
        participant_emails = sanitize_action_item_participants(
            self._extract_participant_emails_from_payload(payload),
        )
        if not participant_emails:
            if not user_settings_user_id:
                return self.read_ai_client
            effective_settings = self._resolve_settings_for_participants(
                [],
                user_settings_user_id=user_settings_user_id,
            )
        else:
            effective_settings = self._resolve_settings_for_participants(
                participant_emails,
                user_settings_user_id=user_settings_user_id,
            )
        if not effective_settings.read_ai_api_key:
            return self.read_ai_client

        if effective_settings.read_ai_api_key == self.settings.read_ai_api_key:
            return self.read_ai_client

        return ReadAiApiClient(
            api_url=effective_settings.read_ai_api_url,
            api_key=effective_settings.read_ai_api_key,
            timeout_seconds=effective_settings.read_ai_api_timeout_seconds,
            user_agent=effective_settings.read_ai_api_user_agent,
        )

    def _enrich_read_ai_transcript(
        self,
        meeting_id: str | None,
        transcript_id: str | None,
        transcript_text: str | None,
        meeting_url: str | None,
        read_ai_client: ReadAiApiClient | None = None,
    ) -> tuple[str | None, str | None, str | None, dict[str, Any] | None, str, str | None]:
        if not meeting_id:
            return (
                transcript_text,
                transcript_id,
                meeting_url,
                None,
                "skipped_no_meeting_id",
                "No meeting_id provided in webhook.",
            )

        active_client = read_ai_client or self.read_ai_client
        if not active_client:
            return (
                transcript_text,
                transcript_id,
                meeting_url,
                None,
                "skipped_missing_api_key",
                "READ_AI_API_KEY is not configured.",
            )

        try:
            read_ai_transcript = self._fetch_read_ai_transcript(
                meeting_id,
                read_ai_client=active_client,
            )
        except ReadAiApiError as exc:
            return (
                transcript_text,
                transcript_id,
                meeting_url,
                None,
                "failed_fetch",
                str(exc),
            )

        text_from_read_ai = self._extract_read_ai_text(read_ai_transcript)
        
        return (
            text_from_read_ai or transcript_text,
            transcript_id,
            meeting_url,
            read_ai_transcript,
            "completed",
            None,
        )

    def _fetch_read_ai_transcript(
        self,
        meeting_id: str,
        read_ai_client: ReadAiApiClient | None = None,
    ) -> dict[str, Any]:
        active_client = read_ai_client or self.read_ai_client
        if not active_client:
             raise ReadAiApiError("Read AI API client is not configured.")
        return active_client.fetch_meeting_details(meeting_id)

    def _extract_read_ai_text(self, transcript_data: Mapping[str, Any]) -> str | None:
        explicit_text = self._extract_first_string(transcript_data, ("transcript_text", "text", "content"))
        if explicit_text:
            return explicit_text

        sentences = self._extract_read_ai_sentences(transcript_data)
        if sentences:
            return "\n".join(s.text for s in sentences)
            
        return None

    def _extract_read_ai_sentences(
        self,
        read_ai_transcript: Mapping[str, Any] | None,
    ) -> list[TranscriptionSentence]:
        if not read_ai_transcript:
            return []

        raw_list = None
        for key in ("transcript", "segments", "timeline", "sentences"):
             val = read_ai_transcript.get(key)
             if isinstance(val, list):
                 raw_list = val
                 break
        
        if not raw_list and isinstance(read_ai_transcript.get("transcript"), Mapping):
             val = read_ai_transcript["transcript"].get("speaker_blocks")
             if isinstance(val, list):
                 raw_list = val
        
        if not raw_list:
            return []

        sentences: list[TranscriptionSentence] = []
        for idx, item in enumerate(raw_list):
            if not isinstance(item, Mapping):
                continue
            
            text = self._to_text(item.get("text") or item.get("content") or item.get("words"))
            if not text:
                continue

            speaker_name = self._to_text(item.get("speaker_name") or item.get("speaker"))
            speaker_block_speaker = item.get("speaker")
            if not speaker_name and isinstance(speaker_block_speaker, Mapping):
                 speaker_name = self._to_text(speaker_block_speaker.get("name"))
            
            start_time = self._to_float(item.get("start_time") or item.get("start") or item.get("startTime"))
            if isinstance(item.get("start_time"), Mapping): # mongo style { $numberLong: ... }
                 # This might happen if payload is already from DB dump but here we have raw payload.
                 # The user payload shows { "$numberLong": "..." } which indicates it might be coming from MongoDB Extended JSON or similar, 
                 # OR the raw payload actually has this format?
                 # Wait, raw payload usually has standard JSON. 
                 # The user provided "Guardado en la base de datos", so that structure is from DB.
                 # But raw_payload field in DB stores the original payload.
                 # If the original payload has { $numberLong ... }, usually that's unusual for webhooks unless it's BSON dump.
                 # Assuming user provided what is stored in Mongo. 
                 # If webhook sends standard JSON, start_time is likely a number or string.
                 pass

            end_time = self._to_float(item.get("end_time") or item.get("end") or item.get("endTime"))

            sentences.append(
                TranscriptionSentence(
                    index=idx,
                    speaker_name=speaker_name,
                    speaker_id=None,
                    text=text,
                    start_time=start_time,
                    end_time=end_time
                )
            )
        return sentences

    def _extract_read_ai_participant_emails(
        self,
        read_ai_transcript: Mapping[str, Any] | None,
    ) -> list[str]:
        if not read_ai_transcript:
            return []

        participants = self._extract_read_ai_participants(read_ai_transcript)
        if participants:
            return self._extract_participant_emails_from_participants(participants)
        return []

    def _create_read_ai_client(self) -> ReadAiApiClient | None:
        if not self.settings.read_ai_api_key:
            return None
        return ReadAiApiClient(
            api_url=self.settings.read_ai_api_url,
            api_key=self.settings.read_ai_api_key,
            timeout_seconds=self.settings.read_ai_api_timeout_seconds,
            user_agent=self.settings.read_ai_api_user_agent,
        )

    def _map_record(self, record: Mapping[str, Any]) -> TranscriptionRecord:
        raw_fireflies_transcript = self._to_mapping(record.get("fireflies_transcript"))
        raw_read_ai_transcript = self._to_mapping(record.get("read_ai_transcript"))
        raw_payload = self._to_mapping(record.get("raw_payload")) or {}

        sentences: list[TranscriptionSentence] = []
        if raw_fireflies_transcript:
            sentences = self._extract_transcription_sentences(raw_fireflies_transcript)
        elif raw_read_ai_transcript:
            sentences = self._extract_read_ai_sentences(raw_read_ai_transcript)

        participants = self._normalize_participants(record.get("participants"))
        if not participants and raw_fireflies_transcript:
            participants = self._extract_fireflies_participants(raw_fireflies_transcript)
        if not participants and raw_read_ai_transcript:
            participants = self._extract_read_ai_participants(raw_read_ai_transcript)
        if not participants:
            participants = self._extract_participants_from_payload(record)
        if not participants:
            participants = self._extract_participants_from_payload(raw_payload)
        if not participants:
            participants = self._extract_participants_from_sentence_objects(sentences)

        participant_emails = sanitize_action_item_participants(
            self._extract_string_values(record.get("participant_emails")),
        )
        if not participant_emails:
            participant_emails = self._extract_participant_emails_from_participants(participants)
        if not participant_emails and raw_fireflies_transcript:
            participant_emails = self._extract_participant_emails(raw_fireflies_transcript)
        if not participant_emails and raw_read_ai_transcript:
            participant_emails = self._extract_read_ai_participant_emails(raw_read_ai_transcript)
        if not participant_emails and raw_payload:
            participant_emails = self._extract_participant_emails_from_payload(raw_payload)

        participant_models = [
            TranscriptionParticipant.model_validate(participant)
            for participant in participants
        ]
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
            transcript_sentences=sentences,
            participants=participant_models,
            participant_emails=participant_emails,
            enrichment_status=self._to_text(record.get("enrichment_status")),
            enrichment_error=self._to_text(record.get("enrichment_error")),
            action_items_sync=self._to_mapping(record.get("action_items_sync")),
            fireflies_transcript=raw_fireflies_transcript,
            read_ai_transcript=raw_read_ai_transcript,
            raw_payload=raw_payload,
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
        user_settings_user_id: str | None = None,
        calendar_attendee_emails: list[str] | None = None,
        force_disable_google_calendar: bool = False,
        force_disable_outlook_calendar: bool = False,
        skip_google_meeting_items: bool = False,
        skip_outlook_meeting_items: bool = False,
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
            forced_settings_error = self._validate_forced_user_settings()
            if forced_settings_error:
                return {
                    "status": "skipped_missing_forced_user_settings",
                    "extracted_count": 0,
                    "created_count": 0,
                    "monday_status": "not_required_missing_forced_user_settings",
                    "monday_created_count": 0,
                    "monday_error": None,
                    "google_calendar_status": "not_required_missing_forced_user_settings",
                    "google_calendar_created_count": 0,
                    "google_calendar_error": None,
                    "outlook_calendar_status": "not_required_missing_forced_user_settings",
                    "outlook_calendar_created_count": 0,
                    "outlook_calendar_error": None,
                    "items": [],
                    "error": forced_settings_error,
                    "synced_at": datetime.now(UTC),
                }
            if user_settings_user_id:
                effective_settings = self._resolve_settings_for_participants(
                    sanitized_participants,
                    user_settings_user_id=user_settings_user_id,
                )
            else:
                effective_settings = self._resolve_settings_for_participants(
                    sanitized_participants,
                )
            if force_disable_google_calendar:
                effective_settings = self._override_settings(
                    effective_settings,
                    {
                        "google_calendar_api_token": "",
                        "google_calendar_refresh_token": "",
                    },
                )
            if force_disable_outlook_calendar:
                effective_settings = self._override_settings(
                    effective_settings,
                    {
                        "outlook_calendar_api_token": "",
                        "outlook_calendar_refresh_token": "",
                    },
                )
            sync_service = ActionItemSyncService(effective_settings)
        if not effective_settings.transcription_autosync_enabled:
            return {
                "status": "skipped_disabled_by_user",
                "extracted_count": 0,
                "created_count": 0,
                "monday_status": "not_required_disabled_by_user",
                "monday_created_count": 0,
                "monday_error": None,
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
            sync_kwargs: dict[str, Any] = {
                "meeting_id": meeting_id,
                "transcript_text": transcript_text,
                "transcript_sentences": transcript_sentences,
                "participant_emails": sanitized_participants,
            }
            if calendar_attendee_emails is not None:
                sync_kwargs["calendar_attendee_emails"] = sanitize_action_item_participants(
                    calendar_attendee_emails,
                )
            if skip_google_meeting_items:
                sync_kwargs["skip_google_meeting_items"] = True
            if skip_outlook_meeting_items:
                sync_kwargs["skip_outlook_meeting_items"] = True
            return sync_service.sync(**sync_kwargs)
        except TypeError as exc:
            error_message = str(exc)
            removed_any_kwarg = False
            if "skip_google_meeting_items" in error_message:
                sync_kwargs.pop("skip_google_meeting_items", None)
                removed_any_kwarg = True
            if "skip_outlook_meeting_items" in error_message:
                sync_kwargs.pop("skip_outlook_meeting_items", None)
                removed_any_kwarg = True
            if removed_any_kwarg:
                try:
                    return sync_service.sync(**sync_kwargs)
                except Exception as retry_exc:
                    return {
                        "status": "failed_unexpected",
                        "extracted_count": 0,
                        "created_count": 0,
                        "monday_status": "not_required_no_due_dates",
                        "monday_created_count": 0,
                        "monday_error": None,
                        "items": [],
                        "error": str(retry_exc),
                        "synced_at": datetime.now(UTC),
                    }
            return {
                "status": "failed_unexpected",
                "extracted_count": 0,
                "created_count": 0,
                "monday_status": "not_required_no_due_dates",
                "monday_created_count": 0,
                "monday_error": None,
                "items": [],
                "error": error_message,
                "synced_at": datetime.now(UTC),
            }
        except Exception as exc:
            return {
                "status": "failed_unexpected",
                "extracted_count": 0,
                "created_count": 0,
                "monday_status": "not_required_no_due_dates",
                "monday_created_count": 0,
                "monday_error": None,
                "items": [],
                "error": str(exc),
                "synced_at": datetime.now(UTC),
            }

    def _sync_action_items_with_team_routing(
        self,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, Any]],
        participant_emails: list[str],
        user_settings_user_id: str | None = None,
    ) -> dict[str, Any]:
        normalized_participant_emails = sanitize_action_item_participants(participant_emails)
        target_user_ids, matched_team_ids = self._resolve_team_recipient_user_ids(
            participant_emails=normalized_participant_emails,
            user_settings_user_id=user_settings_user_id,
        )
        if not target_user_ids:
            if matched_team_ids:
                return {
                    "status": "skipped_no_active_team_recipients",
                    "extracted_count": 0,
                    "created_count": 0,
                    "monday_status": "not_required_no_due_dates",
                    "monday_created_count": 0,
                    "monday_error": None,
                    "google_calendar_status": "not_required_no_due_dates",
                    "google_calendar_created_count": 0,
                    "google_calendar_error": None,
                    "outlook_calendar_status": "not_required_no_due_dates",
                    "outlook_calendar_created_count": 0,
                    "outlook_calendar_error": None,
                    "items": [],
                    "error": None,
                    "synced_at": datetime.now(UTC),
                    "routed_via_team_memberships": True,
                    "matched_team_ids": matched_team_ids,
                    "target_users": [],
                }
            direct_result = self._sync_action_items(
                meeting_id=meeting_id,
                transcript_text=transcript_text,
                transcript_sentences=transcript_sentences,
                participant_emails=normalized_participant_emails,
                resolve_user_settings=True,
                user_settings_user_id=user_settings_user_id,
            )
            direct_result["routed_via_team_memberships"] = False
            direct_result["matched_team_ids"] = []
            direct_result["target_users"] = []
            return direct_result

        user_store = self._get_user_store()
        per_user_results: list[dict[str, Any]] = []
        combined_items: list[dict[str, Any]] = []
        extracted_count = 0
        created_count = 0
        monday_created_count = 0
        google_calendar_created_count = 0
        outlook_calendar_created_count = 0

        (
            preferred_google_owner_user_id,
            preferred_outlook_owner_user_id,
        ) = self._resolve_team_calendar_owner_ids(
            target_user_ids=target_user_ids,
            preferred_user_id=user_settings_user_id,
        )
        ordered_target_user_ids = self._prioritize_user_ids(
            user_ids=target_user_ids,
            prioritized_user_ids=[
                user_settings_user_id,
                preferred_google_owner_user_id,
                preferred_outlook_owner_user_id,
            ],
        )
        google_owner_user_id: str | None = None
        outlook_owner_user_id: str | None = None
        google_owner_created_count = 0
        outlook_owner_created_count = 0
        calendar_attendee_emails = self._build_team_calendar_attendee_emails(
            participant_emails=normalized_participant_emails,
            target_user_ids=target_user_ids,
            matched_team_ids=matched_team_ids,
        )

        raw_user_runs: list[dict[str, Any]] = []
        shared_google_payloads: dict[str, dict[str, Any]] = {}
        shared_outlook_payloads: dict[str, dict[str, Any]] = {}

        for target_user_id in ordered_target_user_ids:
            target_user_email: str | None = None
            if user_store:
                user_record = user_store.get_user_by_id(target_user_id)
                if user_record:
                    target_user_email = self._to_text(user_record.get("email"))
            skip_google_meeting_items = bool(
                google_owner_user_id and target_user_id != google_owner_user_id,
            )
            skip_outlook_meeting_items = bool(
                outlook_owner_user_id and target_user_id != outlook_owner_user_id,
            )
            result = self._sync_action_items(
                meeting_id=meeting_id,
                transcript_text=transcript_text,
                transcript_sentences=transcript_sentences,
                participant_emails=normalized_participant_emails,
                resolve_user_settings=True,
                user_settings_user_id=target_user_id,
                calendar_attendee_emails=calendar_attendee_emails,
                skip_google_meeting_items=skip_google_meeting_items,
                skip_outlook_meeting_items=skip_outlook_meeting_items,
            )
            raw_user_runs.append(
                {
                    "user_id": target_user_id,
                    "user_email": target_user_email,
                    "result": result,
                },
            )
            extracted_count += self._to_int(result.get("extracted_count")) or 0
            created_count += self._to_int(result.get("created_count")) or 0
            monday_created_count += self._to_int(result.get("monday_created_count")) or 0

            raw_items = result.get("items")
            if not isinstance(raw_items, list):
                continue
            result_google_count = self._to_int(result.get("google_calendar_created_count")) or 0
            result_outlook_count = self._to_int(result.get("outlook_calendar_created_count")) or 0
            if result_google_count > 0 and not google_owner_user_id:
                google_owner_user_id = target_user_id
                google_owner_created_count = result_google_count
                shared_google_payloads = self._build_shared_channel_payloads(
                    items=raw_items,
                    channel="google",
                )
            elif target_user_id == google_owner_user_id:
                shared_google_payloads = self._build_shared_channel_payloads(
                    items=raw_items,
                    channel="google",
                )
            if result_outlook_count > 0 and not outlook_owner_user_id:
                outlook_owner_user_id = target_user_id
                outlook_owner_created_count = result_outlook_count
                shared_outlook_payloads = self._build_shared_channel_payloads(
                    items=raw_items,
                    channel="outlook",
                )
            elif target_user_id == outlook_owner_user_id:
                shared_outlook_payloads = self._build_shared_channel_payloads(
                    items=raw_items,
                    channel="outlook",
                )

        if google_owner_user_id:
            google_calendar_created_count = google_owner_created_count
        else:
            google_calendar_created_count = sum(
                self._to_int(raw_run["result"].get("google_calendar_created_count")) or 0
                for raw_run in raw_user_runs
            )
        if outlook_owner_user_id:
            outlook_calendar_created_count = outlook_owner_created_count
        else:
            outlook_calendar_created_count = sum(
                self._to_int(raw_run["result"].get("outlook_calendar_created_count")) or 0
                for raw_run in raw_user_runs
            )

        for raw_run in raw_user_runs:
            target_user_id = self._to_text(raw_run.get("user_id"))
            target_user_email = self._to_text(raw_run.get("user_email"))
            result = self._to_mapping(raw_run.get("result"))
            raw_items = result.get("items")
            patched_items = self._apply_shared_team_calendar_payloads(
                items=raw_items if isinstance(raw_items, list) else [],
                shared_google_payloads=shared_google_payloads,
                shared_outlook_payloads=shared_outlook_payloads,
                target_user_id=target_user_id,
                google_owner_user_id=google_owner_user_id,
                outlook_owner_user_id=outlook_owner_user_id,
            )
            google_status, google_error = self._summarize_team_user_channel_status(
                items=patched_items,
                channel="google",
                fallback_status=self._to_text(result.get("google_calendar_status")),
                fallback_error=self._to_text(result.get("google_calendar_error")),
            )
            outlook_status, outlook_error = self._summarize_team_user_channel_status(
                items=patched_items,
                channel="outlook",
                fallback_status=self._to_text(result.get("outlook_calendar_status")),
                fallback_error=self._to_text(result.get("outlook_calendar_error")),
            )
            result_status = self._to_text(result.get("status")) or "unknown"
            result_error = self._to_text(result.get("error"))
            per_user_results.append(
                {
                    "user_id": target_user_id,
                    "user_email": target_user_email,
                    "status": result_status,
                    "error": result_error,
                    "created_count": self._to_int(result.get("created_count")) or 0,
                    "extracted_count": self._to_int(result.get("extracted_count")) or 0,
                    "monday_status": self._to_text(result.get("monday_status")),
                    "monday_created_count": self._to_int(result.get("monday_created_count")) or 0,
                    "monday_error": self._to_text(result.get("monday_error")),
                    "google_calendar_status": google_status,
                    "google_calendar_created_count": (
                        self._to_int(result.get("google_calendar_created_count")) or 0
                    ),
                    "google_calendar_error": google_error,
                    "outlook_calendar_status": outlook_status,
                    "outlook_calendar_created_count": (
                        self._to_int(result.get("outlook_calendar_created_count")) or 0
                    ),
                    "outlook_calendar_error": outlook_error,
                },
            )
            for patched_item in patched_items:
                enriched_item = dict(patched_item)
                enriched_item["target_user_id"] = target_user_id
                enriched_item["target_user_email"] = target_user_email
                combined_items.append(enriched_item)

        overall_status, overall_error = self._summarize_multi_user_sync_status(per_user_results)
        (
            monday_status,
            monday_error,
        ) = self._summarize_multi_user_channel_status(
            per_user_results=per_user_results,
            channel="monday",
        )
        (
            google_calendar_status,
            google_calendar_error,
        ) = self._summarize_multi_user_channel_status(
            per_user_results=per_user_results,
            channel="google_calendar",
        )
        (
            outlook_calendar_status,
            outlook_calendar_error,
        ) = self._summarize_multi_user_channel_status(
            per_user_results=per_user_results,
            channel="outlook_calendar",
        )
        return {
            "status": overall_status,
            "extracted_count": extracted_count,
            "created_count": created_count,
            "monday_status": monday_status,
            "monday_created_count": monday_created_count,
            "monday_error": monday_error,
            "google_calendar_status": google_calendar_status,
            "google_calendar_created_count": google_calendar_created_count,
            "google_calendar_error": google_calendar_error,
            "outlook_calendar_status": outlook_calendar_status,
            "outlook_calendar_created_count": outlook_calendar_created_count,
            "outlook_calendar_error": outlook_calendar_error,
            "items": combined_items,
            "error": overall_error,
            "synced_at": datetime.now(UTC),
            "routed_via_team_memberships": True,
            "matched_team_ids": matched_team_ids,
            "target_users": per_user_results,
        }

    def _resolve_team_recipient_user_ids(
        self,
        *,
        participant_emails: list[str],
        user_settings_user_id: str | None = None,
    ) -> tuple[list[str], list[str]]:
        team_membership_service = self._get_team_membership_service()
        if not team_membership_service:
            return [], []
        try:
            return team_membership_service.resolve_team_recipients_for_participants(
                participant_emails=participant_emails,
                lead_user_id=user_settings_user_id,
            )
        except Exception:
            return [], []

    def _summarize_multi_user_sync_status(
        self,
        per_user_results: list[dict[str, Any]],
    ) -> tuple[str, str | None]:
        if not per_user_results:
            return "skipped_no_target_users", None
        statuses = [
            self._to_text(result.get("status")) or "unknown"
            for result in per_user_results
        ]
        errors = [
            self._to_text(result.get("error"))
            for result in per_user_results
        ]
        has_failures = any(status_text.startswith("failed") for status_text in statuses)
        has_partial = any(status_text == "completed_with_errors" for status_text in statuses)
        has_success = any(status_text == "completed" for status_text in statuses)
        has_skipped = any(status_text.startswith("skipped") for status_text in statuses)

        if has_failures and not has_success and not has_partial:
            return "failed_multi_user_sync", self._join_errors(errors)
        if has_failures or has_partial:
            return "completed_with_errors", self._join_errors(errors)
        if has_success:
            return "completed", None
        if has_skipped:
            unique_skips = sorted(set(statuses))
            if len(unique_skips) == 1:
                return unique_skips[0], self._join_errors(errors)
            return "skipped_multi_user", self._join_errors(errors)
        return statuses[0], self._join_errors(errors)

    def _summarize_multi_user_channel_status(
        self,
        *,
        per_user_results: list[dict[str, Any]],
        channel: str,
    ) -> tuple[str, str | None]:
        if not per_user_results:
            return "not_required_no_due_dates", None
        status_key = f"{channel}_status"
        error_key = f"{channel}_error"
        statuses = [
            self._to_text(result.get(status_key))
            for result in per_user_results
            if self._to_text(result.get(status_key))
        ]
        if not statuses:
            return "not_required_no_due_dates", None
        errors = [self._to_text(result.get(error_key)) for result in per_user_results]
        if any(status_text == "failed_sync" for status_text in statuses):
            return "completed_with_errors", self._join_errors(errors)
        if any(status_text == "completed_with_errors" for status_text in statuses):
            return "completed_with_errors", self._join_errors(errors)
        if any(status_text == "failed" for status_text in statuses):
            return "completed_with_errors", self._join_errors(errors)
        if "completed" in statuses or "shared_from_team_event" in statuses:
            return "completed", None
        if len(set(statuses)) == 1:
            return statuses[0], self._join_errors(errors)
        return "mixed", self._join_errors(errors)

    def _resolve_team_calendar_owner_ids(
        self,
        *,
        target_user_ids: list[str],
        preferred_user_id: str | None = None,
    ) -> tuple[str | None, str | None]:
        if not target_user_ids:
            return None, None
        normalized_preferred_user_id = (preferred_user_id or "").strip()
        ordered_user_ids = [user_id for user_id in target_user_ids if user_id]
        if normalized_preferred_user_id in ordered_user_ids:
            ordered_user_ids = [
                normalized_preferred_user_id,
                *[
                    user_id
                    for user_id in ordered_user_ids
                    if user_id != normalized_preferred_user_id
                ],
            ]
        google_owner_user_id: str | None = None
        outlook_owner_user_id: str | None = None
        for user_id in ordered_user_ids:
            user_settings = self._resolve_settings_for_user_id(user_id)
            if not user_settings.transcription_autosync_enabled:
                continue
            if not google_owner_user_id and (
                user_settings.google_calendar_api_token.strip()
                or user_settings.google_calendar_refresh_token.strip()
            ):
                google_owner_user_id = user_id
            if not outlook_owner_user_id and (
                user_settings.outlook_calendar_api_token.strip()
                or user_settings.outlook_calendar_refresh_token.strip()
            ):
                outlook_owner_user_id = user_id
            if google_owner_user_id and outlook_owner_user_id:
                break
        return google_owner_user_id, outlook_owner_user_id

    def _prioritize_user_ids(
        self,
        *,
        user_ids: list[str],
        prioritized_user_ids: list[str | None],
    ) -> list[str]:
        normalized_user_ids = [user_id for user_id in user_ids if user_id]
        prioritized: list[str] = []
        seen: set[str] = set()
        for raw_user_id in prioritized_user_ids:
            normalized_user_id = (raw_user_id or "").strip()
            if not normalized_user_id:
                continue
            if normalized_user_id not in normalized_user_ids:
                continue
            if normalized_user_id in seen:
                continue
            seen.add(normalized_user_id)
            prioritized.append(normalized_user_id)
        for user_id in normalized_user_ids:
            if user_id in seen:
                continue
            seen.add(user_id)
            prioritized.append(user_id)
        return prioritized

    def _build_team_calendar_attendee_emails(
        self,
        *,
        participant_emails: list[str],
        target_user_ids: list[str],
        matched_team_ids: list[str],
    ) -> list[str]:
        attendee_emails = set(sanitize_action_item_participants(participant_emails))
        user_store = self._get_user_store()
        if user_store:
            for user_id in target_user_ids:
                user_record = user_store.get_user_by_id(user_id)
                if not user_record:
                    continue
                user_email = self._to_text(user_record.get("email"))
                if user_email and "@" in user_email:
                    attendee_emails.add(user_email.strip().lower())
        team_membership_service = self._get_team_membership_service()
        team_store = (
            team_membership_service.team_store
            if team_membership_service and hasattr(team_membership_service, "team_store")
            else None
        )
        if team_store:
            for team_id in matched_team_ids:
                pending_invitations = team_store.list_pending_invitations_for_team(team_id)
                for invitation in pending_invitations:
                    invitation_status = self._to_text(invitation.get("status")) or "pending"
                    if invitation_status != "pending":
                        continue
                    invited_email = self._to_text(invitation.get("invited_email"))
                    if invited_email and "@" in invited_email:
                        attendee_emails.add(invited_email.strip().lower())
        return sorted(attendee_emails)

    def _build_shared_channel_payloads(
        self,
        *,
        items: list[Any],
        channel: str,
    ) -> dict[str, dict[str, Any]]:
        payloads: dict[str, dict[str, Any]] = {}
        for index, raw_item in enumerate(items):
            if not isinstance(raw_item, Mapping):
                continue
            if not self._is_meeting_action_item(raw_item):
                continue
            match_key = self._build_action_item_match_key(raw_item, index)
            if channel == "google":
                payloads[match_key] = {
                    "status": self._to_text(raw_item.get("google_calendar_status")),
                    "event_id": self._to_text(raw_item.get("google_calendar_event_id")),
                    "error": self._to_text(raw_item.get("google_calendar_error")),
                    "meeting_link": self._to_text(raw_item.get("google_meet_link")),
                }
            elif channel == "outlook":
                payloads[match_key] = {
                    "status": self._to_text(raw_item.get("outlook_calendar_status")),
                    "event_id": self._to_text(raw_item.get("outlook_calendar_event_id")),
                    "error": self._to_text(raw_item.get("outlook_calendar_error")),
                    "meeting_link": self._to_text(raw_item.get("outlook_teams_link")),
                }
        return payloads

    def _apply_shared_team_calendar_payloads(
        self,
        *,
        items: list[Any],
        shared_google_payloads: Mapping[str, Mapping[str, Any]],
        shared_outlook_payloads: Mapping[str, Mapping[str, Any]],
        target_user_id: str | None,
        google_owner_user_id: str | None,
        outlook_owner_user_id: str | None,
    ) -> list[dict[str, Any]]:
        patched_items: list[dict[str, Any]] = []
        normalized_target_user_id = (target_user_id or "").strip()
        for index, raw_item in enumerate(items):
            if not isinstance(raw_item, Mapping):
                continue
            patched_item = dict(raw_item)
            if self._is_meeting_action_item(patched_item):
                item_key = self._build_action_item_match_key(patched_item, index)
                if self._item_requires_google_meet(patched_item):
                    google_payload = shared_google_payloads.get(item_key)
                    if google_payload:
                        google_status = self._to_text(google_payload.get("status"))
                        google_link = self._to_text(google_payload.get("meeting_link"))
                        patched_item["google_calendar_event_id"] = self._to_text(
                            google_payload.get("event_id"),
                        )
                        patched_item["google_meet_link"] = google_link
                        patched_item["google_calendar_error"] = self._to_text(
                            google_payload.get("error"),
                        )
                        if (
                            google_status == "created"
                            and google_owner_user_id
                            and normalized_target_user_id != google_owner_user_id
                        ):
                            patched_item["google_calendar_status"] = "shared_from_team_event"
                        elif google_status:
                            patched_item["google_calendar_status"] = google_status
                if self._item_requires_teams(patched_item):
                    outlook_payload = shared_outlook_payloads.get(item_key)
                    if outlook_payload:
                        outlook_status = self._to_text(outlook_payload.get("status"))
                        teams_link = self._to_text(outlook_payload.get("meeting_link"))
                        patched_item["outlook_calendar_event_id"] = self._to_text(
                            outlook_payload.get("event_id"),
                        )
                        patched_item["outlook_teams_link"] = teams_link
                        patched_item["outlook_calendar_error"] = self._to_text(
                            outlook_payload.get("error"),
                        )
                        if (
                            outlook_status == "created"
                            and outlook_owner_user_id
                            and normalized_target_user_id != outlook_owner_user_id
                        ):
                            patched_item["outlook_calendar_status"] = "shared_from_team_event"
                        elif outlook_status:
                            patched_item["outlook_calendar_status"] = outlook_status
            patched_items.append(patched_item)
        return patched_items

    def _summarize_team_user_channel_status(
        self,
        *,
        items: list[dict[str, Any]],
        channel: str,
        fallback_status: str | None,
        fallback_error: str | None,
    ) -> tuple[str, str | None]:
        status_key = "google_calendar_status" if channel == "google" else "outlook_calendar_status"
        error_key = "google_calendar_error" if channel == "google" else "outlook_calendar_error"
        statuses = [
            self._to_text(item.get(status_key))
            for item in items
            if self._to_text(item.get(status_key))
        ]
        errors = [
            self._to_text(item.get(error_key))
            for item in items
            if self._to_text(item.get(error_key))
        ]
        if not statuses:
            return fallback_status or "not_required_no_due_dates", fallback_error
        if any(status.startswith("failed") for status in statuses):
            return "completed_with_errors", self._join_errors(errors) or fallback_error
        if any(status == "completed_with_errors" for status in statuses):
            return "completed_with_errors", self._join_errors(errors) or fallback_error
        if any(status == "shared_from_team_event" for status in statuses):
            return "shared_from_team_event", None
        if any(status == "created" for status in statuses):
            return "completed", None
        if len(set(statuses)) == 1:
            return statuses[0], self._join_errors(errors) or fallback_error
        return fallback_status or "mixed", self._join_errors(errors) or fallback_error

    def _is_meeting_action_item(self, item: Mapping[str, Any]) -> bool:
        platform = (self._to_text(item.get("online_meeting_platform")) or "").lower()
        return platform in {"google_meet", "microsoft_teams"}

    def _item_requires_google_meet(self, item: Mapping[str, Any]) -> bool:
        platform = (self._to_text(item.get("online_meeting_platform")) or "").lower()
        return platform == "google_meet"

    def _item_requires_teams(self, item: Mapping[str, Any]) -> bool:
        platform = (self._to_text(item.get("online_meeting_platform")) or "").lower()
        return platform == "microsoft_teams"

    def _build_action_item_match_key(self, item: Mapping[str, Any], index: int) -> str:
        due_date = (self._to_text(item.get("due_date")) or "").strip().lower()
        scheduled_start = (self._to_text(item.get("scheduled_start")) or "").strip().lower()
        scheduled_end = (self._to_text(item.get("scheduled_end")) or "").strip().lower()
        event_timezone = (self._to_text(item.get("event_timezone")) or "").strip().lower()
        recurrence_rule = (self._to_text(item.get("recurrence_rule")) or "").strip().lower()
        platform = (self._to_text(item.get("online_meeting_platform")) or "").strip().lower()
        return (
            f"{index}|{due_date}|{scheduled_start}|"
            f"{scheduled_end}|{event_timezone}|{recurrence_rule}|{platform}"
        )

    def _join_errors(self, errors: list[str | None]) -> str | None:
        normalized_errors: list[str] = []
        seen: set[str] = set()
        for raw_error in errors:
            normalized_error = self._to_text(raw_error)
            if not normalized_error:
                continue
            if normalized_error in seen:
                continue
            seen.add(normalized_error)
            normalized_errors.append(normalized_error)
        if not normalized_errors:
            return None
        if len(normalized_errors) == 1:
            return normalized_errors[0]
        return "; ".join(normalized_errors[:3])

    def _resolve_settings_for_participants(
        self,
        participant_emails: list[str],
        user_settings_user_id: str | None = None,
    ) -> Settings:
        normalized_participants = sanitize_action_item_participants(participant_emails)
        user_store = self._get_user_store()
        if not user_store:
            return self.settings

        if user_settings_user_id:
            return self._resolve_settings_for_user_id(user_settings_user_id)

        forced_user_id = self.settings.force_user_settings_user_id.strip()
        if forced_user_id:
            forced_user_values = user_store.get_user_settings_values(forced_user_id)
            if forced_user_values:
                return self._merge_settings_with_user_values(forced_user_values)

        if not normalized_participants:
            return self.settings

        seen_user_ids: set[str] = set()
        best_user_values: dict[str, str] | None = None
        best_score = 0

        for participant_email in normalized_participants:
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

    def _resolve_settings_for_user_id(self, user_id: str) -> Settings:
        normalized_user_id = (user_id or "").strip()
        if not normalized_user_id:
            return self.settings

        user_store = self._get_user_store()
        if not user_store:
            return self.settings

        user_record = user_store.get_user_by_id(normalized_user_id)
        if not user_record:
            return self.settings

        user_values = user_store.get_user_settings_values(normalized_user_id)
        if not user_values:
            return self.settings
        return self._merge_settings_with_user_values(user_values)

    def _validate_forced_user_settings(self) -> str | None:
        forced_user_id = self.settings.force_user_settings_user_id.strip()
        if not forced_user_id:
            return None

        user_store = self._get_user_store()
        if not user_store:
            return "FORCE_USER_SETTINGS_USER_ID is configured but user store is unavailable."

        forced_user = user_store.get_user_by_id(forced_user_id)
        if not forced_user:
            return (
                "FORCE_USER_SETTINGS_USER_ID is configured but user was not found: "
                f"{forced_user_id}."
            )

        forced_values = user_store.get_user_settings_values(forced_user_id)
        effective_settings = self._merge_settings_with_user_values(forced_values)

        # TODO: Migrar/completar la configuración del usuario forzado para no depender
        # del fallback a valores base (.env) y poder desactivar esta compatibilidad.
        required_fields = (
            ("FIREFLIES_API_KEY", effective_settings.fireflies_api_key),
        )
        missing_required = [
            env_var
            for env_var, raw_value in required_fields
            if not str(raw_value).strip()
        ]
        notion_ready = all(
            (
                effective_settings.notion_api_token.strip(),
                effective_settings.notion_tasks_database_id.strip(),
                effective_settings.notion_task_status_property.strip(),
            ),
        )
        monday_ready = all(
            (
                effective_settings.monday_api_token.strip(),
                effective_settings.monday_board_id.strip(),
                effective_settings.monday_group_id.strip(),
            ),
        )
        if not notion_ready and not monday_ready:
            missing_required.append("NOTION_OR_MONDAY_OUTPUT")
        if missing_required:
            missing_list = ", ".join(missing_required)
            return (
                "FORCE_USER_SETTINGS_USER_ID is configured but missing required settings: "
                f"{missing_list}."
            )
        return None

    def _score_user_settings(self, user_values: Mapping[str, str]) -> int:
        score = 0
        for env_var in (
            "FIREFLIES_API_KEY",
            "READ_AI_API_KEY",
            "NOTION_API_TOKEN",
            "NOTION_TASKS_DATABASE_ID",
            "MONDAY_API_TOKEN",
            "MONDAY_BOARD_ID",
            "MONDAY_GROUP_ID",
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
        # By product rule, autosync starts enabled unless the user explicitly disables it.
        overrides: dict[str, Any] = {"transcription_autosync_enabled": True}
        for env_var, raw_value in user_values.items():
            # GEMINI_API_KEY must always come from environment-level settings (.env).
            if env_var == "GEMINI_API_KEY":
                continue
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

    def _override_settings(
        self,
        base_settings: Settings,
        overrides: Mapping[str, Any],
    ) -> Settings:
        merged_payload = base_settings.model_dump()
        merged_payload.update(dict(overrides))
        try:
            return Settings.model_validate(merged_payload)
        except Exception:
            return base_settings

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

    def _get_team_membership_service(self) -> TeamMembershipService | None:
        if self._team_membership_service:
            return self._team_membership_service
        if self._team_membership_service_lookup_failed:
            return None
        try:
            self._team_membership_service = TeamMembershipService(
                self.settings,
                user_store=self._get_user_store(),
            )
        except Exception:
            self._team_membership_service_lookup_failed = True
            return None
        return self._team_membership_service

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
            notion_status = self._to_text(raw_item.get("notion_status")) or self._to_text(
                raw_item.get("status"),
            )
            monday_status = self._to_text(raw_item.get("monday_status"))
            if notion_status != "created" and monday_status != "created":
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

    def _extract_fireflies_participants(
        self,
        fireflies_transcript: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        if not fireflies_transcript:
            return []

        participants: list[dict[str, Any]] = []
        for key in ("meeting_attendees", "participants", "fireflies_users"):
            participants.extend(self._normalize_participants(fireflies_transcript.get(key)))

        raw_user = fireflies_transcript.get("user")
        if isinstance(raw_user, Mapping):
            participants.extend(
                self._normalize_participants(
                    [
                        {
                            "email": raw_user.get("email"),
                            "name": raw_user.get("name") or raw_user.get("displayName"),
                        },
                    ],
                ),
            )

        participants.extend(
            self._normalize_participants(
                [
                    {
                        "email": fireflies_transcript.get("organizer_email"),
                        "role": "organizer",
                    },
                    {
                        "email": fireflies_transcript.get("host_email"),
                        "role": "host",
                    },
                ],
            ),
        )

        sentence_participants = self._extract_participants_from_sentence_objects(
            self._extract_transcription_sentences(fireflies_transcript),
        )
        participants = self._merge_sentence_participants(
            participants=participants,
            sentence_participants=sentence_participants,
        )
        return self._deduplicate_participants(participants)

    def _extract_read_ai_participants(
        self,
        read_ai_transcript: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        if not read_ai_transcript:
            return []

        participants: list[dict[str, Any]] = []
        for key in ("participants", "attendees"):
            participants.extend(self._normalize_participants(read_ai_transcript.get(key)))

        raw_meeting = read_ai_transcript.get("meeting")
        if isinstance(raw_meeting, Mapping):
            participants.extend(self._normalize_participants(raw_meeting.get("participants")))
            participants.extend(self._normalize_participants(raw_meeting.get("attendees")))

        if not participants:
            participants.extend(
                self._extract_participants_from_sentence_objects(
                    self._extract_read_ai_sentences(read_ai_transcript),
                ),
            )
        return self._deduplicate_participants(participants)

    def _extract_participants_from_payload(
        self,
        payload: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        participants: list[dict[str, Any]] = []
        for path in (
            "participants",
            "attendees",
            "meeting.participants",
            "meeting.attendees",
            "meeting_attendees",
        ):
            participants.extend(self._normalize_participants(self._extract_path(payload, path)))

        participants.extend(
            self._normalize_participants(
                [
                    {
                        "email": self._extract_path(payload, "organizer_email"),
                        "role": "organizer",
                    },
                    {
                        "email": self._extract_path(payload, "host_email"),
                        "role": "host",
                    },
                ],
            ),
        )
        return self._deduplicate_participants(participants)

    def _extract_participants_from_sentences(
        self,
        transcript_sentences: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        participants: list[dict[str, Any]] = []
        for sentence in transcript_sentences:
            if not isinstance(sentence, Mapping):
                continue
            participant = self._build_normalized_participant(
                name=sentence.get("speaker_name") or sentence.get("speaker"),
                external_id=sentence.get("speaker_id"),
                role="speaker",
            )
            if participant:
                participants.append(participant)
        return self._deduplicate_participants(participants)

    def _extract_participants_from_sentence_objects(
        self,
        transcript_sentences: list[TranscriptionSentence],
    ) -> list[dict[str, Any]]:
        participants: list[dict[str, Any]] = []
        for sentence in transcript_sentences:
            participant = self._build_normalized_participant(
                name=sentence.speaker_name,
                external_id=sentence.speaker_id,
                role="speaker",
            )
            if participant:
                participants.append(participant)
        return self._deduplicate_participants(participants)

    def _merge_sentence_participants(
        self,
        *,
        participants: list[dict[str, Any]],
        sentence_participants: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not sentence_participants:
            return participants
        merged_participants = [dict(participant) for participant in participants]

        for sentence_participant in sentence_participants:
            sentence_name = self._to_text(sentence_participant.get("name"))
            sentence_external_id = self._to_text(sentence_participant.get("external_id"))
            target_index = self._find_matching_participant_index(
                participants=merged_participants,
                sentence_name=sentence_name,
                sentence_external_id=sentence_external_id,
            )
            if target_index is None:
                merged_participants.append(dict(sentence_participant))
                continue

            target_participant = merged_participants[target_index]
            for field in ("name", "email", "external_id"):
                if target_participant.get(field):
                    continue
                if sentence_participant.get(field):
                    target_participant[field] = sentence_participant[field]
            if not target_participant.get("role") and sentence_participant.get("role"):
                sentence_role = self._to_text(sentence_participant.get("role"))
                target_email = self._to_text(target_participant.get("email"))
                if sentence_role == "speaker" and target_email:
                    continue
                target_participant["role"] = sentence_participant["role"]

        return merged_participants

    def _find_matching_participant_index(
        self,
        *,
        participants: list[dict[str, Any]],
        sentence_name: str | None,
        sentence_external_id: str | None,
    ) -> int | None:
        normalized_sentence_name = (sentence_name or "").strip().lower()
        normalized_sentence_external_id = (sentence_external_id or "").strip().lower()
        if normalized_sentence_external_id:
            for participant_index, participant in enumerate(participants):
                normalized_participant_external_id = (
                    self._to_text(participant.get("external_id")) or ""
                ).strip().lower()
                if normalized_participant_external_id == normalized_sentence_external_id:
                    return participant_index

        if normalized_sentence_name:
            for participant_index, participant in enumerate(participants):
                normalized_participant_name = (self._to_text(participant.get("name")) or "").strip().lower()
                if normalized_participant_name == normalized_sentence_name:
                    return participant_index

            user_store = self._get_user_store()
            if user_store:
                for participant_index, participant in enumerate(participants):
                    participant_name = self._to_text(participant.get("name"))
                    participant_email = self._to_text(participant.get("email"))
                    if participant_name or not participant_email:
                        continue
                    user_record = user_store.get_user_by_email(participant_email)
                    if not user_record:
                        continue
                    known_name = self._to_text(user_record.get("full_name"))
                    if not known_name:
                        continue
                    if known_name.strip().lower() == normalized_sentence_name:
                        return participant_index
        return None

    def _extract_participant_emails_from_participants(
        self,
        participants: list[Mapping[str, Any]],
    ) -> list[str]:
        emails: set[str] = set()
        for participant in participants:
            email = self._to_text(participant.get("email"))
            if not email:
                continue
            normalized_email = email.lower()
            if "@" not in normalized_email:
                continue
            emails.add(normalized_email)
        return sorted(emails)

    def _build_participants_from_emails(
        self,
        participant_emails: list[str],
    ) -> list[dict[str, Any]]:
        return self._normalize_participants(participant_emails)

    def _extract_string_values(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        normalized: list[str] = []
        for item in value:
            text = self._to_text(item)
            if text:
                normalized.append(text)
        return normalized

    def _normalize_participants(self, raw_participants: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_participants, list):
            return []

        participants: list[dict[str, Any]] = []
        for raw_participant in raw_participants:
            participant = self._normalize_participant(raw_participant)
            if participant:
                participants.append(participant)
        return self._deduplicate_participants(participants)

    def _normalize_participant(self, raw_participant: Any) -> dict[str, Any] | None:
        if isinstance(raw_participant, Mapping):
            return self._build_normalized_participant(
                email=(
                    raw_participant.get("email")
                    or raw_participant.get("mail")
                    or raw_participant.get("user_email")
                    or raw_participant.get("address")
                ),
                name=(
                    raw_participant.get("name")
                    or raw_participant.get("displayName")
                    or raw_participant.get("display_name")
                    or raw_participant.get("full_name")
                    or raw_participant.get("user_name")
                ),
                external_id=(
                    raw_participant.get("id")
                    or raw_participant.get("participant_id")
                    or raw_participant.get("user_id")
                    or raw_participant.get("external_id")
                    or raw_participant.get("uuid")
                ),
                role=(
                    raw_participant.get("role")
                    or raw_participant.get("type")
                    or raw_participant.get("participant_type")
                ),
            )

        participant_text = self._to_text(raw_participant)
        if not participant_text:
            return None
        if "@" in participant_text:
            return self._build_normalized_participant(email=participant_text)
        return self._build_normalized_participant(name=participant_text)

    def _build_normalized_participant(
        self,
        *,
        email: Any = None,
        name: Any = None,
        external_id: Any = None,
        role: Any = None,
    ) -> dict[str, Any] | None:
        normalized_email = self._to_text(email)
        normalized_name = self._to_text(name)
        normalized_external_id = self._to_text(external_id)
        normalized_role = self._to_text(role)
        if normalized_email:
            normalized_email = normalized_email.lower()
            if "@" not in normalized_email:
                normalized_email = None
        if not any((normalized_email, normalized_name, normalized_external_id)):
            return None
        return {
            "name": normalized_name,
            "email": normalized_email,
            "external_id": normalized_external_id,
            "role": normalized_role,
        }

    def _deduplicate_participants(
        self,
        participants: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        deduplicated: list[dict[str, Any]] = []
        index_by_key: dict[str, int] = {}

        for participant in participants:
            email = self._to_text(participant.get("email"))
            external_id = self._to_text(participant.get("external_id"))
            name = self._to_text(participant.get("name"))

            if email:
                dedupe_key = f"email:{email.lower()}"
            elif external_id:
                dedupe_key = f"id:{external_id.lower()}"
            elif name:
                dedupe_key = f"name:{name.lower()}"
            else:
                continue

            existing_index = index_by_key.get(dedupe_key)
            if existing_index is None:
                deduplicated.append(dict(participant))
                index_by_key[dedupe_key] = len(deduplicated) - 1
                continue

            existing = deduplicated[existing_index]
            for field in ("name", "email", "external_id", "role"):
                if not existing.get(field) and participant.get(field):
                    existing[field] = participant[field]
        return deduplicated

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
            "transcript.speaker_blocks",
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
            for key in ("text", "content", "transcript", "value", "words"):
                text = self._to_text(value.get(key))
                if text:
                    return text
            # Handle Read AI speaker_blocks structure implicitly via list recursion
            if "speaker_blocks" in value:
                return self._to_text(value["speaker_blocks"])
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
