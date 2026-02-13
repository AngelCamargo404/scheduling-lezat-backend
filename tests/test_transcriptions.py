import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import app
from app.services.action_item_creation_store import (
    clear_action_item_creation_store_cache,
    create_action_item_creation_store,
)
from app.services.action_item_sync_service import ActionItemSyncService
from app.services.transcription_service import TranscriptionService
from app.services.transcription_store import clear_transcription_store_cache
from app.services.user_store import clear_user_store_cache, create_user_store

client = TestClient(app)


@pytest.fixture(autouse=True)
def clear_settings_cache() -> None:
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("TRANSCRIPTIONS_STORE", "memory")
    monkeypatch.setenv("USER_DATA_STORE", "memory")
    monkeypatch.setenv("AUTH_SECRET_KEY", "test-secret")
    monkeypatch.setenv("DEFAULT_ADMIN_EMAIL", "admin")
    monkeypatch.setenv("DEFAULT_ADMIN_PASSWORD", "admin")
    monkeypatch.setenv("FIREFLIES_WEBHOOK_SECRET", "")
    monkeypatch.setenv("FIREFLIES_API_KEY", "")
    monkeypatch.setenv("READ_AI_WEBHOOK_SECRET", "")
    monkeypatch.setenv("GEMINI_API_KEY", "")
    monkeypatch.setenv("NOTION_API_TOKEN", "")
    monkeypatch.setenv("NOTION_TASKS_DATABASE_ID", "")
    monkeypatch.setenv("GOOGLE_CALENDAR_API_TOKEN", "")
    monkeypatch.setenv("GOOGLE_CALENDAR_REFRESH_TOKEN", "")
    monkeypatch.setenv("OUTLOOK_CALENDAR_API_TOKEN", "")
    get_settings.cache_clear()
    clear_transcription_store_cache()
    clear_action_item_creation_store_cache()
    clear_user_store_cache()
    yield
    monkeypatch.undo()
    get_settings.cache_clear()
    clear_transcription_store_cache()
    clear_action_item_creation_store_cache()
    clear_user_store_cache()


def _get_admin_token() -> str:
    response = client.post(
        "/api/auth/login",
        json={"email": "admin", "password": "admin"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


def test_fireflies_webhook_accepts_google_meet_transcript(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIREFLIES_WEBHOOK_SECRET", "")
    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-fireflies-1",
            "platform": "google_meet",
            "url": "https://meet.google.com/abc-defg-hij",
        },
        "transcript": {
            "id": "transcript-fireflies-1",
            "text": "Hola equipo, iniciamos la reunion de seguimiento.",
        },
    }

    response = client.post("/api/transcriptions/webhooks/fireflies", json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["provider"] == "fireflies"
    assert data["meeting_id"] == "meeting-fireflies-1"
    assert data["transcript_id"] == "transcript-fireflies-1"
    assert data["is_google_meet"] is True
    assert data["transcript_text_available"] is True
    assert data["enrichment_status"] == "skipped_missing_api_key"
    assert data["stored_record_id"]


def test_fireflies_webhook_accepts_valid_hmac_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "correct-secret"
    monkeypatch.setenv("FIREFLIES_WEBHOOK_SECRET", secret)
    payload = {
        "eventType": "transcription completed",
        "meetingId": "meeting-fireflies-2",
        "transcript": {"text": "Texto firmado"},
    }
    raw_body = json.dumps(payload).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()

    response = client.post(
        "/api/transcriptions/webhooks/fireflies",
        content=raw_body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature": f"sha256={signature}",
        },
    )

    assert response.status_code == 202
    data = response.json()
    assert data["provider"] == "fireflies"
    assert data["meeting_id"] == "meeting-fireflies-2"
    assert data["event_type"] == "transcription completed"
    assert data["transcript_text_available"] is True
    assert data["enrichment_status"] == "skipped_missing_api_key"
    assert data["stored_record_id"]


def test_read_ai_webhook_accepts_google_meet_transcript() -> None:
    payload = {
        "event_type": "meeting.completed",
        "meeting": {
            "external_id": "meeting-read-ai-1",
            "join_url": "https://meet.google.com/zyx-wvut-srq",
        },
        "summary": {
            "transcript": "Reunion finalizada, se definieron los siguientes acuerdos.",
        },
    }

    response = client.post("/api/transcriptions/webhooks/read-ai", json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["provider"] == "read_ai"
    assert data["meeting_id"] == "meeting-read-ai-1"
    assert data["is_google_meet"] is True
    assert data["transcript_text_available"] is True
    assert data["enrichment_status"] == "not_required"
    assert data["stored_record_id"]


def test_fireflies_webhook_returns_401_when_auth_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIREFLIES_WEBHOOK_SECRET", "correct-secret")

    payload = {
        "meeting": {"platform": "google_meet"},
        "transcript": {"text": "Texto"},
    }
    response = client.post(
        "/api/transcriptions/webhooks/fireflies",
        json=payload,
        headers={"X-Webhook-Secret": "wrong-secret"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid webhook signature."


def test_list_received_transcriptions_returns_saved_items() -> None:
    payload = {
        "event": "transcript.completed",
        "meeting": {"id": "meeting-list-1", "platform": "google_meet"},
        "transcript": {"text": "Primera transcripcion"},
    }
    client.post("/api/transcriptions/webhooks/fireflies", json=payload)

    response = client.get("/api/transcriptions/received")

    assert response.status_code == 200
    data = response.json()
    assert data["items"]
    assert data["items"][0]["meeting_id"] == "meeting-list-1"


def test_fireflies_webhook_fetches_transcript_with_meeting_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIREFLIES_API_KEY", "fake-api-key")

    def fake_fetch(self: TranscriptionService, meeting_id: str) -> dict[str, object]:
        assert meeting_id == "ASxwZxCstx"
        return {
            "id": "ASxwZxCstx",
            "meeting_link": "https://meet.google.com/abc-defg-hij",
            "meeting_attendees": [
                {"email": "cesar@example.com"},
                {"email": "alex@example.com"},
            ],
            "participants": ["alex@example.com"],
            "organizer_email": "cesar@example.com",
            "sentences": [
                {"index": 0, "speaker_name": "Cesar", "text": "Hola a todos"},
                {"index": 1, "speaker_name": "Alex", "text": "Seguimos con agenda"},
            ],
        }

    monkeypatch.setattr(TranscriptionService, "_fetch_fireflies_transcript", fake_fetch)

    payload = {
        "meetingId": "ASxwZxCstx",
        "eventType": "Transcription completed",
        "clientReferenceId": "be582c46-4ac9-4565-9ba6-6ab4264496a8",
    }
    response = client.post("/api/transcriptions/webhooks/fireflies", json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["meeting_id"] == "ASxwZxCstx"
    assert data["client_reference_id"] == "be582c46-4ac9-4565-9ba6-6ab4264496a8"
    assert data["transcript_text_available"] is True
    assert data["enrichment_status"] == "completed"
    assert data["is_google_meet"] is True

    lookup_response = client.get("/api/transcriptions/received/by-meeting/ASxwZxCstx")
    assert lookup_response.status_code == 200
    record = lookup_response.json()
    assert record["meeting_id"] == "ASxwZxCstx"
    assert record["transcript_text"] == "Hola a todos\nSeguimos con agenda"
    assert len(record["transcript_sentences"]) == 2
    assert record["transcript_sentences"][0]["speaker_name"] == "Cesar"
    assert record["participant_emails"] == ["alex@example.com", "cesar@example.com"]
    assert record["enrichment_status"] == "completed"
    assert record["fireflies_transcript"]["id"] == "ASxwZxCstx"


def test_backfill_updates_existing_record_by_meeting_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "meetingId": "legacy-meeting-1",
        "eventType": "Transcription completed",
        "clientReferenceId": "legacy-ref-1",
    }
    create_response = client.post("/api/transcriptions/webhooks/fireflies", json=payload)
    assert create_response.status_code == 202
    created_data = create_response.json()
    assert created_data["enrichment_status"] == "skipped_missing_api_key"
    assert created_data["transcript_text_available"] is False

    monkeypatch.setenv("FIREFLIES_API_KEY", "fake-api-key")
    get_settings.cache_clear()

    def fake_fetch(self: TranscriptionService, meeting_id: str) -> dict[str, object]:
        assert meeting_id == "legacy-meeting-1"
        return {
            "id": "transcript-legacy-1",
            "meeting_link": "https://meet.google.com/legacy-meeting-1",
            "meeting_attendees": [{"email": "legacy@example.com"}],
            "sentences": [
                {"index": 0, "speaker_name": "Legacy", "text": "Linea uno"},
                {"index": 1, "speaker_name": "Legacy", "text": "Linea dos"},
            ],
        }

    monkeypatch.setattr(TranscriptionService, "_fetch_fireflies_transcript", fake_fetch)

    token = _get_admin_token()
    backfill_response = client.post(
        "/api/transcriptions/backfill/legacy-meeting-1",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert backfill_response.status_code == 200
    backfill_data = backfill_response.json()
    assert backfill_data["meeting_id"] == "legacy-meeting-1"
    assert backfill_data["updated_count"] == 1
    assert backfill_data["record"]["transcript_text_available"] is True
    assert backfill_data["record"]["transcript_text"] == "Linea uno\nLinea dos"
    assert backfill_data["record"]["participant_emails"] == ["legacy@example.com"]
    assert len(backfill_data["record"]["transcript_sentences"]) == 2
    assert backfill_data["record"]["enrichment_status"] == "completed"


def test_webhook_persists_action_items_sync_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIREFLIES_WEBHOOK_SECRET", "")

    def fake_sync(
        self: TranscriptionService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        resolve_user_settings: bool = False,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-action-items-1"
        assert transcript_text == "Debes enviar la propuesta hoy."
        assert transcript_sentences == []
        assert participant_emails == []
        assert resolve_user_settings is True
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "items": [
                {
                    "title": "Enviar propuesta",
                    "status": "created",
                    "notion_page_id": "notion-page-1",
                },
            ],
            "error": None,
        }

    monkeypatch.setattr(TranscriptionService, "_sync_action_items", fake_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-action-items-1",
            "platform": "google_meet",
        },
        "transcript": {
            "id": "transcript-action-items-1",
            "text": "Debes enviar la propuesta hoy.",
        },
    }

    create_response = client.post("/api/transcriptions/webhooks/fireflies", json=payload)
    assert create_response.status_code == 202
    response_payload = create_response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 1

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-action-items-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    assert stored_record["action_items_sync"]["status"] == "completed"
    assert stored_record["action_items_sync"]["created_count"] == 1

    settings = get_settings()
    creation_store = create_action_item_creation_store(
        store_name=settings.transcriptions_store,
        mongodb_uri=settings.mongodb_uri,
        mongodb_db_name=settings.mongodb_db_name,
        mongodb_collection_name=settings.mongodb_action_item_creations_collection,
        mongodb_connect_timeout_ms=settings.mongodb_connect_timeout_ms,
    )
    creation_records = creation_store.list_recent(limit=10, meeting_id="meeting-action-items-1")
    assert len(creation_records) == 1
    creation_record = creation_records[0]
    assert creation_record["source"] == "webhook"
    assert creation_record["provider"] == "fireflies"
    assert creation_record["meeting_id"] == "meeting-action-items-1"
    assert creation_record["title"] == "Enviar propuesta"
    assert creation_record["notion_status"] == "created"
    assert creation_record["notion_page_id"] == "notion-page-1"


def test_webhook_uses_user_integration_tokens_based_on_participant_email(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIREFLIES_API_KEY", "fake-api-key")
    monkeypatch.setenv("NOTION_API_TOKEN", "")
    monkeypatch.setenv("NOTION_TASKS_DATABASE_ID", "")
    monkeypatch.setenv("GOOGLE_CALENDAR_API_TOKEN", "")
    monkeypatch.setenv("OUTLOOK_CALENDAR_API_TOKEN", "")
    get_settings.cache_clear()
    clear_user_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    user = user_store.create_user(
        email="adcamargo10@gmail.com",
        full_name="Ada Camargo",
        password_hash="hashed",
        role="user",
    )
    user_store.upsert_user_settings_values(
        str(user["_id"]),
        {
            "NOTION_API_TOKEN": "user-notion-token",
            "NOTION_TASKS_DATABASE_ID": "user-notion-db",
            "GOOGLE_CALENDAR_API_TOKEN": "user-google-token",
            "OUTLOOK_CALENDAR_API_TOKEN": "user-outlook-token",
        },
    )

    def fake_fetch(self: TranscriptionService, meeting_id: str) -> dict[str, object]:
        assert meeting_id == "meeting-user-settings-1"
        return {
            "id": "meeting-user-settings-1",
            "meeting_link": "https://meet.google.com/abc-defg-hij",
            "meeting_attendees": [{"email": "adcamargo10@gmail.com"}],
            "sentences": [{"index": 0, "speaker_name": "Ada", "text": "Enviar propuesta manana."}],
        }

    monkeypatch.setattr(TranscriptionService, "_fetch_fireflies_transcript", fake_fetch)

    captured_tokens: dict[str, str] = {}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
    ) -> dict[str, object]:
        captured_tokens["notion"] = self.settings.notion_api_token
        captured_tokens["notion_db"] = self.settings.notion_tasks_database_id
        captured_tokens["google"] = self.settings.google_calendar_api_token
        captured_tokens["outlook"] = self.settings.outlook_calendar_api_token
        assert meeting_id == "meeting-user-settings-1"
        assert transcript_text is not None
        assert transcript_sentences
        assert participant_emails == ["adcamargo10@gmail.com"]
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "completed",
            "google_calendar_created_count": 1,
            "google_calendar_error": None,
            "outlook_calendar_status": "completed",
            "outlook_calendar_created_count": 1,
            "outlook_calendar_error": None,
            "items": [],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "meetingId": "meeting-user-settings-1",
        "eventType": "Transcription completed",
    }
    response = client.post("/api/transcriptions/webhooks/fireflies", json=payload)
    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 1
    assert captured_tokens["notion"] == "user-notion-token"
    assert captured_tokens["notion_db"] == "user-notion-db"
    assert captured_tokens["google"] == "user-google-token"
    assert captured_tokens["outlook"] == "user-outlook-token"


def test_webhook_skips_action_item_sync_when_autosync_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRANSCRIPTION_AUTOSYNC_ENABLED", "false")
    get_settings.cache_clear()

    calls = {"sync": 0}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
    ) -> dict[str, object]:
        calls["sync"] += 1
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "items": [],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {"id": "meeting-autosync-off-1", "platform": "google_meet"},
        "transcript": {"text": "Crear tarea de seguimiento para el viernes."},
    }
    response = client.post("/api/transcriptions/webhooks/fireflies", json=payload)

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "skipped_disabled_by_user"
    assert response_payload["action_items_created_count"] == 0
    assert calls["sync"] == 0

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-autosync-off-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    assert stored_record["action_items_sync"]["status"] == "skipped_disabled_by_user"
    assert stored_record["action_items_sync"]["created_count"] == 0
