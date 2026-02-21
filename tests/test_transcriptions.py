import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import app
from app.schemas.transcription import TranscriptionProvider
from app.services.action_item_creation_store import (
    clear_action_item_creation_store_cache,
    create_action_item_creation_store,
)
from app.services.action_item_sync_service import ActionItemSyncService
from app.services.team_membership_store import (
    clear_team_membership_store_cache,
    create_team_membership_store,
)
from app.services.transcription_service import (
    TranscriptionService,
    clear_transcription_processing_locks,
)
from app.services.transcription_store import clear_transcription_store_cache
from app.services.user_store import clear_user_store_cache, create_user_store

client = TestClient(app)


@pytest.fixture(autouse=True)
def clear_settings_cache() -> None:
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("TRANSCRIPTIONS_STORE", "memory")
    monkeypatch.setenv("USER_DATA_STORE", "memory")
    monkeypatch.setenv("GEMINI_API_KEY", "")
    get_settings.cache_clear()
    clear_transcription_store_cache()
    clear_action_item_creation_store_cache()
    clear_user_store_cache()
    clear_team_membership_store_cache()
    clear_transcription_processing_locks()
    yield
    monkeypatch.undo()
    get_settings.cache_clear()
    clear_transcription_store_cache()
    clear_action_item_creation_store_cache()
    clear_user_store_cache()
    clear_team_membership_store_cache()
    clear_transcription_processing_locks()


def _get_admin_token() -> str:
    response = client.post(
        "/api/auth/login",
        json={"email": "admin", "password": "admin"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


def _get_default_webhook_user_id() -> str:
    settings = get_settings()
    user_store = create_user_store(settings)
    admin_user = user_store.get_user_by_email("admin")
    if admin_user:
        return str(admin_user["_id"])
    user = user_store.create_user(
        email="webhook.default@example.com",
        full_name="Webhook Default",
        password_hash="hashed",
        role="user",
    )
    return str(user["_id"])


def _fireflies_webhook_url() -> str:
    return f"/api/transcriptions/webhooks/fireflies/{_get_default_webhook_user_id()}"


def _read_ai_webhook_url() -> str:
    return f"/api/transcriptions/webhooks/read-ai/{_get_default_webhook_user_id()}"


def test_unscoped_fireflies_webhook_is_rejected() -> None:
    response = client.post(
        "/api/transcriptions/webhooks/fireflies",
        json={"meetingId": "meeting-without-user-scope"},
    )
    assert response.status_code == 422
    assert "user_id" in response.json()["detail"]


def test_unscoped_read_ai_webhook_is_rejected() -> None:
    response = client.post(
        "/api/transcriptions/webhooks/read-ai",
        json={"meeting": {"external_id": "meeting-without-user-scope"}},
    )
    assert response.status_code == 422
    assert "user_id" in response.json()["detail"]


def test_fireflies_webhook_accepts_google_meet_transcript(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    response = client.post(_fireflies_webhook_url(), json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["provider"] == "fireflies"
    assert data["meeting_id"] == "meeting-fireflies-1"
    assert data["transcript_id"] == "transcript-fireflies-1"
    assert data["is_google_meet"] is True
    assert data["transcript_text_available"] is True
    assert data["enrichment_status"] == "completed_payload"
    assert data["stored_record_id"]


def test_fireflies_webhook_is_idempotent_with_same_meeting_and_transcript_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_calls = {"count": 0}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
    ) -> dict[str, object]:
        sync_calls["count"] += 1
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "monday_status": "not_required_no_action_items",
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
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload_v1 = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-idempotent-1",
            "platform": "google_meet",
        },
        "transcript": {
            "id": "transcript-idempotent-1",
            "text": "Primer payload.",
        },
        "delivery_id": "delivery-1",
    }
    payload_v2 = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-idempotent-1",
            "platform": "google_meet",
        },
        "transcript": {
            "id": "transcript-idempotent-1",
            "text": "Segundo payload con metadata distinta.",
        },
        "delivery_id": "delivery-2",
        "received_at": "2026-02-19T10:00:00Z",
    }

    first_response = client.post(_fireflies_webhook_url(), json=payload_v1)
    second_response = client.post(_fireflies_webhook_url(), json=payload_v2)

    assert first_response.status_code == 202
    assert second_response.status_code == 202
    assert sync_calls["count"] == 1
    assert first_response.json()["stored_record_id"] == second_response.json()["stored_record_id"]
    assert second_response.json()["enrichment_status"] == "skipped_duplicate"


def test_fireflies_webhook_accepts_valid_hmac_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "correct-secret"
    payload = {
        "eventType": "transcription completed",
        "meetingId": "meeting-fireflies-2",
        "transcript": {"text": "Texto firmado"},
    }
    raw_body = json.dumps(payload).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()

    response = client.post(
        _fireflies_webhook_url(),
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
    assert data["enrichment_status"] == "completed_payload"
    assert data["stored_record_id"]


def test_fireflies_webhook_with_payload_transcript_skips_enrichment_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIREFLIES_API_KEY", "configured-fireflies-key")
    get_settings.cache_clear()

    fetch_calls = {"count": 0}

    def fake_fetch(
        self: TranscriptionService,
        meeting_id: str,
        fireflies_client: object | None = None,
    ) -> dict[str, object]:
        fetch_calls["count"] += 1
        return {
            "id": meeting_id,
            "sentences": [{"index": 0, "speaker_name": "Bot", "text": "No deberia usar fetch."}],
        }

    monkeypatch.setattr(TranscriptionService, "_fetch_fireflies_transcript", fake_fetch)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-fireflies-payload-only-1",
            "platform": "google_meet",
            "url": "https://meet.google.com/fireflies-payload-only-1",
        },
        "transcript": {
            "id": "transcript-fireflies-payload-only-1",
            "text": "Texto ya disponible en webhook Fireflies.",
        },
    }

    response = client.post(_fireflies_webhook_url(), json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["enrichment_status"] == "completed_payload"
    assert data["transcript_text_available"] is True
    assert fetch_calls["count"] == 0


def test_fireflies_webhook_returns_inflight_duplicate_without_reprocessing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_calls = {"count": 0}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
    ) -> dict[str, object]:
        sync_calls["count"] += 1
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "not_required_no_due_dates",
            "google_calendar_created_count": 0,
            "google_calendar_error": None,
            "outlook_calendar_status": "not_required_no_due_dates",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": None,
            "items": [],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {"id": "meeting-fireflies-inflight-duplicate-1"},
        "transcript": {
            "id": "transcript-fireflies-inflight-duplicate-1",
            "text": "Contenido para validar lock de ingestion en Fireflies.",
        },
    }

    service = TranscriptionService(get_settings())
    ingestion_key = service._build_ingestion_key(
        provider=TranscriptionProvider.fireflies,
        payload=payload,
    )
    assert ingestion_key
    assert service._try_acquire_ingestion_processing_slot(ingestion_key)

    try:
        response = service.process_webhook(
            provider=TranscriptionProvider.fireflies,
            payload=payload,
            shared_secret=service.settings.fireflies_webhook_secret or None,
        )
    finally:
        service._release_ingestion_processing_slot(ingestion_key)

    assert response.enrichment_status == "skipped_duplicate_in_progress"
    assert response.action_items_sync_status == "skipped_duplicate_in_progress"
    assert sync_calls["count"] == 0


def test_read_ai_webhook_accepts_google_meet_transcript() -> None:
    payload = {
        "event_type": "meeting.completed",
        "meeting": {
            "external_id": "meeting-read-ai-1",
            "join_url": "https://meet.google.com/zyx-wvut-srq",
        },
        "participants": [
            {"name": "Ana", "email": "ana@example.com"},
            {"name": "Luis", "email": "luis@example.com"},
        ],
        "summary": {
            "transcript": "Reunion finalizada, se definieron los siguientes acuerdos.",
        },
    }

    response = client.post(_read_ai_webhook_url(), json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["provider"] == "read_ai"
    assert data["meeting_id"] == "meeting-read-ai-1"
    assert data["is_google_meet"] is True
    assert data["transcript_text_available"] is True
    assert data["enrichment_status"] == "completed_payload"
    assert data["stored_record_id"]

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-read-ai-1")
    assert lookup_response.status_code == 200
    record = lookup_response.json()
    assert record["participant_emails"] == ["ana@example.com", "luis@example.com"]
    assert record["participants"] == [
        {"name": "Ana", "email": "ana@example.com", "external_id": None, "role": None},
        {"name": "Luis", "email": "luis@example.com", "external_id": None, "role": None},
    ]


def test_read_ai_webhook_with_payload_transcript_skips_enrichment_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("READ_AI_API_KEY", "configured-read-ai-key")
    get_settings.cache_clear()

    fetch_calls = {"count": 0}

    def fake_fetch(
        self: TranscriptionService,
        meeting_id: str,
        read_ai_client: object | None = None,
    ) -> dict[str, object]:
        fetch_calls["count"] += 1
        return {"sentences": [{"text": "No deberia ser necesario."}]}

    monkeypatch.setattr(TranscriptionService, "_fetch_read_ai_transcript", fake_fetch)

    payload = {
        "event_type": "meeting.completed",
        "meeting": {
            "external_id": "meeting-read-ai-payload-only-1",
            "join_url": "https://meet.google.com/abc-defg-hij",
        },
        "summary": {
            "transcript": "Texto ya disponible desde Read AI webhook.",
        },
    }

    response = client.post(_read_ai_webhook_url(), json=payload)

    assert response.status_code == 202
    data = response.json()
    assert data["enrichment_status"] == "completed_payload"
    assert data["transcript_text_available"] is True
    assert fetch_calls["count"] == 0


def test_read_ai_webhook_returns_inflight_duplicate_without_reprocessing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_calls = {"count": 0}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
    ) -> dict[str, object]:
        sync_calls["count"] += 1
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "not_required_no_due_dates",
            "google_calendar_created_count": 0,
            "google_calendar_error": None,
            "outlook_calendar_status": "not_required_no_due_dates",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": None,
            "items": [],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event_type": "meeting.completed",
        "meeting": {"external_id": "meeting-read-ai-inflight-duplicate-1"},
        "summary": {"transcript": "Contenido para validar lock de ingestion."},
    }

    service = TranscriptionService(get_settings())
    ingestion_key = service._build_ingestion_key(
        provider=TranscriptionProvider.read_ai,
        payload=payload,
    )
    assert ingestion_key
    assert service._try_acquire_ingestion_processing_slot(ingestion_key)

    try:
        response = service.process_webhook(
            provider=TranscriptionProvider.read_ai,
            payload=payload,
            shared_secret=None,
        )
    finally:
        service._release_ingestion_processing_slot(ingestion_key)

    assert response.enrichment_status == "skipped_duplicate_in_progress"
    assert response.action_items_sync_status == "skipped_duplicate_in_progress"
    assert sync_calls["count"] == 0


def test_fireflies_webhook_with_user_id_uses_user_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    user_store = create_user_store(settings)
    user = user_store.create_user(
        email="webhook.fireflies@example.com",
        full_name="Webhook Fireflies",
        password_hash="hashed",
        role="user",
    )
    user_store.upsert_user_settings_values(
        str(user["_id"]),
        {"FIREFLIES_API_KEY": "user-fireflies-key"},
    )

    def fake_fetch(
        self: TranscriptionService,
        meeting_id: str,
        fireflies_client: object | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-fireflies-user-1"
        assert fireflies_client is not None
        return {
            "id": "meeting-fireflies-user-1",
            "meeting_link": "https://meet.google.com/abc-defg-hij",
            "meeting_attendees": [{"email": "webhook.fireflies@example.com"}],
            "sentences": [{"index": 0, "speaker_name": "User", "text": "Tarea 1"}],
        }

    monkeypatch.setattr(TranscriptionService, "_fetch_fireflies_transcript", fake_fetch)

    payload = {
        "meetingId": "meeting-fireflies-user-1",
        "eventType": "Transcription completed",
    }
    response = client.post(
        f"/api/transcriptions/webhooks/fireflies/{user['_id']}",
        json=payload,
    )

    assert response.status_code == 202
    data = response.json()
    assert data["meeting_id"] == "meeting-fireflies-user-1"
    assert data["enrichment_status"] == "completed"
    assert data["transcript_text_available"] is True


def test_fireflies_webhook_with_unknown_user_id_returns_404() -> None:
    payload = {
        "meetingId": "meeting-fireflies-unknown-user",
        "eventType": "Transcription completed",
    }
    response = client.post(
        "/api/transcriptions/webhooks/fireflies/unknown-user-id",
        json=payload,
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "User not found for webhook URL."


def test_read_ai_webhook_with_user_id_uses_user_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    user_store = create_user_store(settings)
    user = user_store.create_user(
        email="webhook.readai@example.com",
        full_name="Webhook Read AI",
        password_hash="hashed",
        role="user",
    )
    user_store.upsert_user_settings_values(
        str(user["_id"]),
        {"READ_AI_API_KEY": "user-read-ai-key"},
    )

    def fake_fetch(
        self: TranscriptionService,
        meeting_id: str,
        read_ai_client: object | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-read-ai-user-1"
        assert read_ai_client is not None
        return {
            "participants": [{"email": "webhook.readai@example.com"}],
            "sentences": [{"text": "Acuerdo 1"}],
        }

    monkeypatch.setattr(TranscriptionService, "_fetch_read_ai_transcript", fake_fetch)

    payload = {
        "event_type": "meeting.completed",
        "meeting": {"external_id": "meeting-read-ai-user-1"},
    }
    response = client.post(
        f"/api/transcriptions/webhooks/read-ai/{user['_id']}",
        json=payload,
    )

    assert response.status_code == 202
    data = response.json()
    assert data["meeting_id"] == "meeting-read-ai-user-1"
    assert data["enrichment_status"] == "completed"
    assert data["transcript_text_available"] is True


def test_fireflies_webhook_accepts_request_even_with_invalid_webhook_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "meeting": {"platform": "google_meet"},
        "transcript": {"text": "Texto"},
    }
    response = client.post(
        _fireflies_webhook_url(),
        json=payload,
        headers={"X-Webhook-Secret": "wrong-secret"},
    )

    assert response.status_code == 202


def test_list_received_transcriptions_returns_saved_items() -> None:
    payload = {
        "event": "transcript.completed",
        "meeting": {"id": "meeting-list-1", "platform": "google_meet"},
        "transcript": {"text": "Primera transcripcion"},
    }
    client.post(_fireflies_webhook_url(), json=payload)

    response = client.get("/api/transcriptions/received")

    assert response.status_code == 200
    data = response.json()
    assert data["items"]
    assert data["items"][0]["meeting_id"] == "meeting-list-1"


def test_fireflies_webhook_fetches_transcript_with_meeting_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    user_store = create_user_store(settings)
    user = user_store.create_user(
        email="cesar@example.com",
        full_name="Cesar",
        password_hash="hashed",
        role="user",
    )
    user_store.upsert_user_settings_values(
        str(user["_id"]),
        {"FIREFLIES_API_KEY": "fake-api-key"},
    )

    def fake_fetch(
        self: TranscriptionService,
        meeting_id: str,
        fireflies_client: object | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "ASxwZxCstx"
        assert fireflies_client is not None
        return {
            "id": "ASxwZxCstx",
            "meeting_link": "https://meet.google.com/abc-defg-hij",
            "meeting_attendees": [
                {"email": "cesar@example.com", "name": "Cesar"},
                {"email": "alex@example.com", "name": "Alex"},
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
        "participants": ["cesar@example.com"],
    }
    response = client.post(_fireflies_webhook_url(), json=payload)

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
    assert record["participants"] == [
        {
            "name": "Cesar",
            "email": "cesar@example.com",
            "external_id": None,
            "role": "organizer",
        },
        {"name": "Alex", "email": "alex@example.com", "external_id": None, "role": None},
    ]
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
    create_response = client.post(_fireflies_webhook_url(), json=payload)
    assert create_response.status_code == 202
    created_data = create_response.json()
    assert created_data["enrichment_status"] == "skipped_missing_api_key"
    assert created_data["transcript_text_available"] is False

    token = _get_admin_token()
    settings = get_settings()
    user_store = create_user_store(settings)
    admin_user = user_store.get_user_by_email("admin")
    assert admin_user is not None
    user_store.upsert_user_settings_values(
        str(admin_user["_id"]),
        {"FIREFLIES_API_KEY": "fake-api-key"},
    )

    def fake_fetch(
        self: TranscriptionService,
        meeting_id: str,
        fireflies_client: object | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "legacy-meeting-1"
        assert fireflies_client is not None
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
    def fake_sync(
        self: TranscriptionService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        resolve_user_settings: bool = False,
        user_settings_user_id: str | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-action-items-1"
        assert transcript_text == "Debes enviar la propuesta hoy."
        assert transcript_sentences == []
        assert participant_emails == []
        assert resolve_user_settings is True
        assert user_settings_user_id is None
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

    create_response = client.post(_fireflies_webhook_url(), json=payload)
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
            "FIREFLIES_API_KEY": "user-fireflies-token",
            "NOTION_API_TOKEN": "user-notion-token",
            "NOTION_TASKS_DATABASE_ID": "user-notion-db",
            "GOOGLE_CALENDAR_API_TOKEN": "user-google-token",
            "OUTLOOK_CALENDAR_API_TOKEN": "user-outlook-token",
        },
    )

    def fake_fetch(
        self: TranscriptionService,
        meeting_id: str,
        fireflies_client: object | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-user-settings-1"
        assert fireflies_client is not None
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
        calendar_attendee_emails: list[str] | None = None,
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
        "participants": ["adcamargo10@gmail.com"],
    }
    response = client.post(_fireflies_webhook_url(), json=payload)
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
    clear_user_store_cache()
    settings = get_settings()
    user_store = create_user_store(settings)
    user = user_store.create_user(
        email="autosync.off@example.com",
        full_name="Autosync Off",
        password_hash="hashed",
        role="user",
    )
    user_store.upsert_user_settings_values(
        str(user["_id"]),
        {"TRANSCRIPTION_AUTOSYNC_ENABLED": "false"},
    )

    calls = {"sync": 0}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
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
        "participants": ["autosync.off@example.com"],
        "transcript": {"text": "Crear tarea de seguimiento para el viernes."},
    }
    response = client.post(_fireflies_webhook_url(), json=payload)

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


def test_webhook_routes_action_items_to_configured_team_recipients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.team@example.com",
        full_name="Lead Team",
        password_hash="hashed",
        role="user",
    )
    member = user_store.create_user(
        email="member.team@example.com",
        full_name="Member Team",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])
    member_user_id = str(member["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )
    user_store.upsert_user_settings_values(
        member_user_id,
        {
            "NOTION_API_TOKEN": "member-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    team = team_store.create_team(
        name="Equipo Shared",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id, member_user_id],
    )
    team_id = str(team["_id"])
    team_store.upsert_membership(
        team_id=team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=team_id,
        user_id=member_user_id,
        role="member",
        status="accepted",
    )

    captured_notion_tokens: list[str] = []

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
    ) -> dict[str, object]:
        captured_notion_tokens.append(self.settings.notion_api_token)
        assert meeting_id == "meeting-team-routing-1"
        assert transcript_text is not None
        assert transcript_sentences == []
        assert participant_emails == ["member.team@example.com"]
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "not_required_no_due_dates",
            "google_calendar_created_count": 0,
            "google_calendar_error": None,
            "outlook_calendar_status": "not_required_no_due_dates",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": None,
            "items": [{"title": "Tarea", "status": "created"}],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-team-routing-1",
            "platform": "google_meet",
        },
        "participants": ["member.team@example.com"],
        "transcript": {
            "id": "transcript-team-routing-1",
            "text": "Registrar acuerdo del equipo.",
        },
    }
    response = client.post(_fireflies_webhook_url(), json=payload)

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 2
    assert sorted(captured_notion_tokens) == [
        "lead-notion-token",
        "member-notion-token",
    ]

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-team-routing-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    assert stored_record["action_items_sync"]["routed_via_team_memberships"] is True
    assert stored_record["action_items_sync"]["matched_team_ids"] == [team_id]
    assert len(stored_record["action_items_sync"]["target_users"]) == 2
    assert stored_record["action_items_sync"]["created_count"] == 2


def test_webhook_routes_action_items_to_checked_team_recipients_when_participant_matches_team(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.all-members@example.com",
        full_name="Lead All Members",
        password_hash="hashed",
        role="user",
    )
    member = user_store.create_user(
        email="member.all-members@example.com",
        full_name="Member All Members",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])
    member_user_id = str(member["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-all-members-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )
    user_store.upsert_user_settings_values(
        member_user_id,
        {
            "NOTION_API_TOKEN": "member-all-members-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    team = team_store.create_team(
        name="Equipo All Members",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id],
    )
    team_id = str(team["_id"])
    team_store.upsert_membership(
        team_id=team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=team_id,
        user_id=member_user_id,
        role="member",
        status="accepted",
    )

    captured_notion_tokens: list[str] = []

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
        skip_google_meeting_items: bool = False,
        skip_outlook_meeting_items: bool = False,
        pre_extracted_action_items: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        captured_notion_tokens.append(self.settings.notion_api_token)
        assert meeting_id == "meeting-team-all-members-1"
        assert participant_emails == ["member.all-members@example.com"]
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "not_required_no_due_dates",
            "google_calendar_created_count": 0,
            "google_calendar_error": None,
            "outlook_calendar_status": "not_required_no_due_dates",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": None,
            "items": [{"title": "Tarea", "status": "created"}],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-team-all-members-1",
            "platform": "google_meet",
        },
        "participants": ["member.all-members@example.com"],
        "transcript": {
            "id": "transcript-team-all-members-1",
            "text": "Registrar acuerdo para todos.",
        },
    }
    response = client.post(_fireflies_webhook_url(), json=payload)

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 1
    assert captured_notion_tokens == ["lead-all-members-notion-token"]

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-team-all-members-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    assert stored_record["action_items_sync"]["routed_via_team_memberships"] is True
    assert stored_record["action_items_sync"]["matched_team_ids"] == [team_id]
    assert len(stored_record["action_items_sync"]["target_users"]) == 1
    assert stored_record["action_items_sync"]["created_count"] == 1


def test_webhook_reuses_extracted_action_items_for_team_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.cached@example.com",
        full_name="Lead Cached",
        password_hash="hashed",
        role="user",
    )
    member = user_store.create_user(
        email="member.cached@example.com",
        full_name="Member Cached",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])
    member_user_id = str(member["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-cached-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )
    user_store.upsert_user_settings_values(
        member_user_id,
        {
            "NOTION_API_TOKEN": "member-cached-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    team = team_store.create_team(
        name="Equipo Cached Extraction",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id, member_user_id],
    )
    team_id = str(team["_id"])
    team_store.upsert_membership(
        team_id=team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=team_id,
        user_id=member_user_id,
        role="member",
        status="accepted",
    )

    captured_pre_extracted: dict[str, list[dict[str, object]] | None] = {}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
        pre_extracted_action_items: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        token = self.settings.notion_api_token
        captured_pre_extracted[token] = pre_extracted_action_items

        if token == "lead-cached-notion-token":
            assert pre_extracted_action_items is None
            return {
                "status": "completed",
                "extracted_count": 1,
                "created_count": 1,
                "google_calendar_status": "not_required_no_due_dates",
                "google_calendar_created_count": 0,
                "google_calendar_error": None,
                "outlook_calendar_status": "not_required_no_due_dates",
                "outlook_calendar_created_count": 0,
                "outlook_calendar_error": None,
                "items": [
                    {
                        "title": "Entrega de credenciales",
                        "assignee_email": None,
                        "assignee_name": None,
                        "due_date": "2026-04-14",
                        "details": "Se debe realizar la entrega.",
                        "source_sentence": "Se realizará la entrega de credenciales.",
                        "scheduled_start": None,
                        "scheduled_end": None,
                        "event_timezone": None,
                        "recurrence_rule": None,
                        "online_meeting_platform": None,
                        "status": "created",
                    },
                ],
                "error": None,
            }

        if token == "member-cached-notion-token":
            assert pre_extracted_action_items is not None
            assert len(pre_extracted_action_items) == 1
            assert pre_extracted_action_items[0]["title"] == "Entrega de credenciales"
            return {
                "status": "completed",
                "extracted_count": 1,
                "created_count": 1,
                "google_calendar_status": "not_required_no_due_dates",
                "google_calendar_created_count": 0,
                "google_calendar_error": None,
                "outlook_calendar_status": "not_required_no_due_dates",
                "outlook_calendar_created_count": 0,
                "outlook_calendar_error": None,
                "items": [
                    {
                        **pre_extracted_action_items[0],
                        "status": "created",
                    },
                ],
                "error": None,
            }

        raise AssertionError(f"Notion token inesperado: {token}")

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-team-cached-extraction-1",
            "platform": "google_meet",
        },
        "participants": ["member.cached@example.com"],
        "transcript": {
            "id": "transcript-team-cached-extraction-1",
            "text": "Crear entrega de credenciales para el equipo.",
        },
    }
    response = client.post(_fireflies_webhook_url(), json=payload)

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 2
    assert captured_pre_extracted["lead-cached-notion-token"] is None
    assert captured_pre_extracted["member-cached-notion-token"] is not None

    lookup_response = client.get(
        "/api/transcriptions/received/by-meeting/meeting-team-cached-extraction-1",
    )
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    action_items_sync = stored_record["action_items_sync"]
    assert action_items_sync["routed_via_team_memberships"] is True
    assert action_items_sync["matched_team_ids"] == [team_id]
    target_users = {
        target["user_id"]: target
        for target in action_items_sync["target_users"]
    }
    assert sorted(target_users.keys()) == sorted([lead_user_id, member_user_id])
    assert target_users[lead_user_id]["status"] == "completed"
    assert target_users[member_user_id]["status"] == "completed"
    assert target_users[member_user_id]["created_count"] == 1


def test_user_scoped_webhook_routes_to_lead_and_members_with_user_specific_outputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.outputs@example.com",
        full_name="Lead Outputs",
        password_hash="hashed",
        role="user",
    )
    member = user_store.create_user(
        email="member.outputs@example.com",
        full_name="Member Outputs",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])
    member_user_id = str(member["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-notion-token",
            "GOOGLE_CALENDAR_API_TOKEN": "lead-google-token",
            "OUTLOOK_CALENDAR_API_TOKEN": "",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )
    user_store.upsert_user_settings_values(
        member_user_id,
        {
            "NOTION_API_TOKEN": "member-notion-token",
            "GOOGLE_CALENDAR_API_TOKEN": "",
            "OUTLOOK_CALENDAR_API_TOKEN": "member-outlook-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    team = team_store.create_team(
        name="Equipo Outputs",
        created_by_user_id=lead_user_id,
        # Keep member out of recipient list on purpose: user-scoped webhooks
        # must honor checked recipients only.
        recipient_user_ids=[lead_user_id],
    )
    team_id = str(team["_id"])
    team_store.upsert_membership(
        team_id=team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=team_id,
        user_id=member_user_id,
        role="member",
        status="accepted",
    )

    captured_user_channels: list[tuple[str, str]] = []

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-lead-user-scope-1"
        assert transcript_text == "Enviar resumen y crear tareas."
        assert transcript_sentences == []
        assert participant_emails == ["external.guest@example.com"]

        google_token = self.settings.google_calendar_api_token
        outlook_token = self.settings.outlook_calendar_api_token
        captured_user_channels.append((google_token, outlook_token))

        if google_token == "lead-google-token":
            return {
                "status": "completed",
                "extracted_count": 1,
                "created_count": 1,
                "google_calendar_status": "completed",
                "google_calendar_created_count": 1,
                "google_calendar_error": None,
                "outlook_calendar_status": "not_required_no_due_dates",
                "outlook_calendar_created_count": 0,
                "outlook_calendar_error": None,
                "items": [],
                "error": None,
            }

        if outlook_token == "member-outlook-token":
            return {
                "status": "completed",
                "extracted_count": 1,
                "created_count": 1,
                "google_calendar_status": "not_required_no_due_dates",
                "google_calendar_created_count": 0,
                "google_calendar_error": None,
                "outlook_calendar_status": "completed",
                "outlook_calendar_created_count": 1,
                "outlook_calendar_error": None,
                "items": [],
                "error": None,
            }

        return {
            "status": "failed_unexpected",
            "extracted_count": 0,
            "created_count": 0,
            "google_calendar_status": "failed_sync",
            "google_calendar_created_count": 0,
            "google_calendar_error": "Unexpected token combination.",
            "outlook_calendar_status": "failed_sync",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": "Unexpected token combination.",
            "items": [],
            "error": "Unexpected token combination.",
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-lead-user-scope-1",
            "platform": "google_meet",
        },
        # No internal participant emails: team routing must still work
        # because webhook is scoped to the lead user id.
        "participants": ["external.guest@example.com"],
        "transcript": {
            "id": "transcript-lead-user-scope-1",
            "text": "Enviar resumen y crear tareas.",
        },
    }
    response = client.post(
        f"/api/transcriptions/webhooks/fireflies/{lead_user_id}",
        json=payload,
    )

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 1
    assert captured_user_channels == [("lead-google-token", "")]

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-lead-user-scope-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    action_items_sync = stored_record["action_items_sync"]
    assert action_items_sync["routed_via_team_memberships"] is True
    assert action_items_sync["matched_team_ids"] == [team_id]
    assert action_items_sync["created_count"] == 1
    assert action_items_sync["google_calendar_created_count"] == 1
    assert action_items_sync["outlook_calendar_created_count"] == 0

    target_users = {
        target["user_id"]: target
        for target in action_items_sync["target_users"]
    }
    assert sorted(target_users.keys()) == [lead_user_id]
    assert target_users[lead_user_id]["google_calendar_status"] == "completed"
    assert target_users[lead_user_id]["outlook_calendar_status"] == "not_required_no_due_dates"


def test_user_scoped_webhook_shares_meeting_links_for_team_and_invites_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.shared@example.com",
        full_name="Lead Shared",
        password_hash="hashed",
        role="user",
    )
    member = user_store.create_user(
        email="member.shared@example.com",
        full_name="Member Shared",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])
    member_user_id = str(member["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-notion-token",
            "GOOGLE_CALENDAR_API_TOKEN": "lead-google-token",
            "OUTLOOK_CALENDAR_API_TOKEN": "lead-outlook-token",
            "TEAM_LEADER_TIMEZONE": "America/Mexico_City",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )
    user_store.upsert_user_settings_values(
        member_user_id,
        {
            "NOTION_API_TOKEN": "member-notion-token",
            "GOOGLE_CALENDAR_API_TOKEN": "member-google-token",
            "OUTLOOK_CALENDAR_API_TOKEN": "member-outlook-token",
            "TEAM_LEADER_TIMEZONE": "Europe/Madrid",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    team = team_store.create_team(
        name="Equipo Shared Links",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id, member_user_id],
    )
    team_id = str(team["_id"])
    team_store.upsert_membership(
        team_id=team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=team_id,
        user_id=member_user_id,
        role="member",
        status="accepted",
    )
    team_store.create_or_get_pending_invitation(
        team_id=team_id,
        invited_email="pending.shared@example.com",
        invited_by_user_id=lead_user_id,
    )

    captured_calls: list[dict[str, object]] = []

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
        skip_google_meeting_items: bool = False,
        skip_outlook_meeting_items: bool = False,
        pre_extracted_action_items: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-team-shared-links-1"
        assert transcript_text == "Agendar reunion semanal del equipo."
        assert participant_emails == ["external.shared@example.com"]
        captured_calls.append(
            {
                "notion": self.settings.notion_api_token,
                "google": self.settings.google_calendar_api_token,
                "outlook": self.settings.outlook_calendar_api_token,
                "team_timezone": self.settings.team_leader_timezone,
                "attendees": sorted(calendar_attendee_emails or []),
            },
        )
        item = {
            "title": "Reunion semanal del equipo",
            "status": "created",
            "online_meeting_platform": "google_meet",
            "due_date": "2026-04-02",
            "scheduled_start": "2026-04-02T10:00:00",
            "google_calendar_status": "skipped_missing_configuration",
            "google_calendar_created_count": 0,
            "google_calendar_event_id": None,
            "google_meet_link": None,
            "google_calendar_error": "GOOGLE_CALENDAR_API_TOKEN is missing.",
            "outlook_calendar_status": "skipped_missing_configuration",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_event_id": None,
            "outlook_teams_link": None,
            "outlook_calendar_error": "OUTLOOK_CALENDAR_API_TOKEN is missing.",
        }
        google_token = self.settings.google_calendar_api_token
        if google_token:
            item["google_calendar_status"] = "created"
            item["google_calendar_event_id"] = "google-event-lead"
            item["google_meet_link"] = "https://meet.google.com/shared-team-link"
            item["google_calendar_error"] = None
        outlook_token = self.settings.outlook_calendar_api_token
        if outlook_token:
            item["outlook_calendar_status"] = "created"
            item["outlook_calendar_event_id"] = "outlook-event-lead"
            item["outlook_teams_link"] = "https://teams.live.com/meet/shared-team-link"
            item["outlook_calendar_error"] = None
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "monday_status": "not_required_no_action_items",
            "monday_created_count": 0,
            "monday_error": None,
            "google_calendar_status": "completed" if google_token else "skipped_missing_configuration",
            "google_calendar_created_count": 1 if google_token else 0,
            "google_calendar_error": None if google_token else "GOOGLE_CALENDAR_API_TOKEN is missing.",
            "outlook_calendar_status": "completed" if outlook_token else "skipped_missing_configuration",
            "outlook_calendar_created_count": 1 if outlook_token else 0,
            "outlook_calendar_error": None if outlook_token else "OUTLOOK_CALENDAR_API_TOKEN is missing.",
            "items": [item],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-team-shared-links-1",
            "platform": "google_meet",
        },
        "participants": ["external.shared@example.com"],
        "transcript": {
            "id": "transcript-team-shared-links-1",
            "text": "Agendar reunion semanal del equipo.",
        },
    }
    response = client.post(
        f"/api/transcriptions/webhooks/fireflies/{lead_user_id}",
        json=payload,
    )

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 2
    assert len(captured_calls) == 2
    expected_attendees = sorted(
        [
            "external.shared@example.com",
            "lead.shared@example.com",
            "member.shared@example.com",
        ],
    )
    for call in captured_calls:
        assert call["attendees"] == expected_attendees
        assert call["team_timezone"] == "America/Mexico_City"
    assert any(
        call["google"] == "lead-google-token" and call["outlook"] == "lead-outlook-token"
        for call in captured_calls
    )
    assert any(
        call["google"] == "member-google-token" and call["outlook"] == "member-outlook-token"
        for call in captured_calls
    )

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-team-shared-links-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    action_items_sync = stored_record["action_items_sync"]
    assert action_items_sync["google_calendar_created_count"] == 1
    assert action_items_sync["outlook_calendar_created_count"] == 1
    items = action_items_sync["items"]
    lead_item = next(item for item in items if item["target_user_id"] == lead_user_id)
    member_item = next(item for item in items if item["target_user_id"] == member_user_id)
    assert lead_item["google_meet_link"] == "https://meet.google.com/shared-team-link"
    assert member_item["google_meet_link"] == "https://meet.google.com/shared-team-link"
    assert lead_item["outlook_teams_link"] == "https://teams.live.com/meet/shared-team-link"
    assert member_item["outlook_teams_link"] == "https://teams.live.com/meet/shared-team-link"
    assert member_item["google_calendar_status"] == "shared_from_team_event"
    assert member_item["outlook_calendar_status"] == "created"


def test_user_scoped_webhook_falls_back_to_independent_user_when_all_teams_are_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.disabled@example.com",
        full_name="Lead Disabled",
        password_hash="hashed",
        role="user",
    )
    member = user_store.create_user(
        email="member.disabled@example.com",
        full_name="Member Disabled",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])
    member_user_id = str(member["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-disabled-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )
    user_store.upsert_user_settings_values(
        member_user_id,
        {
            "NOTION_API_TOKEN": "member-disabled-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    team = team_store.create_team(
        name="Equipo Deshabilitado",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id, member_user_id],
    )
    team_id = str(team["_id"])
    team_store.upsert_membership(
        team_id=team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=team_id,
        user_id=member_user_id,
        role="member",
        status="accepted",
    )
    team_store.set_team_activation(team_id, False)

    calls = {"sync": 0}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
    ) -> dict[str, object]:
        calls["sync"] += 1
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "not_required_no_due_dates",
            "google_calendar_created_count": 0,
            "google_calendar_error": None,
            "outlook_calendar_status": "not_required_no_due_dates",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": None,
            "items": [],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-lead-disabled-team-1",
            "platform": "google_meet",
        },
        "participants": ["external.disabled@example.com"],
        "transcript": {
            "id": "transcript-lead-disabled-team-1",
            "text": "No debe crear notas por equipo deshabilitado.",
        },
    }
    response = client.post(
        f"/api/transcriptions/webhooks/fireflies/{lead_user_id}",
        json=payload,
    )

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 1
    assert calls["sync"] == 1

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-lead-disabled-team-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    action_items_sync = stored_record["action_items_sync"]
    assert action_items_sync["routed_via_team_memberships"] is False
    assert action_items_sync["matched_team_ids"] == []
    assert action_items_sync["target_users"] == []
    assert action_items_sync["created_count"] == 1


def test_user_scoped_webhook_uses_enabled_lead_team_when_participant_matches_disabled_team(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.mixed@example.com",
        full_name="Lead Mixed",
        password_hash="hashed",
        role="user",
    )
    disabled_member = user_store.create_user(
        email="member.disabled.team@example.com",
        full_name="Disabled Team Member",
        password_hash="hashed",
        role="user",
    )
    enabled_member = user_store.create_user(
        email="member.enabled.team@example.com",
        full_name="Enabled Team Member",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])
    disabled_member_user_id = str(disabled_member["_id"])
    enabled_member_user_id = str(enabled_member["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-mixed-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    disabled_team = team_store.create_team(
        name="Equipo Deshabilitado Mixed",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id, disabled_member_user_id],
    )
    disabled_team_id = str(disabled_team["_id"])
    team_store.upsert_membership(
        team_id=disabled_team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=disabled_team_id,
        user_id=disabled_member_user_id,
        role="member",
        status="accepted",
    )
    team_store.set_team_activation(disabled_team_id, False)

    enabled_team = team_store.create_team(
        name="Equipo Habilitado Mixed",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id],
    )
    enabled_team_id = str(enabled_team["_id"])
    team_store.upsert_membership(
        team_id=enabled_team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=enabled_team_id,
        user_id=enabled_member_user_id,
        role="member",
        status="accepted",
    )

    calls = {"sync": 0}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
    ) -> dict[str, object]:
        calls["sync"] += 1
        assert self.settings.notion_api_token == "lead-mixed-notion-token"
        assert meeting_id == "meeting-mixed-team-routing-1"
        assert transcript_text == "Debe usar el equipo habilitado del lider."
        assert participant_emails == ["member.disabled.team@example.com"]
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "not_required_no_due_dates",
            "google_calendar_created_count": 0,
            "google_calendar_error": None,
            "outlook_calendar_status": "not_required_no_due_dates",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": None,
            "items": [],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-mixed-team-routing-1",
            "platform": "google_meet",
        },
        "participants": ["member.disabled.team@example.com"],
        "transcript": {
            "id": "transcript-mixed-team-routing-1",
            "text": "Debe usar el equipo habilitado del lider.",
        },
    }
    response = client.post(
        f"/api/transcriptions/webhooks/fireflies/{lead_user_id}",
        json=payload,
    )

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 1
    assert calls["sync"] == 1

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-mixed-team-routing-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    action_items_sync = stored_record["action_items_sync"]
    assert action_items_sync["routed_via_team_memberships"] is True
    assert action_items_sync["matched_team_ids"] == [enabled_team_id]
    assert len(action_items_sync["target_users"]) == 1
    assert action_items_sync["target_users"][0]["user_id"] == lead_user_id


def test_user_scoped_webhook_does_not_duplicate_lead_when_leading_multiple_enabled_teams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.multi@example.com",
        full_name="Lead Multi Team",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-multi-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    team_one = team_store.create_team(
        name="Equipo Uno Multi",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id],
    )
    team_one_id = str(team_one["_id"])
    team_store.upsert_membership(
        team_id=team_one_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )

    team_two = team_store.create_team(
        name="Equipo Dos Multi",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id],
    )
    team_two_id = str(team_two["_id"])
    team_store.upsert_membership(
        team_id=team_two_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )

    calls = {"sync": 0}

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
    ) -> dict[str, object]:
        calls["sync"] += 1
        assert self.settings.notion_api_token == "lead-multi-notion-token"
        assert meeting_id == "meeting-multi-lead-no-dup-1"
        assert transcript_text == "Debe ejecutarse una sola vez para el lider."
        assert participant_emails == ["external.multi@example.com"]
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "not_required_no_due_dates",
            "google_calendar_created_count": 0,
            "google_calendar_error": None,
            "outlook_calendar_status": "not_required_no_due_dates",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": None,
            "items": [],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event": "transcript.completed",
        "meeting": {
            "id": "meeting-multi-lead-no-dup-1",
            "platform": "google_meet",
        },
        "participants": ["external.multi@example.com"],
        "transcript": {
            "id": "transcript-multi-lead-no-dup-1",
            "text": "Debe ejecutarse una sola vez para el lider.",
        },
    }
    response = client.post(
        f"/api/transcriptions/webhooks/fireflies/{lead_user_id}",
        json=payload,
    )

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 1
    assert calls["sync"] == 1

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-multi-lead-no-dup-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    action_items_sync = stored_record["action_items_sync"]
    assert action_items_sync["routed_via_team_memberships"] is True
    assert sorted(action_items_sync["matched_team_ids"]) == sorted([team_one_id, team_two_id])
    assert len(action_items_sync["target_users"]) == 1
    assert action_items_sync["target_users"][0]["user_id"] == lead_user_id
    assert action_items_sync["created_count"] == 1


def test_user_scoped_read_ai_webhook_routes_only_to_checked_recipients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_user_store_cache()
    clear_team_membership_store_cache()

    settings = get_settings()
    user_store = create_user_store(settings)
    team_store = create_team_membership_store(settings)

    lead = user_store.create_user(
        email="lead.readai@example.com",
        full_name="Lead Read AI",
        password_hash="hashed",
        role="user",
    )
    member = user_store.create_user(
        email="member.readai@example.com",
        full_name="Member Read AI",
        password_hash="hashed",
        role="user",
    )
    lead_user_id = str(lead["_id"])
    member_user_id = str(member["_id"])

    user_store.upsert_user_settings_values(
        lead_user_id,
        {
            "NOTION_API_TOKEN": "lead-readai-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )
    user_store.upsert_user_settings_values(
        member_user_id,
        {
            "NOTION_API_TOKEN": "member-readai-notion-token",
            "TRANSCRIPTION_AUTOSYNC_ENABLED": "true",
        },
    )

    team = team_store.create_team(
        name="Equipo Read AI",
        created_by_user_id=lead_user_id,
        recipient_user_ids=[lead_user_id],
    )
    team_id = str(team["_id"])
    team_store.upsert_membership(
        team_id=team_id,
        user_id=lead_user_id,
        role="lead",
        status="accepted",
    )
    team_store.upsert_membership(
        team_id=team_id,
        user_id=member_user_id,
        role="member",
        status="accepted",
    )

    captured_notion_tokens: list[str] = []

    def fake_action_sync(
        self: ActionItemSyncService,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-read-ai-team-routing-1"
        assert transcript_text == "Resumen Read AI para el equipo."
        assert participant_emails == ["external.readai@example.com"]
        captured_notion_tokens.append(self.settings.notion_api_token)
        return {
            "status": "completed",
            "extracted_count": 1,
            "created_count": 1,
            "google_calendar_status": "not_required_no_due_dates",
            "google_calendar_created_count": 0,
            "google_calendar_error": None,
            "outlook_calendar_status": "not_required_no_due_dates",
            "outlook_calendar_created_count": 0,
            "outlook_calendar_error": None,
            "items": [],
            "error": None,
        }

    monkeypatch.setattr(ActionItemSyncService, "sync", fake_action_sync)

    payload = {
        "event_type": "meeting.completed",
        "meeting": {"external_id": "meeting-read-ai-team-routing-1"},
        "participants": [{"email": "external.readai@example.com"}],
        "summary": {"transcript": "Resumen Read AI para el equipo."},
    }
    response = client.post(
        f"/api/transcriptions/webhooks/read-ai/{lead_user_id}",
        json=payload,
    )

    assert response.status_code == 202
    response_payload = response.json()
    assert response_payload["action_items_sync_status"] == "completed"
    assert response_payload["action_items_created_count"] == 1
    assert captured_notion_tokens == ["lead-readai-notion-token"]

    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-read-ai-team-routing-1")
    assert lookup_response.status_code == 200
    stored_record = lookup_response.json()
    action_items_sync = stored_record["action_items_sync"]
    assert action_items_sync["routed_via_team_memberships"] is True
    assert action_items_sync["matched_team_ids"] == [team_id]
    assert len(action_items_sync["target_users"]) == 1
    assert action_items_sync["created_count"] == 1


def test_fireflies_webhook_infers_second_participant_from_speakers_when_email_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    user_store = create_user_store(settings)
    user = user_store.create_user(
        email="adcamargo10@gmail.com",
        full_name="Angel Camargo",
        password_hash="hashed",
        role="user",
    )
    user_store.upsert_user_settings_values(
        str(user["_id"]),
        {"FIREFLIES_API_KEY": "fake-api-key"},
    )

    def fake_fetch(
        self: TranscriptionService,
        meeting_id: str,
        fireflies_client: object | None = None,
    ) -> dict[str, object]:
        assert meeting_id == "meeting-speaker-fallback-1"
        assert fireflies_client is not None
        return {
            "id": "meeting-speaker-fallback-1",
            "meeting_link": "https://meet.google.com/hbj-rkbt-hgp",
            "organizer_email": "adcamargo10@gmail.com",
            "participants": ["adcamargo10@gmail.com"],
            "fireflies_users": ["adcamargo10@gmail.com"],
            "meeting_attendees": [],
            "sentences": [
                {
                    "index": 0,
                    "speaker_name": "Angel Camargo",
                    "speaker_id": 0,
                    "text": "Primera frase",
                },
                {
                    "index": 1,
                    "speaker_name": "Cesar A Leon",
                    "speaker_id": 1,
                    "text": "Segunda frase",
                },
            ],
        }

    monkeypatch.setattr(TranscriptionService, "_fetch_fireflies_transcript", fake_fetch)

    payload = {
        "meetingId": "meeting-speaker-fallback-1",
        "eventType": "Transcription completed",
    }
    response = client.post(
        f"/api/transcriptions/webhooks/fireflies/{user['_id']}",
        json=payload,
    )

    assert response.status_code == 202
    lookup_response = client.get("/api/transcriptions/received/by-meeting/meeting-speaker-fallback-1")
    assert lookup_response.status_code == 200
    record = lookup_response.json()
    assert record["participant_emails"] == ["adcamargo10@gmail.com"]
    assert record["participants"] == [
        {
            "name": "Angel Camargo",
            "email": "adcamargo10@gmail.com",
            "external_id": "0",
            "role": "organizer",
        },
        {
            "name": "Cesar A Leon",
            "email": None,
            "external_id": "1",
            "role": "speaker",
        },
    ]

