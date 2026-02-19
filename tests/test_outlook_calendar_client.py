import io
import json
from urllib import error

import pytest

from app.services.action_item_models import ActionItem
from app.services.outlook_calendar_client import OutlookCalendarClient, OutlookCalendarError


class _MockResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "_MockResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
        return False

    def read(self) -> bytes:
        return self._payload


def _http_error(status_code: int, payload: dict[str, object]) -> error.HTTPError:
    return error.HTTPError(
        url="https://graph.microsoft.com/v1.0/me/events",
        code=status_code,
        msg="error",
        hdrs=None,
        fp=io.BytesIO(json.dumps(payload).encode("utf-8")),
    )


def test_outlook_calendar_client_refreshes_token_after_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        target = req.full_url
        if "login.microsoftonline.com/common/oauth2/v2.0/token" in target:
            calls.append("refresh")
            return _MockResponse(
                {
                    "access_token": "new-access-token",
                    "refresh_token": "new-refresh-token",
                },
            )

        auth_header = req.headers.get("Authorization", "")
        if auth_header == "Bearer old-access-token":
            calls.append("calendar-old")
            raise _http_error(
                401,
                {"error": {"message": "IDX14100: JWT is not well formed, there are no dots (.)"}},
            )
        if auth_header == "Bearer new-access-token":
            calls.append("calendar-new")
            return _MockResponse({"id": "event-123"})
        raise AssertionError(f"Unexpected Authorization header: {auth_header}")

    monkeypatch.setattr("app.services.outlook_calendar_client.request.urlopen", fake_urlopen)

    client = OutlookCalendarClient(
        access_token="old-access-token",
        refresh_token="refresh-token",
        client_id="outlook-client-id",
        client_secret="outlook-client-secret",
        tenant_id="common",
    )
    event_id = client.create_due_date_event(
        item=ActionItem(title="Enviar propuesta", due_date="2026-02-13"),
        meeting_id="meeting-1",
    )

    assert event_id == "event-123"
    assert calls == ["calendar-old", "refresh", "calendar-new"]
    assert client.access_token == "new-access-token"
    assert client.refresh_token == "new-refresh-token"


def test_outlook_calendar_client_normalizes_pasted_env_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_headers: list[str] = []

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        observed_headers.append(req.headers.get("Authorization", ""))
        return _MockResponse({"id": "event-123"})

    monkeypatch.setattr("app.services.outlook_calendar_client.request.urlopen", fake_urlopen)

    client = OutlookCalendarClient(
        access_token="OUTLOOK_CALENDAR_API_TOKEN: Bearer pasted-token",
    )
    event_id = client.create_due_date_event(
        item=ActionItem(title="Enviar propuesta", due_date="2026-02-13"),
        meeting_id="meeting-2",
    )

    assert event_id == "event-123"
    assert observed_headers == ["Bearer pasted-token"]


def test_outlook_calendar_client_returns_clear_error_for_invalid_graph_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        raise _http_error(
            401,
            {"error": {"message": "IDX14100: JWT is not well formed, there are no dots (.)"}},
        )

    monkeypatch.setattr("app.services.outlook_calendar_client.request.urlopen", fake_urlopen)

    client = OutlookCalendarClient(access_token="opaque-or-invalid-token")
    with pytest.raises(OutlookCalendarError, match="not valid for Microsoft Graph"):
        client.create_due_date_event(
            item=ActionItem(title="Enviar propuesta", due_date="2026-02-13"),
            meeting_id="meeting-3",
        )


def test_outlook_calendar_client_creates_teams_with_recurrence_and_time_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        body = req.data.decode("utf-8") if req.data else "{}"
        captured_payload.update(json.loads(body))
        return _MockResponse({"id": "event-789"})

    monkeypatch.setattr("app.services.outlook_calendar_client.request.urlopen", fake_urlopen)

    client = OutlookCalendarClient(access_token="token")
    event_id = client.create_due_date_event(
        item=ActionItem(
            title="Reunion de revision",
            due_date="2026-02-19",
            scheduled_start="2026-02-19T11:00:00",
            scheduled_end="2026-02-19T12:00:00",
            recurrence_rule="FREQ=WEEKLY;INTERVAL=1;BYDAY=TH",
            online_meeting_platform="microsoft_teams",
        ),
        meeting_id="meeting-4",
    )

    assert event_id == "event-789"
    assert captured_payload["isOnlineMeeting"] is True
    assert "onlineMeetingProvider" not in captured_payload
    recurrence = captured_payload["recurrence"]
    assert isinstance(recurrence, dict)
    pattern = recurrence["pattern"]
    assert isinstance(pattern, dict)
    assert pattern["type"] == "weekly"
    assert pattern["interval"] == 1
    assert pattern["daysOfWeek"] == ["thursday"]
    event_range = recurrence["range"]
    assert isinstance(event_range, dict)
    assert event_range["type"] == "noEnd"


def test_outlook_calendar_client_returns_teams_link_in_event_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        return _MockResponse(
            {
                "id": "event-790",
                "isOnlineMeeting": True,
                "onlineMeeting": {"joinUrl": "https://teams.live.com/meet/test"},
            },
        )

    monkeypatch.setattr("app.services.outlook_calendar_client.request.urlopen", fake_urlopen)

    client = OutlookCalendarClient(access_token="token")
    details = client.create_due_date_event_with_details(
        item=ActionItem(
            title="Reunion de seguimiento",
            due_date="2026-03-20",
            scheduled_start="2026-03-20T09:00:00",
            online_meeting_platform="auto",
        ),
        meeting_id="meeting-5",
    )

    assert details["event_id"] == "event-790"
    assert details["is_online_meeting"] is True
    assert details["teams_join_url"] == "https://teams.live.com/meet/test"


def test_outlook_calendar_client_sends_attendees(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        body = req.data.decode("utf-8") if req.data else "{}"
        captured_payload.update(json.loads(body))
        return _MockResponse({"id": "event-791"})

    monkeypatch.setattr("app.services.outlook_calendar_client.request.urlopen", fake_urlopen)

    client = OutlookCalendarClient(access_token="token")
    event_id = client.create_due_date_event(
        item=ActionItem(
            title="Reunion de invitados",
            due_date="2026-03-23",
            scheduled_start="2026-03-23T10:00:00",
            online_meeting_platform="auto",
        ),
        meeting_id="meeting-6",
        attendee_emails=["a@example.com", "b@example.com"],
    )

    assert event_id == "event-791"
    attendees = captured_payload["attendees"]
    assert isinstance(attendees, list)
    assert attendees == [
        {"emailAddress": {"address": "a@example.com"}, "type": "required"},
        {"emailAddress": {"address": "b@example.com"}, "type": "required"},
    ]


def test_outlook_calendar_client_uses_item_timezone_for_naive_scheduled_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        body = req.data.decode("utf-8") if req.data else "{}"
        captured_payload.update(json.loads(body))
        return _MockResponse({"id": "event-792"})

    monkeypatch.setattr("app.services.outlook_calendar_client.request.urlopen", fake_urlopen)

    client = OutlookCalendarClient(access_token="token")
    event_id = client.create_due_date_event(
        item=ActionItem(
            title="Reunion timezone",
            due_date="2026-11-12",
            scheduled_start="2026-11-12T09:00:00",
            scheduled_end="2026-11-12T10:00:00",
            event_timezone="America/Mexico_City",
        ),
        meeting_id="meeting-7",
    )

    assert event_id == "event-792"
    assert captured_payload["start"] == {
        "dateTime": "2026-11-12T09:00:00",
        "timeZone": "America/Mexico_City",
    }
    assert captured_payload["end"] == {
        "dateTime": "2026-11-12T10:00:00",
        "timeZone": "America/Mexico_City",
    }
