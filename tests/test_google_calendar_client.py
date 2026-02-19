import io
import json
from urllib import error

import pytest

from app.services.action_item_models import ActionItem
from app.services.google_calendar_client import GoogleCalendarClient, GoogleCalendarError


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
        url="https://www.googleapis.com/calendar/v3/calendars/primary/events",
        code=status_code,
        msg="error",
        hdrs=None,
        fp=io.BytesIO(json.dumps(payload).encode("utf-8")),
    )


def test_google_calendar_client_refreshes_token_after_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        target = req.full_url
        if "oauth2.googleapis.com/token" in target:
            calls.append("refresh")
            return _MockResponse({"access_token": "new-access-token"})

        auth_header = req.headers.get("Authorization", "")
        if auth_header == "Bearer old-access-token":
            calls.append("calendar-old")
            raise _http_error(401, {"error": {"message": "Invalid Credentials"}})
        if auth_header == "Bearer new-access-token":
            calls.append("calendar-new")
            return _MockResponse({"id": "event-123"})
        raise AssertionError(f"Unexpected Authorization header: {auth_header}")

    monkeypatch.setattr("app.services.google_calendar_client.request.urlopen", fake_urlopen)

    client = GoogleCalendarClient(
        access_token="old-access-token",
        refresh_token="refresh-token",
        client_id="google-client-id",
        client_secret="google-client-secret",
    )
    event_id = client.create_due_date_event(
        item=ActionItem(title="Enviar propuesta", due_date="2026-02-13"),
        meeting_id="meeting-1",
    )

    assert event_id == "event-123"
    assert calls == ["calendar-old", "refresh", "calendar-new"]


def test_google_calendar_client_returns_error_when_401_and_no_refresh_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        raise _http_error(401, {"error": {"message": "Invalid Credentials"}})

    monkeypatch.setattr("app.services.google_calendar_client.request.urlopen", fake_urlopen)

    client = GoogleCalendarClient(access_token="expired-token")
    with pytest.raises(GoogleCalendarError, match="HTTP 401"):
        client.create_due_date_event(
            item=ActionItem(title="Enviar propuesta", due_date="2026-02-13"),
            meeting_id="meeting-2",
        )


def test_google_calendar_client_creates_meet_with_recurrence_and_time_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_url = {"value": ""}
    captured_payload: dict[str, object] = {}

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        captured_url["value"] = req.full_url
        body = req.data.decode("utf-8") if req.data else "{}"
        captured_payload.update(json.loads(body))
        return _MockResponse({"id": "event-456"})

    monkeypatch.setattr("app.services.google_calendar_client.request.urlopen", fake_urlopen)

    client = GoogleCalendarClient(access_token="token")
    event_id = client.create_due_date_event(
        item=ActionItem(
            title="Reunion de seguimiento",
            due_date="2026-02-19",
            scheduled_start="2026-02-19T11:00:00",
            scheduled_end="2026-02-19T12:00:00",
            recurrence_rule="FREQ=WEEKLY;INTERVAL=1;BYDAY=TH",
            online_meeting_platform="google_meet",
        ),
        meeting_id="meeting-3",
    )

    assert event_id == "event-456"
    assert "conferenceDataVersion=1" in captured_url["value"]
    assert captured_payload["recurrence"] == ["RRULE:FREQ=WEEKLY;INTERVAL=1;BYDAY=TH"]
    assert captured_payload["start"] == {
        "dateTime": "2026-02-19T11:00:00",
        "timeZone": "UTC",
    }
    assert captured_payload["end"] == {
        "dateTime": "2026-02-19T12:00:00",
        "timeZone": "UTC",
    }
    conference_data = captured_payload["conferenceData"]
    assert isinstance(conference_data, dict)
    create_request = conference_data["createRequest"]
    assert isinstance(create_request, dict)
    solution_key = create_request["conferenceSolutionKey"]
    assert isinstance(solution_key, dict)
    assert solution_key["type"] == "hangoutsMeet"


def test_google_calendar_client_returns_meet_link_in_event_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        return _MockResponse(
            {
                "id": "event-457",
                "hangoutLink": "https://meet.google.com/test-link",
            },
        )

    monkeypatch.setattr("app.services.google_calendar_client.request.urlopen", fake_urlopen)

    client = GoogleCalendarClient(access_token="token")
    details = client.create_due_date_event_with_details(
        item=ActionItem(
            title="Reunion de seguimiento",
            due_date="2026-03-21",
            scheduled_start="2026-03-21T10:00:00",
            online_meeting_platform="google_meet",
        ),
        meeting_id="meeting-4",
    )

    assert details["event_id"] == "event-457"
    assert details["google_meet_link"] == "https://meet.google.com/test-link"


def test_google_calendar_client_sends_attendees_and_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_url = {"value": ""}
    captured_payload: dict[str, object] = {}

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        captured_url["value"] = req.full_url
        body = req.data.decode("utf-8") if req.data else "{}"
        captured_payload.update(json.loads(body))
        return _MockResponse({"id": "event-458"})

    monkeypatch.setattr("app.services.google_calendar_client.request.urlopen", fake_urlopen)

    client = GoogleCalendarClient(access_token="token")
    event_id = client.create_due_date_event(
        item=ActionItem(
            title="Reunion de invitados",
            due_date="2026-03-22",
            scheduled_start="2026-03-22T09:00:00",
            online_meeting_platform="google_meet",
        ),
        meeting_id="meeting-5",
        attendee_emails=["a@example.com", "b@example.com"],
    )

    assert event_id == "event-458"
    assert "sendUpdates=all" in captured_url["value"]
    assert captured_payload["attendees"] == [
        {"email": "a@example.com"},
        {"email": "b@example.com"},
    ]


def test_google_calendar_client_uses_item_timezone_for_naive_scheduled_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        body = req.data.decode("utf-8") if req.data else "{}"
        captured_payload.update(json.loads(body))
        return _MockResponse({"id": "event-459"})

    monkeypatch.setattr("app.services.google_calendar_client.request.urlopen", fake_urlopen)

    client = GoogleCalendarClient(access_token="token")
    event_id = client.create_due_date_event(
        item=ActionItem(
            title="Reunion timezone",
            due_date="2026-11-12",
            scheduled_start="2026-11-12T09:00:00",
            scheduled_end="2026-11-12T10:00:00",
            event_timezone="America/Mexico_City",
        ),
        meeting_id="meeting-6",
    )

    assert event_id == "event-459"
    assert captured_payload["start"] == {
        "dateTime": "2026-11-12T09:00:00",
        "timeZone": "America/Mexico_City",
    }
    assert captured_payload["end"] == {
        "dateTime": "2026-11-12T10:00:00",
        "timeZone": "America/Mexico_City",
    }
