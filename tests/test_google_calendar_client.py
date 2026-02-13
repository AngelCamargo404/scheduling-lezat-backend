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
