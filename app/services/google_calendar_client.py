import json
from datetime import date, timedelta
from typing import Any
from urllib import error, parse, request

from app.services.action_item_models import ActionItem


class GoogleCalendarError(Exception):
    pass


class GoogleCalendarClient:
    def __init__(
        self,
        *,
        access_token: str,
        refresh_token: str = "",
        client_id: str = "",
        client_secret: str = "",
        calendar_id: str = "primary",
        timeout_seconds: float = 10.0,
        default_timezone: str = "UTC",
        api_base_url: str = "https://www.googleapis.com/calendar/v3",
        oauth_token_url: str = "https://oauth2.googleapis.com/token",
    ) -> None:
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.calendar_id = calendar_id
        self.timeout_seconds = timeout_seconds
        self.default_timezone = default_timezone
        self.api_base_url = api_base_url.rstrip("/")
        self.oauth_token_url = oauth_token_url

    def create_due_date_event(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
    ) -> str:
        if not item.due_date:
            raise GoogleCalendarError("Action item does not include due_date.")

        due_date = self._parse_date(item.due_date)
        end_date = due_date + timedelta(days=1)
        summary = self._truncate(item.title, 500)
        description = self._build_description(item=item, meeting_id=meeting_id)

        payload: dict[str, Any] = {
            "summary": summary,
            "description": description,
            "start": {"date": due_date.isoformat(), "timeZone": self.default_timezone},
            "end": {"date": end_date.isoformat(), "timeZone": self.default_timezone},
        }
        response_payload = self._request_json(
            "POST",
            f"/calendars/{parse.quote(self.calendar_id, safe='')}/events",
            payload=payload,
        )
        event_id = response_payload.get("id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise GoogleCalendarError("Google Calendar create event response missing id.")
        return event_id

    def _request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.access_token and self._can_refresh_access_token():
            self._refresh_access_token()

        target = f"{self.api_base_url}{path}"
        raw_payload: bytes | None = None
        if payload is not None:
            raw_payload = json.dumps(payload).encode("utf-8")

        req = request.Request(
            target,
            data=raw_payload,
            method=method,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            },
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_body = response.read()
        except TimeoutError as exc:
            raise GoogleCalendarError("Google Calendar API request timed out.") from exc
        except error.HTTPError as exc:
            if exc.code == 401 and self._can_refresh_access_token():
                try:
                    self._refresh_access_token()
                except GoogleCalendarError:
                    pass
                else:
                    return self._request_json(method, path, payload)
            body = exc.read().decode("utf-8", errors="ignore")
            raise GoogleCalendarError(
                f"Google Calendar API HTTP {exc.code}: {body or 'empty response body'}",
            ) from exc
        except error.URLError as exc:
            raise GoogleCalendarError(
                f"Google Calendar API connection error: {exc.reason}",
            ) from exc

        try:
            parsed_body = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise GoogleCalendarError("Google Calendar API returned invalid JSON.") from exc

        if not isinstance(parsed_body, dict):
            raise GoogleCalendarError("Google Calendar API response is not a JSON object.")
        return parsed_body

    def _can_refresh_access_token(self) -> bool:
        return bool(
            self.refresh_token.strip()
            and self.client_id.strip()
            and self.client_secret.strip()
        )

    def _refresh_access_token(self) -> None:
        if not self._can_refresh_access_token():
            raise GoogleCalendarError(
                "Google Calendar refresh token flow is not configured.",
            )
        body = parse.urlencode(
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
        ).encode("utf-8")
        req = request.Request(
            self.oauth_token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_body = response.read()
        except TimeoutError as exc:
            raise GoogleCalendarError("Google OAuth refresh request timed out.") from exc
        except error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="ignore")
            raise GoogleCalendarError(
                f"Google OAuth refresh HTTP {exc.code}: {body_text or 'empty response body'}",
            ) from exc
        except error.URLError as exc:
            raise GoogleCalendarError(
                f"Google OAuth refresh connection error: {exc.reason}",
            ) from exc

        try:
            payload = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise GoogleCalendarError("Google OAuth refresh returned invalid JSON.") from exc

        if not isinstance(payload, dict):
            raise GoogleCalendarError("Google OAuth refresh response is not a JSON object.")
        new_access_token = payload.get("access_token")
        if not isinstance(new_access_token, str) or not new_access_token.strip():
            raise GoogleCalendarError("Google OAuth refresh did not include access_token.")
        self.access_token = new_access_token.strip()
        refreshed_refresh_token = payload.get("refresh_token")
        if isinstance(refreshed_refresh_token, str) and refreshed_refresh_token.strip():
            self.refresh_token = refreshed_refresh_token.strip()

    def _parse_date(self, raw_date: str) -> date:
        try:
            return date.fromisoformat(raw_date)
        except ValueError as exc:
            raise GoogleCalendarError("Action item due_date is not a valid ISO date.") from exc

    def _build_description(self, *, item: ActionItem, meeting_id: str | None) -> str:
        lines: list[str] = []
        if item.details:
            lines.append(f"Detalles: {item.details}")
        if item.source_sentence:
            lines.append(f"Evidencia de reunion: {item.source_sentence}")
        if item.assignee_name or item.assignee_email:
            lines.append(
                f"Asignado a: {item.assignee_name or item.assignee_email}",
            )
        if meeting_id:
            lines.append(f"Meeting ID: {meeting_id}")
        if not lines:
            return "Tarea detectada automaticamente desde transcripcion de reunion."
        return self._truncate("\n".join(lines), 8000)

    def _truncate(self, value: str, limit: int) -> str:
        if len(value) <= limit:
            return value
        return value[: limit - 3] + "..."
