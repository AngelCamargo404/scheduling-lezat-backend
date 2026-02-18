import json
from datetime import date, datetime, time, timedelta
from typing import Any
from urllib import error, request

from app.services.action_item_models import ActionItem


class OutlookCalendarError(Exception):
    pass


class OutlookCalendarClient:
    def __init__(
        self,
        *,
        access_token: str,
        timeout_seconds: float = 10.0,
        default_timezone: str = "UTC",
        api_base_url: str = "https://graph.microsoft.com/v1.0",
    ) -> None:
        normalized_token = access_token.strip()
        # Accept pasted values like "Bearer <token>" and keep only the raw token.
        if normalized_token.lower().startswith("bearer "):
            normalized_token = normalized_token.split(" ", maxsplit=1)[1].strip()
        self.access_token = normalized_token
        self.timeout_seconds = timeout_seconds
        self.default_timezone = default_timezone
        self.api_base_url = api_base_url.rstrip("/")

    def create_due_date_event(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
    ) -> str:
        if not item.due_date:
            raise OutlookCalendarError("Action item does not include due_date.")

        due_date = self._parse_date(item.due_date)
        start_dt = datetime.combine(due_date, time(hour=9, minute=0))
        end_dt = start_dt + timedelta(hours=1)
        payload: dict[str, Any] = {
            "subject": self._truncate(item.title, 255),
            "body": {
                "contentType": "text",
                "content": self._build_description(item=item, meeting_id=meeting_id),
            },
            "start": {"dateTime": start_dt.isoformat(), "timeZone": self.default_timezone},
            "end": {"dateTime": end_dt.isoformat(), "timeZone": self.default_timezone},
        }
        response_payload = self._request_json("POST", "/me/events", payload=payload)
        event_id = response_payload.get("id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise OutlookCalendarError("Outlook Calendar create event response missing id.")
        return event_id

    def _request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.access_token.count(".") < 2:
            raise OutlookCalendarError(
                "OUTLOOK_CALENDAR_API_TOKEN is invalid. Reconnect Outlook Calendar using OAuth.",
            )
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
            raise OutlookCalendarError("Outlook Calendar API request timed out.") from exc
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise OutlookCalendarError(
                f"Outlook Calendar API HTTP {exc.code}: {body or 'empty response body'}",
            ) from exc
        except error.URLError as exc:
            raise OutlookCalendarError(
                f"Outlook Calendar API connection error: {exc.reason}",
            ) from exc

        try:
            parsed_body = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise OutlookCalendarError("Outlook Calendar API returned invalid JSON.") from exc
        if not isinstance(parsed_body, dict):
            raise OutlookCalendarError("Outlook Calendar API response is not a JSON object.")
        return parsed_body

    def _parse_date(self, raw_date: str) -> date:
        try:
            return date.fromisoformat(raw_date)
        except ValueError as exc:
            raise OutlookCalendarError("Action item due_date is not a valid ISO date.") from exc

    def _build_description(self, *, item: ActionItem, meeting_id: str | None) -> str:
        lines: list[str] = []
        if item.details:
            lines.append(f"Detalles: {item.details}")
        if item.source_sentence:
            lines.append(f"Evidencia de reunion: {item.source_sentence}")
        if item.assignee_name or item.assignee_email:
            lines.append(f"Asignado a: {item.assignee_name or item.assignee_email}")
        if meeting_id:
            lines.append(f"Meeting ID: {meeting_id}")
        if not lines:
            return "Tarea detectada automaticamente desde transcripcion de reunion."
        return self._truncate("\n".join(lines), 4000)

    def _truncate(self, value: str, limit: int) -> str:
        if len(value) <= limit:
            return value
        return value[: limit - 3] + "..."
