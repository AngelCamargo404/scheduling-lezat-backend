import json
from datetime import date, datetime, timedelta
from typing import Any
from urllib import error, parse, request
from uuid import uuid4

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
        attendee_emails: list[str] | None = None,
    ) -> str:
        event_details = self.create_due_date_event_with_details(
            item=item,
            meeting_id=meeting_id,
            attendee_emails=attendee_emails,
        )
        return event_details["event_id"]

    def create_due_date_event_with_details(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
        attendee_emails: list[str] | None = None,
    ) -> dict[str, str | None]:
        start_payload, end_payload = self._resolve_event_time_window(item)
        summary = self._truncate(item.title, 500)
        description = self._build_description(item=item, meeting_id=meeting_id)

        payload: dict[str, Any] = {
            "summary": summary,
            "description": description,
            "start": start_payload,
            "end": end_payload,
        }
        recurrence_rule = self._normalize_rrule(item.recurrence_rule)
        if recurrence_rule:
            payload["recurrence"] = [f"RRULE:{recurrence_rule}"]

        normalized_attendees = self._normalize_attendee_emails(attendee_emails)
        if normalized_attendees:
            payload["attendees"] = [{"email": email} for email in normalized_attendees]

        endpoint_path = f"/calendars/{parse.quote(self.calendar_id, safe='')}/events"
        query_params: list[tuple[str, str]] = []
        if self._should_create_google_meet(item):
            payload["conferenceData"] = {
                "createRequest": {
                    "requestId": f"lezat-{uuid4().hex}",
                    "conferenceSolutionKey": {"type": "hangoutsMeet"},
                },
            }
            query_params.append(("conferenceDataVersion", "1"))
        if normalized_attendees:
            query_params.append(("sendUpdates", "all"))
        if query_params:
            endpoint_path = f"{endpoint_path}?{parse.urlencode(query_params)}"

        response_payload = self._request_json(
            "POST",
            endpoint_path,
            payload=payload,
        )
        event_id = response_payload.get("id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise GoogleCalendarError("Google Calendar create event response missing id.")
        return {
            "event_id": event_id,
            "google_meet_link": self._extract_google_meet_link(response_payload),
        }

    def get_event_google_meet_link(self, event_id: str) -> str | None:
        cleaned_event_id = event_id.strip()
        if not cleaned_event_id:
            return None
        endpoint_path = (
            f"/calendars/{parse.quote(self.calendar_id, safe='')}/events/"
            f"{parse.quote(cleaned_event_id, safe='')}"
        )
        response_payload = self._request_json("GET", endpoint_path)
        return self._extract_google_meet_link(response_payload)

    def _resolve_event_time_window(
        self,
        item: ActionItem,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        event_timezone = self._resolve_event_timezone(item)
        if item.scheduled_start:
            parsed_start = self._parse_datetime(item.scheduled_start)
            parsed_end = self._parse_datetime(item.scheduled_end) if item.scheduled_end else None
            if not parsed_end or parsed_end <= parsed_start:
                parsed_end = parsed_start + timedelta(minutes=60)
            return (
                self._to_datetime_payload(parsed_start, event_timezone),
                self._to_datetime_payload(parsed_end, event_timezone),
            )

        if not item.due_date:
            raise GoogleCalendarError("Action item does not include due_date or scheduled_start.")

        due_date = self._parse_date(item.due_date)
        end_date = due_date + timedelta(days=1)
        return (
            {"date": due_date.isoformat(), "timeZone": event_timezone},
            {"date": end_date.isoformat(), "timeZone": event_timezone},
        )

    def _to_datetime_payload(self, value: datetime, event_timezone: str) -> dict[str, Any]:
        payload: dict[str, Any] = {"dateTime": value.isoformat()}
        if value.tzinfo is None:
            payload["timeZone"] = event_timezone
        return payload

    def _resolve_event_timezone(self, item: ActionItem) -> str:
        if item.event_timezone and item.event_timezone.strip():
            return item.event_timezone.strip()
        return self.default_timezone

    def _parse_datetime(self, raw_value: str) -> datetime:
        cleaned = raw_value.strip()
        normalized = cleaned.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise GoogleCalendarError("Action item scheduled datetime is not valid ISO format.") from exc

    def _normalize_rrule(self, raw_rule: str | None) -> str | None:
        if not raw_rule:
            return None
        cleaned = raw_rule.strip()
        if not cleaned:
            return None
        if cleaned.upper().startswith("RRULE:"):
            cleaned = cleaned[len("RRULE:") :]
        normalized_parts: list[str] = []
        for raw_part in cleaned.split(";"):
            if "=" not in raw_part:
                continue
            key, value = raw_part.split("=", maxsplit=1)
            key_clean = key.strip().upper()
            value_clean = value.strip().upper()
            if not key_clean or not value_clean:
                continue
            normalized_parts.append(f"{key_clean}={value_clean}")
        if not normalized_parts:
            return None
        if not any(part.startswith("FREQ=") for part in normalized_parts):
            return None
        return ";".join(normalized_parts)

    def _should_create_google_meet(self, item: ActionItem) -> bool:
        platform = (item.online_meeting_platform or "").strip().lower()
        return platform in {"google_meet", "auto"}

    def _normalize_attendee_emails(self, attendee_emails: list[str] | None) -> list[str]:
        if not attendee_emails:
            return []
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_email in attendee_emails:
            cleaned = raw_email.strip().lower()
            if not cleaned or "@" not in cleaned:
                continue
            if cleaned in seen:
                continue
            seen.add(cleaned)
            normalized.append(cleaned)
        return normalized

    def _extract_google_meet_link(self, payload: dict[str, Any]) -> str | None:
        hangout_link = payload.get("hangoutLink")
        if isinstance(hangout_link, str) and hangout_link.strip():
            return hangout_link.strip()
        conference_data = payload.get("conferenceData")
        if not isinstance(conference_data, dict):
            return None
        entry_points = conference_data.get("entryPoints")
        if not isinstance(entry_points, list):
            return None
        for raw_entry in entry_points:
            if not isinstance(raw_entry, dict):
                continue
            uri = raw_entry.get("uri")
            if isinstance(uri, str) and uri.strip():
                return uri.strip()
        return None

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
        cleaned = raw_date.strip()
        try:
            return date.fromisoformat(cleaned)
        except ValueError as exc:
            # Accept datetime-like ISO strings (e.g. 2026-02-20T00:00:00Z)
            normalized = cleaned.replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(normalized).date()
            except ValueError:
                pass

            # Accept common explicit date formats as compatibility fallback.
            for fmt in ("%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
                try:
                    return datetime.strptime(cleaned, fmt).date()
                except ValueError:
                    continue

            raise GoogleCalendarError("Action item due_date is not a valid date format.") from exc

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
