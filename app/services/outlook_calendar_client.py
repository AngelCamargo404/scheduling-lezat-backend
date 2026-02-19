import json
from datetime import UTC, date, datetime, time, timedelta
import re
from typing import Any
from urllib import error, parse, request
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.services.action_item_models import ActionItem

_OUTLOOK_GRAPH_SCOPES = (
    "offline_access "
    "https://graph.microsoft.com/User.Read "
    "https://graph.microsoft.com/Calendars.ReadWrite"
)


class OutlookCalendarError(Exception):
    pass


class OutlookCalendarClient:
    def __init__(
        self,
        *,
        access_token: str,
        refresh_token: str = "",
        client_id: str = "",
        client_secret: str = "",
        tenant_id: str = "common",
        timeout_seconds: float = 10.0,
        default_timezone: str = "UTC",
        api_base_url: str = "https://graph.microsoft.com/v1.0",
        oauth_token_url_template: str = (
            "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        ),
    ) -> None:
        self.access_token = self._normalize_access_token(access_token)
        self.refresh_token = refresh_token.strip().strip('"').strip("'")
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.tenant_id = tenant_id.strip() or "common"
        self.timeout_seconds = timeout_seconds
        self.default_timezone = default_timezone
        self.api_base_url = api_base_url.rstrip("/")
        self.oauth_token_url_template = oauth_token_url_template

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
    ) -> dict[str, str | bool | None]:
        event_timezone = self._resolve_event_timezone(item)
        start_dt, end_dt = self._resolve_event_time_window(item)
        start_dt = self._to_outlook_local_datetime(start_dt, event_timezone)
        end_dt = self._to_outlook_local_datetime(end_dt, event_timezone)
        payload: dict[str, Any] = {
            "subject": self._truncate(item.title, 255),
            "body": {
                "contentType": "text",
                "content": self._build_description(item=item, meeting_id=meeting_id),
            },
            "start": {"dateTime": start_dt.isoformat(), "timeZone": event_timezone},
            "end": {"dateTime": end_dt.isoformat(), "timeZone": event_timezone},
        }
        recurrence_payload = self._build_recurrence_payload(
            recurrence_rule=item.recurrence_rule,
            start_date=start_dt.date(),
        )
        if recurrence_payload:
            payload["recurrence"] = recurrence_payload
        normalized_attendees = self._normalize_attendee_emails(attendee_emails)
        if normalized_attendees:
            payload["attendees"] = [
                {
                    "emailAddress": {"address": email},
                    "type": "required",
                }
                for email in normalized_attendees
            ]

        if self._should_create_teams_meeting(item):
            payload["isOnlineMeeting"] = True

        response_payload = self._request_json("POST", "/me/events", payload=payload)
        event_id = response_payload.get("id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise OutlookCalendarError("Outlook Calendar create event response missing id.")
        teams_join_url = self._extract_teams_join_url(response_payload)
        is_online_meeting = bool(response_payload.get("isOnlineMeeting"))
        if self._should_create_teams_meeting(item) and not teams_join_url:
            teams_join_url = self.get_event_teams_join_url(event_id)
            is_online_meeting = is_online_meeting or bool(teams_join_url)
        return {
            "event_id": event_id,
            "teams_join_url": teams_join_url,
            "is_online_meeting": is_online_meeting,
        }

    def get_event_teams_join_url(self, event_id: str) -> str | None:
        cleaned_event_id = event_id.strip()
        if not cleaned_event_id:
            return None
        response_payload = self._request_json(
            "GET",
            f"/me/events/{parse.quote(cleaned_event_id, safe='')}",
        )
        return self._extract_teams_join_url(response_payload)

    def _resolve_event_time_window(self, item: ActionItem) -> tuple[datetime, datetime]:
        if item.scheduled_start:
            parsed_start = self._parse_datetime(item.scheduled_start)
            parsed_end = self._parse_datetime(item.scheduled_end) if item.scheduled_end else None
            if not parsed_end or parsed_end <= parsed_start:
                parsed_end = parsed_start + timedelta(minutes=60)
            return parsed_start, parsed_end

        if not item.due_date:
            raise OutlookCalendarError("Action item does not include due_date or scheduled_start.")

        due_date = self._parse_date(item.due_date)
        parsed_start = datetime.combine(due_date, time(hour=9, minute=0))
        return parsed_start, parsed_start + timedelta(hours=1)

    def _parse_datetime(self, raw_value: str) -> datetime:
        cleaned = raw_value.strip()
        normalized = cleaned.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise OutlookCalendarError("Action item scheduled datetime is not valid ISO format.") from exc

    def _resolve_event_timezone(self, item: ActionItem) -> str:
        if item.event_timezone and item.event_timezone.strip():
            return item.event_timezone.strip()
        return self.default_timezone

    def _to_outlook_local_datetime(self, value: datetime, timezone_name: str) -> datetime:
        if value.tzinfo is None:
            return value.replace(microsecond=0)

        target_timezone = self._to_zoneinfo(timezone_name)
        if target_timezone is None:
            # Preserve wall-clock time when target timezone cannot be mapped locally.
            return value.replace(tzinfo=None, microsecond=0)
        return value.astimezone(target_timezone).replace(tzinfo=None, microsecond=0)

    def _to_zoneinfo(self, timezone_name: str) -> Any:
        cleaned = timezone_name.strip()
        if not cleaned:
            return None
        if cleaned.upper() in {"UTC", "GMT"}:
            return UTC
        try:
            return ZoneInfo(cleaned)
        except ZoneInfoNotFoundError:
            return None

    def _build_recurrence_payload(
        self,
        *,
        recurrence_rule: str | None,
        start_date: date,
    ) -> dict[str, Any] | None:
        normalized_rule = self._normalize_rrule(recurrence_rule)
        if not normalized_rule:
            return None

        tokens = self._parse_rrule_tokens(normalized_rule)
        frequency = tokens.get("FREQ")
        interval = self._to_positive_int(tokens.get("INTERVAL")) or 1

        pattern: dict[str, Any] | None = None
        if frequency == "DAILY":
            pattern = {"type": "daily", "interval": interval}
        elif frequency == "WEEKLY":
            days = self._rrule_days_to_outlook(tokens.get("BYDAY"))
            if not days:
                days = [self._weekday_to_outlook_name(start_date.weekday())]
            pattern = {
                "type": "weekly",
                "interval": interval,
                "daysOfWeek": days,
                "firstDayOfWeek": "monday",
            }
        elif frequency == "MONTHLY":
            bymonthday = self._to_positive_int(tokens.get("BYMONTHDAY"))
            if bymonthday is None:
                bymonthday = start_date.day
            pattern = {
                "type": "absoluteMonthly",
                "interval": interval,
                "dayOfMonth": bymonthday,
            }
        elif frequency == "YEARLY":
            bymonthday = self._to_positive_int(tokens.get("BYMONTHDAY")) or start_date.day
            bymonth = self._to_positive_int(tokens.get("BYMONTH")) or start_date.month
            pattern = {
                "type": "absoluteYearly",
                "interval": interval,
                "dayOfMonth": bymonthday,
                "month": bymonth,
            }

        if not pattern:
            return None

        recurrence_range: dict[str, Any] = {
            "type": "noEnd",
            "startDate": start_date.isoformat(),
        }
        count = self._to_positive_int(tokens.get("COUNT"))
        if count:
            recurrence_range = {
                "type": "numbered",
                "startDate": start_date.isoformat(),
                "numberOfOccurrences": count,
            }
        else:
            until = tokens.get("UNTIL")
            until_date = self._parse_rrule_until(until) if until else None
            if until_date:
                recurrence_range = {
                    "type": "endDate",
                    "startDate": start_date.isoformat(),
                    "endDate": until_date.isoformat(),
                }

        return {"pattern": pattern, "range": recurrence_range}

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

    def _parse_rrule_tokens(self, recurrence_rule: str) -> dict[str, str]:
        tokens: dict[str, str] = {}
        for raw_part in recurrence_rule.split(";"):
            if "=" not in raw_part:
                continue
            key, value = raw_part.split("=", maxsplit=1)
            key_clean = key.strip().upper()
            value_clean = value.strip().upper()
            if not key_clean or not value_clean:
                continue
            tokens[key_clean] = value_clean
        return tokens

    def _rrule_days_to_outlook(self, raw_days: str | None) -> list[str]:
        if not raw_days:
            return []
        day_map = {
            "MO": "monday",
            "TU": "tuesday",
            "WE": "wednesday",
            "TH": "thursday",
            "FR": "friday",
            "SA": "saturday",
            "SU": "sunday",
        }
        normalized_days: list[str] = []
        for raw_day in raw_days.split(","):
            mapped = day_map.get(raw_day.strip().upper())
            if mapped and mapped not in normalized_days:
                normalized_days.append(mapped)
        return normalized_days

    def _weekday_to_outlook_name(self, weekday: int) -> str:
        weekday_map = {
            0: "monday",
            1: "tuesday",
            2: "wednesday",
            3: "thursday",
            4: "friday",
            5: "saturday",
            6: "sunday",
        }
        return weekday_map.get(weekday, "monday")

    def _to_positive_int(self, raw_value: str | None) -> int | None:
        if not raw_value:
            return None
        if raw_value.isdigit():
            parsed = int(raw_value)
            return parsed if parsed > 0 else None
        return None

    def _parse_rrule_until(self, raw_until: str) -> date | None:
        if not raw_until:
            return None
        cleaned = raw_until.strip().upper()
        if re.fullmatch(r"\d{8}", cleaned):
            try:
                return date(
                    year=int(cleaned[0:4]),
                    month=int(cleaned[4:6]),
                    day=int(cleaned[6:8]),
                )
            except ValueError:
                return None
        return None

    def _should_create_teams_meeting(self, item: ActionItem) -> bool:
        platform = (item.online_meeting_platform or "").strip().lower()
        return platform in {"microsoft_teams", "auto"}

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

    def _extract_teams_join_url(self, payload: dict[str, Any]) -> str | None:
        online_meeting = payload.get("onlineMeeting")
        if isinstance(online_meeting, dict):
            join_url = online_meeting.get("joinUrl")
            if isinstance(join_url, str) and join_url.strip():
                return join_url.strip()
        online_meeting_url = payload.get("onlineMeetingUrl")
        if isinstance(online_meeting_url, str) and online_meeting_url.strip():
            return online_meeting_url.strip()
        return None

    def _request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        allow_refresh_retry: bool = True,
    ) -> dict[str, Any]:
        if not self.access_token:
            if self._can_refresh_access_token():
                self._refresh_access_token()
            else:
                raise OutlookCalendarError(
                    "OUTLOOK_CALENDAR_API_TOKEN is missing. Reconnect Outlook Calendar using OAuth.",
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
            if exc.code == 401 and allow_refresh_retry and self._can_refresh_access_token():
                try:
                    self._refresh_access_token()
                except OutlookCalendarError:
                    pass
                else:
                    return self._request_json(
                        method,
                        path,
                        payload=payload,
                        allow_refresh_retry=False,
                    )
            if exc.code == 401 and "IDX14100" in body:
                raise OutlookCalendarError(
                    "Outlook Calendar OAuth token is not valid for Microsoft Graph. "
                    "Reconnect Outlook Calendar and verify Graph Calendars.ReadWrite permission.",
                ) from exc
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

    def _can_refresh_access_token(self) -> bool:
        return bool(
            self.refresh_token
            and self.client_id
            and self.client_secret
        )

    def _refresh_access_token(self) -> None:
        if not self._can_refresh_access_token():
            raise OutlookCalendarError(
                "Outlook OAuth refresh token flow is not configured.",
            )
        token_url = self.oauth_token_url_template.format(tenant_id=self.tenant_id)
        body = parse.urlencode(
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
                "scope": _OUTLOOK_GRAPH_SCOPES,
            },
        ).encode("utf-8")
        req = request.Request(
            token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_body = response.read()
        except TimeoutError as exc:
            raise OutlookCalendarError("Outlook OAuth refresh request timed out.") from exc
        except error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="ignore")
            raise OutlookCalendarError(
                f"Outlook OAuth refresh HTTP {exc.code}: {body_text or 'empty response body'}",
            ) from exc
        except error.URLError as exc:
            raise OutlookCalendarError(
                f"Outlook OAuth refresh connection error: {exc.reason}",
            ) from exc

        try:
            payload = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise OutlookCalendarError("Outlook OAuth refresh returned invalid JSON.") from exc
        if not isinstance(payload, dict):
            raise OutlookCalendarError("Outlook OAuth refresh response is not a JSON object.")
        new_access_token = payload.get("access_token")
        if not isinstance(new_access_token, str) or not new_access_token.strip():
            raise OutlookCalendarError("Outlook OAuth refresh did not include access_token.")
        self.access_token = self._normalize_access_token(new_access_token)
        refreshed_refresh_token = payload.get("refresh_token")
        if isinstance(refreshed_refresh_token, str) and refreshed_refresh_token.strip():
            self.refresh_token = refreshed_refresh_token.strip()

    def _normalize_access_token(self, raw_token: str) -> str:
        normalized = (raw_token or "").strip().strip('"').strip("'")
        if not normalized:
            return ""
        lowered = normalized.lower()
        if lowered.startswith("outlook_calendar_api_token") or lowered.startswith(
            "$env:outlook_calendar_api_token",
        ):
            separator_index = normalized.find("=")
            if separator_index < 0:
                separator_index = normalized.find(":")
            if separator_index >= 0:
                normalized = normalized[separator_index + 1 :].strip()
        if normalized.lower().startswith("bearer "):
            normalized = normalized.split(" ", maxsplit=1)[1].strip()
        return normalized.strip().strip('"').strip("'")

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
