from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
import re
from typing import Any
import unicodedata
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_DEFAULT_EVENT_DURATION_MINUTES = 60
_TIMEZONE_ALIAS_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\b(?:utc|gmt)\b", "UTC"),
    (
        r"\b(?:est|edt|eastern(?:\s+time)?|hora\s+del\s+este|horario\s+del\s+este)\b",
        "America/New_York",
    ),
    (
        r"\b(?:cst|cdt|central(?:\s+time)?|hora\s+central|horario\s+central)\b",
        "America/Chicago",
    ),
    (
        r"\b(?:mst|mdt|mountain(?:\s+time)?|hora\s+de\s+la\s+montana)\b",
        "America/Denver",
    ),
    (
        r"\b(?:pst|pdt|pacific(?:\s+time)?|hora\s+del\s+pacifico|horario\s+del\s+pacifico)\b",
        "America/Los_Angeles",
    ),
    (r"\b(?:mexico|cdmx|ciudad\s+de\s+mexico)\b", "America/Mexico_City"),
    (r"\b(?:colombia|bogota)\b", "America/Bogota"),
    (r"\b(?:peru|lima)\b", "America/Lima"),
    (r"\b(?:argentina|buenos\s+aires)\b", "America/Argentina/Buenos_Aires"),
    (r"\b(?:chile|santiago)\b", "America/Santiago"),
    (r"\b(?:espana|madrid|cet|cest)\b", "Europe/Madrid"),
    (r"\b(?:london|united\s+kingdom|uk|bst)\b", "Europe/London"),
)


@dataclass
class ActionItem:
    title: str
    assignee_email: str | None = None
    assignee_name: str | None = None
    due_date: str | None = None
    details: str | None = None
    source_sentence: str | None = None
    scheduled_start: str | None = None
    scheduled_end: str | None = None
    event_timezone: str | None = None
    recurrence_rule: str | None = None
    online_meeting_platform: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "assignee_email": self.assignee_email,
            "assignee_name": self.assignee_name,
            "due_date": self.due_date,
            "details": self.details,
            "source_sentence": self.source_sentence,
            "scheduled_start": self.scheduled_start,
            "scheduled_end": self.scheduled_end,
            "event_timezone": self.event_timezone,
            "recurrence_rule": self.recurrence_rule,
            "online_meeting_platform": self.online_meeting_platform,
        }

    def has_calendar_schedule(self) -> bool:
        return bool(self.due_date or self.scheduled_start)

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, Any],
        *,
        reference_date: date | None = None,
    ) -> ActionItem | None:
        raw_title = payload.get("title")
        if not isinstance(raw_title, str):
            return None

        title = raw_title.strip()
        if not title:
            return None

        base_reference_date = reference_date or datetime.now(UTC).date()
        assignee_email = _normalize_email(payload.get("assignee_email"))
        assignee_name = _normalize_optional_text(payload.get("assignee_name"))
        due_date = _normalize_due_date(
            payload.get("due_date"),
            reference_date=base_reference_date,
        )
        if not due_date:
            due_date = _infer_due_date_from_context(
                payload,
                reference_date=base_reference_date,
            )

        recurrence_rule = _normalize_recurrence_rule(payload.get("recurrence_rule"))
        if not recurrence_rule:
            recurrence_rule = _normalize_recurrence_rule(payload.get("rrule"))
        if not recurrence_rule:
            recurrence_rule = _normalize_recurrence_rule(payload.get("recurrence"))
        if not recurrence_rule:
            recurrence_rule = _infer_recurrence_rule_from_context(
                payload,
                due_date=due_date,
            )

        if not due_date and recurrence_rule:
            inferred_due_date = _infer_due_date_from_recurrence_rule(
                recurrence_rule=recurrence_rule,
                reference_date=base_reference_date,
            )
            if inferred_due_date:
                due_date = inferred_due_date.isoformat()

        online_meeting_platform = _normalize_online_meeting_platform(
            payload.get("online_meeting_platform"),
        )
        for platform_key in (
            "video_meeting_platform",
            "video_platform",
            "meeting_platform",
            "conference_platform",
        ):
            if online_meeting_platform:
                break
            online_meeting_platform = _normalize_online_meeting_platform(
                payload.get(platform_key),
            )
        requires_online_meeting = payload.get("requires_online_meeting")
        requires_online_meeting_text = _normalize_optional_text(requires_online_meeting)
        if not online_meeting_platform and (
            requires_online_meeting is True
            or (
                requires_online_meeting_text
                and requires_online_meeting_text.lower() in {"true", "1", "si", "yes", "on"}
            )
        ):
            online_meeting_platform = "auto"
        if not online_meeting_platform:
            online_meeting_platform = _infer_online_meeting_platform_from_context(payload)
        if not online_meeting_platform and _looks_like_scheduled_meeting_request(
            title=title,
            details=_normalize_optional_text(payload.get("details")),
            source_sentence=_normalize_optional_text(payload.get("source_sentence")),
        ):
            online_meeting_platform = "auto"
        event_timezone = _resolve_event_timezone(payload)

        scheduled_start = _normalize_scheduled_start(
            payload=payload,
            due_date=due_date,
            reference_date=base_reference_date,
        )
        if not due_date and scheduled_start:
            inferred_scheduled_date = _date_from_datetime_text(scheduled_start)
            if inferred_scheduled_date:
                due_date = inferred_scheduled_date.isoformat()

        if due_date and not scheduled_start:
            parsed_time = _infer_time_from_context(payload)
            if parsed_time:
                due_date_obj = date.fromisoformat(due_date)
                scheduled_start = datetime.combine(due_date_obj, parsed_time).isoformat()
            elif recurrence_rule or online_meeting_platform:
                due_date_obj = date.fromisoformat(due_date)
                scheduled_start = datetime.combine(
                    due_date_obj,
                    time(hour=9, minute=0),
                ).isoformat()

        scheduled_end = _normalize_scheduled_end(
            payload=payload,
            scheduled_start=scheduled_start,
        )
        details = _normalize_optional_text(payload.get("details"))
        source_sentence = _normalize_optional_text(payload.get("source_sentence"))
        if not _looks_like_action_item(
            title=title,
            details=details,
            source_sentence=source_sentence,
            assignee_email=assignee_email,
            assignee_name=assignee_name,
            due_date=due_date,
            scheduled_start=scheduled_start,
        ):
            return None

        return cls(
            title=title,
            assignee_email=assignee_email,
            assignee_name=assignee_name,
            due_date=due_date,
            details=details,
            source_sentence=source_sentence,
            scheduled_start=scheduled_start,
            scheduled_end=scheduled_end,
            event_timezone=event_timezone,
            recurrence_rule=recurrence_rule,
            online_meeting_platform=online_meeting_platform,
        )


def _normalize_optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_email(value: Any) -> str | None:
    text = _normalize_optional_text(value)
    if not text:
        return None
    lowered = text.lower()
    if "@" not in lowered:
        return None
    return lowered


def _normalize_due_date(value: Any, *, reference_date: date | None = None) -> str | None:
    text = _normalize_optional_text(value)
    if not text:
        return None

    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        parsed_relative = _parse_due_date_from_text(
            text=text,
            reference_date=reference_date or datetime.now(UTC).date(),
        )
        return parsed_relative.isoformat() if parsed_relative else None


def _infer_due_date_from_context(
    payload: dict[str, Any],
    *,
    reference_date: date | None = None,
) -> str | None:
    base_date = reference_date or datetime.now(UTC).date()
    for key in ("source_sentence", "details", "title"):
        raw_value = payload.get(key)
        text = _normalize_optional_text(raw_value)
        if not text:
            continue
        parsed = _parse_due_date_from_text(text=text, reference_date=base_date)
        if parsed:
            return parsed.isoformat()
    return None


def _normalize_scheduled_start(
    *,
    payload: dict[str, Any],
    due_date: str | None,
    reference_date: date,
) -> str | None:
    for key in (
        "scheduled_start",
        "start_datetime",
        "start_at",
        "event_start",
        "meeting_start",
        "due_datetime",
        "calendar_start",
        "starts_at",
    ):
        parsed = _normalize_datetime_value(payload.get(key), reference_date=reference_date)
        if parsed:
            return parsed

    if due_date:
        parsed_time = _extract_time_from_payload(payload)
        if parsed_time:
            return datetime.combine(date.fromisoformat(due_date), parsed_time).isoformat()

    for key in ("source_sentence", "details", "title"):
        text = _normalize_optional_text(payload.get(key))
        if not text:
            continue
        parsed_due_date = _parse_due_date_from_text(text=text, reference_date=reference_date)
        parsed_time = _parse_time_from_text(text)
        if parsed_due_date and parsed_time:
            return datetime.combine(parsed_due_date, parsed_time).isoformat()

    return None


def _normalize_scheduled_end(
    *,
    payload: dict[str, Any],
    scheduled_start: str | None,
) -> str | None:
    reference_date = _date_from_datetime_text(scheduled_start) if scheduled_start else None
    for key in (
        "scheduled_end",
        "end_datetime",
        "end_at",
        "event_end",
        "meeting_end",
        "calendar_end",
        "ends_at",
    ):
        parsed = _normalize_datetime_value(payload.get(key), reference_date=reference_date)
        if parsed:
            return parsed

    if not scheduled_start:
        return None

    duration_minutes = _extract_duration_minutes(payload)
    parsed_start = _parse_datetime_value(scheduled_start)
    if not parsed_start:
        return None
    return (
        parsed_start + timedelta(minutes=duration_minutes or _DEFAULT_EVENT_DURATION_MINUTES)
    ).isoformat()


def _normalize_datetime_value(value: Any, *, reference_date: date | None = None) -> str | None:
    parsed = _parse_datetime_value(value, reference_date=reference_date)
    if not parsed:
        return None
    return parsed.isoformat()


def _parse_datetime_value(value: Any, *, reference_date: date | None = None) -> datetime | None:
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        normalized = cleaned.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%d/%m/%Y %H:%M",
            "%d-%m-%Y %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
        ):
            try:
                parsed = datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
            if fmt in {"%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"}:
                return datetime.combine(parsed.date(), time(hour=9, minute=0))
            return parsed

        parsed_due_date = _parse_due_date_from_text(
            text=cleaned,
            reference_date=reference_date or datetime.now(UTC).date(),
        )
        parsed_time = _parse_time_from_text(cleaned)
        if parsed_due_date and parsed_time:
            return datetime.combine(parsed_due_date, parsed_time)
        return None

    return None


def _date_from_datetime_text(value: str | None) -> date | None:
    if not value:
        return None
    parsed = _parse_datetime_value(value)
    if not parsed:
        return None
    return parsed.date()


def _extract_duration_minutes(payload: dict[str, Any]) -> int | None:
    for key in ("duration_minutes", "meeting_duration_minutes", "duration"):
        raw_value = payload.get(key)
        if isinstance(raw_value, int) and 0 < raw_value <= 1440:
            return raw_value
        if isinstance(raw_value, float) and raw_value.is_integer():
            normalized = int(raw_value)
            if 0 < normalized <= 1440:
                return normalized
        text = _normalize_optional_text(raw_value)
        if not text:
            continue
        normalized = _normalize_for_matching(text)
        if normalized.isdigit():
            parsed = int(normalized)
            if 0 < parsed <= 1440:
                return parsed
        hours_match = re.search(r"\b(\d+)\s*(?:h|hora|horas)\b", normalized)
        minutes_match = re.search(r"\b(\d+)\s*(?:m|min|minuto|minutos)\b", normalized)
        if hours_match or minutes_match:
            hours = int(hours_match.group(1)) if hours_match else 0
            minutes = int(minutes_match.group(1)) if minutes_match else 0
            total = hours * 60 + minutes
            if 0 < total <= 1440:
                return total
    return None


def _extract_time_from_payload(payload: dict[str, Any]) -> time | None:
    for key in ("due_time", "time", "start_time", "meeting_time", "hora", "hour"):
        raw_value = payload.get(key)
        if isinstance(raw_value, int | float):
            normalized = int(raw_value)
            if 0 <= normalized <= 23:
                return time(hour=normalized, minute=0)
            continue
        text = _normalize_optional_text(raw_value)
        if not text:
            continue
        parsed = _parse_time_from_text(text)
        if parsed:
            return parsed
    return None


def _infer_time_from_context(payload: dict[str, Any]) -> time | None:
    for key in ("source_sentence", "details", "title"):
        text = _normalize_optional_text(payload.get(key))
        if not text:
            continue
        parsed = _parse_time_from_text(text)
        if parsed:
            return parsed
    return None


def _parse_time_from_text(text: str) -> time | None:
    normalized = _normalize_for_matching(text)
    if re.search(r"\bmediodia\b", normalized):
        return time(hour=12, minute=0)
    if re.search(r"\bmedianoche\b", normalized):
        return time(hour=0, minute=0)

    period_match = re.search(
        r"\b(?:a las|a la|para las|para la|desde las|desde la|at)?\s*"
        r"([a-z0-9]+)(?::([0-5]\d))?\s*(?:de la|del)?\s*"
        r"(manana|tarde|noche|morning|afternoon|evening)\b",
        normalized,
    )
    if period_match:
        return _build_time_from_components(
            hour_token=period_match.group(1),
            minute_token=period_match.group(2),
            period_token=period_match.group(3),
        )

    am_pm_match = re.search(
        r"\b(?:a las|a la|para las|para la|desde las|desde la|at)?\s*"
        r"([a-z0-9]+)(?::([0-5]\d))?\s*(am|pm)\b",
        normalized,
    )
    if am_pm_match:
        return _build_time_from_components(
            hour_token=am_pm_match.group(1),
            minute_token=am_pm_match.group(2),
            meridiem_token=am_pm_match.group(3),
        )

    explicit_time_match = re.search(
        r"\b(?:a las|a la|para las|para la|desde las|desde la|at)?\s*"
        r"([01]?\d|2[0-3]):([0-5]\d)\b",
        normalized,
    )
    if explicit_time_match:
        return time(
            hour=int(explicit_time_match.group(1)),
            minute=int(explicit_time_match.group(2)),
        )

    plain_hour_match = re.search(
        r"\b(?:a las|a la|para las|para la|desde las|desde la|at)\s+([a-z0-9]+)\b",
        normalized,
    )
    if plain_hour_match:
        return _build_time_from_components(hour_token=plain_hour_match.group(1))

    return None


def _build_time_from_components(
    *,
    hour_token: str,
    minute_token: str | None = None,
    meridiem_token: str | None = None,
    period_token: str | None = None,
) -> time | None:
    parsed_hour = _parse_hour_token(hour_token)
    if parsed_hour is None:
        return None
    parsed_minute = int(minute_token) if minute_token else 0
    if not 0 <= parsed_minute <= 59:
        return None

    if meridiem_token:
        meridiem = meridiem_token.lower()
        if parsed_hour > 12:
            return None
        if meridiem == "am":
            parsed_hour = 0 if parsed_hour == 12 else parsed_hour
        elif meridiem == "pm" and parsed_hour != 12:
            parsed_hour += 12
    elif period_token:
        period = period_token.lower()
        if period in {"tarde", "noche", "afternoon", "evening"} and 1 <= parsed_hour <= 11:
            parsed_hour += 12
        if period in {"manana", "morning"} and parsed_hour == 12:
            parsed_hour = 0

    if not 0 <= parsed_hour <= 23:
        return None
    return time(hour=parsed_hour, minute=parsed_minute)


def _parse_hour_token(token: str) -> int | None:
    cleaned = token.strip().lower()
    if cleaned.isdigit():
        return int(cleaned)

    parsed_spanish = _parse_spanish_integer(cleaned)
    if parsed_spanish is not None:
        return parsed_spanish

    english_numbers = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
    }
    return english_numbers.get(cleaned)


def _resolve_event_timezone(payload: dict[str, Any]) -> str | None:
    for key in (
        "event_timezone",
        "timezone",
        "time_zone",
        "tz",
        "meeting_timezone",
        "calendar_timezone",
    ):
        parsed = _normalize_timezone_value(payload.get(key))
        if parsed:
            return parsed
    return _infer_timezone_from_context(payload)


def _normalize_timezone_value(value: Any) -> str | None:
    text = _normalize_optional_text(value)
    if not text:
        return None
    return _extract_timezone_from_text(text)


def _infer_timezone_from_context(payload: dict[str, Any]) -> str | None:
    for key in ("source_sentence", "details", "title"):
        text = _normalize_optional_text(payload.get(key))
        if not text:
            continue
        parsed = _extract_timezone_from_text(text)
        if parsed:
            return parsed
    return None


def _extract_timezone_from_text(text: str) -> str | None:
    for raw_token in re.findall(r"\b[A-Za-z]+(?:/[A-Za-z0-9_\-+]+)+\b", text):
        try:
            ZoneInfo(raw_token)
        except ZoneInfoNotFoundError:
            continue
        return raw_token

    normalized = _normalize_for_matching(text)
    for pattern, canonical_tz in _TIMEZONE_ALIAS_PATTERNS:
        if re.search(pattern, normalized):
            return canonical_tz
    return None


def _normalize_recurrence_rule(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, Mapping):
        mapped_rrule = value.get("rrule")
        if isinstance(mapped_rrule, str):
            return _normalize_recurrence_rule(mapped_rrule)

        frequency = _normalize_frequency_token(
            value.get("frequency") or value.get("freq") or value.get("pattern"),
        )
        if not frequency:
            return None
        interval = _normalize_positive_integer(value.get("interval")) or 1
        parts = [f"FREQ={frequency}", f"INTERVAL={interval}"]

        byday = _normalize_weekday_collection(
            value.get("days_of_week") or value.get("days") or value.get("byday"),
        )
        if byday:
            parts.append(f"BYDAY={','.join(byday)}")

        bymonthday = _normalize_positive_integer(
            value.get("day_of_month") or value.get("dayOfMonth") or value.get("bymonthday"),
        )
        if bymonthday is not None and 1 <= bymonthday <= 31:
            parts.append(f"BYMONTHDAY={bymonthday}")

        return ";".join(parts)

    text = _normalize_optional_text(value)
    if not text:
        return None
    normalized = text.strip()
    if normalized.upper().startswith("RRULE:"):
        normalized = normalized[len("RRULE:") :]
    if "FREQ=" in normalized.upper():
        return _sanitize_rrule_text(normalized)
    return _parse_recurrence_from_text(text=normalized)


def _normalize_frequency_token(value: Any) -> str | None:
    text = _normalize_optional_text(value)
    if not text:
        return None
    normalized = _normalize_for_matching(text)
    mapping = {
        "daily": "DAILY",
        "diario": "DAILY",
        "diaria": "DAILY",
        "weekly": "WEEKLY",
        "semanal": "WEEKLY",
        "monthly": "MONTHLY",
        "mensual": "MONTHLY",
        "yearly": "YEARLY",
        "anual": "YEARLY",
    }
    if normalized in mapping:
        return mapping[normalized]
    normalized_upper = normalized.upper()
    if normalized_upper in {"DAILY", "WEEKLY", "MONTHLY", "YEARLY"}:
        return normalized_upper
    return None


def _normalize_positive_integer(value: Any) -> int | None:
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float) and value.is_integer():
        normalized = int(value)
        return normalized if normalized > 0 else None
    text = _normalize_optional_text(value)
    if not text:
        return None
    if text.isdigit():
        normalized = int(text)
        return normalized if normalized > 0 else None
    return _parse_spanish_integer(text)


def _normalize_weekday_collection(value: Any) -> list[str] | None:
    raw_tokens: list[str] = []
    if isinstance(value, str):
        raw_tokens = [token for token in re.split(r"[,\s]+", value) if token]
    elif isinstance(value, list):
        for raw_item in value:
            if raw_item is None:
                continue
            raw_tokens.extend([token for token in re.split(r"[,\s]+", str(raw_item)) if token])
    else:
        return None

    normalized_tokens: list[str] = []
    for token in raw_tokens:
        normalized = _weekday_token_from_text(token)
        if normalized and normalized not in normalized_tokens:
            normalized_tokens.append(normalized)
    return normalized_tokens or None


def _weekday_token_from_text(value: str) -> str | None:
    normalized = _normalize_for_matching(value)
    if normalized.upper() in {"MO", "TU", "WE", "TH", "FR", "SA", "SU"}:
        return normalized.upper()
    mapping = {
        "lunes": "MO",
        "monday": "MO",
        "martes": "TU",
        "tuesday": "TU",
        "miercoles": "WE",
        "wednesday": "WE",
        "jueves": "TH",
        "thursday": "TH",
        "viernes": "FR",
        "friday": "FR",
        "sabado": "SA",
        "saturday": "SA",
        "domingo": "SU",
        "sunday": "SU",
    }
    return mapping.get(normalized)


def _sanitize_rrule_text(raw_rrule: str) -> str | None:
    normalized_parts: list[str] = []
    for raw_part in raw_rrule.split(";"):
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
    tokens = _parse_rrule_tokens(";".join(normalized_parts))
    if tokens.get("FREQ") not in {"DAILY", "WEEKLY", "MONTHLY", "YEARLY"}:
        return None
    return ";".join(normalized_parts)


def _parse_rrule_tokens(recurrence_rule: str) -> dict[str, str]:
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


def _infer_recurrence_rule_from_context(
    payload: dict[str, Any],
    *,
    due_date: str | None,
) -> str | None:
    for key in ("source_sentence", "details", "title"):
        text = _normalize_optional_text(payload.get(key))
        if not text:
            continue
        parsed = _parse_recurrence_from_text(text=text, due_date=due_date)
        if parsed:
            return parsed
    return None


def _parse_recurrence_from_text(text: str, due_date: str | None = None) -> str | None:
    normalized = _normalize_for_matching(text)
    weekday_from_due = _weekday_token_from_due_date(due_date)
    day_of_month_from_due = _day_of_month_from_due_date(due_date)

    recurrence_weekdays = re.findall(
        r"\b(?:cada|todos\s+los|todas\s+las)\s+"
        r"(lunes|martes|miercoles|jueves|viernes|sabado|domingo)\b",
        normalized,
    )
    if recurrence_weekdays:
        weekday_tokens = [
            token for token in (_weekday_token_from_text(day) for day in recurrence_weekdays) if token
        ]
        if weekday_tokens:
            return f"FREQ=WEEKLY;INTERVAL=1;BYDAY={','.join(weekday_tokens)}"

    interval_match = re.search(
        r"\bcada\s+([a-z0-9]+)\s+(dia|dias|semana|semanas|mes|meses|ano|anos)\b",
        normalized,
    )
    if interval_match:
        amount = _parse_spanish_integer(interval_match.group(1)) or 1
        unit = interval_match.group(2)
        if unit in {"dia", "dias"}:
            return f"FREQ=DAILY;INTERVAL={amount}"
        if unit in {"semana", "semanas"}:
            byday = f";BYDAY={weekday_from_due}" if weekday_from_due else ""
            return f"FREQ=WEEKLY;INTERVAL={amount}{byday}"
        if unit in {"mes", "meses"}:
            bymonthday = f";BYMONTHDAY={day_of_month_from_due}" if day_of_month_from_due else ""
            return f"FREQ=MONTHLY;INTERVAL={amount}{bymonthday}"
        return f"FREQ=YEARLY;INTERVAL={amount}"

    if re.search(r"\b(cada semana|semanal|semanalmente)\b", normalized):
        byday = f";BYDAY={weekday_from_due}" if weekday_from_due else ""
        return f"FREQ=WEEKLY;INTERVAL=1{byday}"

    if re.search(r"\b(quincenal|cada dos semanas)\b", normalized):
        byday = f";BYDAY={weekday_from_due}" if weekday_from_due else ""
        return f"FREQ=WEEKLY;INTERVAL=2{byday}"

    if re.search(
        r"\b(?:al|a)?\s*(?:inicio|principio|principios)\s+de\s+cada\s+mes\b",
        normalized,
    ):
        return "FREQ=MONTHLY;INTERVAL=1;BYMONTHDAY=1"

    if re.search(r"\b(cada mes|mensual|mensualmente)\b", normalized):
        bymonthday = f";BYMONTHDAY={day_of_month_from_due}" if day_of_month_from_due else ""
        return f"FREQ=MONTHLY;INTERVAL=1{bymonthday}"

    if re.search(r"\b(cada ano|anual|anualmente|yearly)\b", normalized):
        return "FREQ=YEARLY;INTERVAL=1"

    if re.search(r"\b(cada dia|todos los dias|diario|diariamente)\b", normalized):
        return "FREQ=DAILY;INTERVAL=1"

    return None


def _weekday_token_from_due_date(due_date: str | None) -> str | None:
    if not due_date:
        return None
    try:
        parsed = date.fromisoformat(due_date)
    except ValueError:
        return None
    weekday_map = {
        0: "MO",
        1: "TU",
        2: "WE",
        3: "TH",
        4: "FR",
        5: "SA",
        6: "SU",
    }
    return weekday_map.get(parsed.weekday())


def _day_of_month_from_due_date(due_date: str | None) -> int | None:
    if not due_date:
        return None
    try:
        return date.fromisoformat(due_date).day
    except ValueError:
        return None


def _infer_due_date_from_recurrence_rule(
    *,
    recurrence_rule: str,
    reference_date: date,
) -> date | None:
    tokens = _parse_rrule_tokens(recurrence_rule)
    frequency = tokens.get("FREQ")
    if frequency == "DAILY":
        return reference_date

    if frequency == "WEEKLY":
        raw_byday = tokens.get("BYDAY")
        if not raw_byday:
            return reference_date
        day_map = {
            "MO": 0,
            "TU": 1,
            "WE": 2,
            "TH": 3,
            "FR": 4,
            "SA": 5,
            "SU": 6,
        }
        candidates: list[date] = []
        for day_token in raw_byday.split(","):
            weekday = day_map.get(day_token.strip().upper())
            if weekday is None:
                continue
            candidates.append(_next_weekday(reference_date, weekday, strict_future=False))
        candidates = [candidate for candidate in candidates if candidate >= reference_date]
        if candidates:
            return min(candidates)
        return reference_date

    if frequency == "MONTHLY":
        parsed_day = _normalize_positive_integer(tokens.get("BYMONTHDAY"))
        if parsed_day is None:
            return reference_date
        this_month_day = min(parsed_day, _days_in_month(reference_date.year, reference_date.month))
        this_month = date(reference_date.year, reference_date.month, this_month_day)
        if this_month >= reference_date:
            return this_month
        next_month_reference = _add_months(reference_date, 1)
        next_month_day = min(parsed_day, _days_in_month(next_month_reference.year, next_month_reference.month))
        return date(next_month_reference.year, next_month_reference.month, next_month_day)

    return reference_date


def _normalize_online_meeting_platform(value: Any) -> str | None:
    text = _normalize_optional_text(value)
    if not text:
        return None
    normalized = _normalize_for_matching(text)
    if re.search(r"\b(google\s+meet|meet\.google|meet de google)\b", normalized):
        return "google_meet"
    if re.search(r"\b(microsoft\s+teams|ms\s+teams|teams)\b", normalized):
        return "microsoft_teams"
    if re.search(r"\bauto\b", normalized):
        return "auto"
    return None


def _infer_online_meeting_platform_from_context(payload: dict[str, Any]) -> str | None:
    for key in ("source_sentence", "details", "title"):
        text = _normalize_optional_text(payload.get(key))
        if not text:
            continue
        inferred = _normalize_online_meeting_platform(text)
        if inferred:
            return inferred
    return None


def _looks_like_scheduled_meeting_request(
    *,
    title: str,
    details: str | None,
    source_sentence: str | None,
) -> bool:
    normalized_title = _normalize_for_matching(title)
    if not normalized_title:
        return False
    has_meeting_keyword = bool(
        re.search(
            r"\b(reunion|meeting|llamada|call|sesion|session|kickoff|demo)\b",
            normalized_title,
        ),
    )
    if not has_meeting_keyword:
        return False
    if re.search(
        r"\b(resumen|minuta|notas|acuerdos|recordatorio|follow up|follow-up|seguimiento)\s+de\s+"
        r"(la\s+)?(reunion|meeting)\b",
        normalized_title,
    ):
        return False

    combined_context = _normalize_for_matching(
        " ".join(value for value in (title, details or "", source_sentence or "") if value),
    )
    if re.search(
        r"\b(resumen|minuta|notas|acuerdos|recordatorio|follow up|follow-up|seguimiento)\s+de\s+"
        r"(la\s+)?(reunion|meeting)\b",
        combined_context,
    ):
        return False
    if re.search(
        r"\b(agendar|agenda|programar|programa|coordinar|coordina|calendarizar|organizar|"
        r"planificar|schedule|book|set up|hacer|realizar|tener)\s+(una\s+)?"
        r"(reunion|meeting|llamada|call|sesion|session)\b",
        combined_context,
    ):
        return True
    if re.search(
        r"\b(hay que|tenemos que|tengo que|necesito|necesitamos|debe|deben|deberiamos)\b"
        r".{0,80}\b(reunion|meeting|llamada|call|sesion|session)\b",
        combined_context,
    ):
        return True
    return bool(
        re.search(
            r"\b(agendar|agenda|programar|programa|coordinar|coordina|calendarizar|"
            r"organizar|planificar|schedule|book|set up)\b",
            combined_context,
        ),
    )


def _looks_like_meeting_context(payload: dict[str, Any]) -> bool:
    combined_text = " ".join(
        value
        for value in (
            _normalize_optional_text(payload.get("title")) or "",
            _normalize_optional_text(payload.get("details")) or "",
            _normalize_optional_text(payload.get("source_sentence")) or "",
        )
        if value
    )
    if not combined_text:
        return False
    normalized = _normalize_for_matching(combined_text)
    return bool(
        re.search(
            r"\b(reunion|meeting|llamada|videollamada|video llamada|call|sesion|session|kickoff|demo)\b",
            normalized,
        ),
    )


def _parse_due_date_from_text(*, text: str, reference_date: date) -> date | None:
    normalized = _normalize_for_matching(text)
    parsed_absolute = _parse_absolute_due_date(normalized=normalized, reference_date=reference_date)
    if parsed_absolute:
        return parsed_absolute
    return _parse_relative_due_date(normalized=normalized, reference_date=reference_date)


def _parse_relative_due_date(*, normalized: str, reference_date: date) -> date | None:

    direct_tokens = (
        ("pasado manana", 2),
        ("manana", 1),
        ("hoy", 0),
        ("ayer", -1),
        ("anteayer", -2),
    )
    for token, delta in direct_tokens:
        if token == "manana" and re.search(r"\b(?:de|por)\s+la\s+manana\b", normalized):
            continue
        if re.search(rf"\b{token}\b", normalized):
            return reference_date + timedelta(days=delta)

    compact_relative = re.fullmatch(
        r"([a-z0-9]+)\s+(dia|dias|semana|semanas|mes|meses|ano|anos)",
        normalized,
    )
    if compact_relative:
        amount = _parse_spanish_integer(compact_relative.group(1))
        if amount is not None:
            unit = compact_relative.group(2)
            return _apply_relative_amount(reference_date, amount, unit)

    for pattern in (
        r"\bdentro de\s+([a-z0-9]+)\s+(dia|dias|semana|semanas|mes|meses|ano|anos)\b",
        r"\ben\s+([a-z0-9]+)\s+(dia|dias|semana|semanas|mes|meses|ano|anos)\b",
    ):
        match = re.search(pattern, normalized)
        if not match:
            continue
        amount = _parse_spanish_integer(match.group(1))
        if amount is None:
            continue
        unit = match.group(2)
        return _apply_relative_amount(reference_date, amount, unit)

    if re.search(r"\b(proxima semana|proximo semana|semana que viene)\b", normalized):
        return reference_date + timedelta(weeks=1)

    if re.search(r"\b(proximo mes|proxima mes|mes que viene)\b", normalized):
        return _add_months(reference_date, 1)

    if re.search(r"\b(proximo ano|proxima ano|ano que viene)\b", normalized):
        return _add_years(reference_date, 1)

    weekday = _extract_weekday_reference(normalized)
    if weekday is not None:
        return _next_weekday(reference_date, weekday, strict_future=True)

    return None


def _parse_absolute_due_date(*, normalized: str, reference_date: date) -> date | None:
    pattern = (
        r"\b(?:el\s+)?(\d{1,2})\s+de\s+"
        r"(este mes|mes actual|enero|febrero|marzo|abril|abrir|mayo|junio|julio|agosto|"
        r"septiembre|setiembre|octubre|noviembre|diciembre)"
        r"(?:\s+de\s+(\d{4}))?\b"
    )
    match = re.search(pattern, normalized)
    if not match:
        return None

    day = int(match.group(1))
    month_token = match.group(2)
    raw_year = match.group(3)
    month = _month_from_token(month_token, reference_date=reference_date)
    if month is None:
        return None
    year = int(raw_year) if raw_year else reference_date.year

    try:
        return date(year, month, day)
    except ValueError:
        return None


def _month_from_token(token: str, *, reference_date: date) -> int | None:
    cleaned = token.strip()
    if cleaned in {"este mes", "mes actual"}:
        return reference_date.month

    months = {
        "enero": 1,
        "febrero": 2,
        "marzo": 3,
        "abril": 4,
        "abrir": 4,
        "mayo": 5,
        "junio": 6,
        "julio": 7,
        "agosto": 8,
        "septiembre": 9,
        "setiembre": 9,
        "octubre": 10,
        "noviembre": 11,
        "diciembre": 12,
    }
    return months.get(cleaned)


def _apply_relative_amount(reference_date: date, amount: int, unit: str) -> date:
    if unit in {"dia", "dias"}:
        return reference_date + timedelta(days=amount)
    if unit in {"semana", "semanas"}:
        return reference_date + timedelta(weeks=amount)
    if unit in {"mes", "meses"}:
        return _add_months(reference_date, amount)
    return _add_years(reference_date, amount)


def _parse_spanish_integer(token: str) -> int | None:
    cleaned = token.strip().lower()
    if cleaned.isdigit():
        return int(cleaned)

    dictionary = {
        "un": 1,
        "una": 1,
        "uno": 1,
        "dos": 2,
        "tres": 3,
        "cuatro": 4,
        "cinco": 5,
        "seis": 6,
        "siete": 7,
        "ocho": 8,
        "nueve": 9,
        "diez": 10,
        "once": 11,
        "doce": 12,
    }
    return dictionary.get(cleaned)


def _normalize_for_matching(text: str) -> str:
    lowered = text.lower().strip()
    decomposed = unicodedata.normalize("NFD", lowered)
    without_accents = "".join(
        char for char in decomposed if unicodedata.category(char) != "Mn"
    )
    return re.sub(r"\s+", " ", without_accents)


def _add_months(base_date: date, months: int) -> date:
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base_date.day, _days_in_month(year, month))
    return date(year, month, day)


def _add_years(base_date: date, years: int) -> date:
    year = base_date.year + years
    day = min(base_date.day, _days_in_month(year, base_date.month))
    return date(year, base_date.month, day)


def _days_in_month(year: int, month: int) -> int:
    if month == 2:
        if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
            return 29
        return 28
    if month in {4, 6, 9, 11}:
        return 30
    return 31


def _extract_weekday_reference(normalized: str) -> int | None:
    weekdays = {
        "lunes": 0,
        "martes": 1,
        "miercoles": 2,
        "jueves": 3,
        "viernes": 4,
        "sabado": 5,
        "domingo": 6,
    }
    match = re.search(
        r"\b(?:el|este|esta|proximo|proxima|todos los|todas las|cada)\s+"
        r"(lunes|martes|miercoles|jueves|viernes|sabado|domingo)\b",
        normalized,
    )
    if not match:
        return None
    return weekdays.get(match.group(1))


def _next_weekday(reference_date: date, weekday: int, *, strict_future: bool) -> date:
    days_ahead = weekday - reference_date.weekday()
    if days_ahead < 0 or (strict_future and days_ahead == 0):
        days_ahead += 7
    return reference_date + timedelta(days=days_ahead)


def _looks_like_action_item(
    *,
    title: str,
    details: str | None,
    source_sentence: str | None,
    assignee_email: str | None,
    assignee_name: str | None,
    due_date: str | None,
    scheduled_start: str | None,
) -> bool:
    normalized_title = _normalize_for_matching(title)
    if _is_trivially_non_action_text(normalized_title):
        return False

    combined_text = " ".join(
        value for value in (title, details or "", source_sentence or "") if value
    )
    normalized_combined = _normalize_for_matching(combined_text)
    if _contains_action_marker(normalized_combined):
        return True

    has_assignment_signal = bool(assignee_email or assignee_name)
    has_due_date_signal = bool(due_date or scheduled_start)
    if (has_assignment_signal or has_due_date_signal) and _is_non_generic_title(normalized_title):
        return True

    return False


def _contains_action_marker(normalized_text: str) -> bool:
    obligation_patterns = (
        r"\b(tenemos que|tengo que|tienes que|debo|debes|debe|deberiamos|hay que)\b",
        r"\b(pendiente|recordar|recordatorio|follow up|follow-up|to do|todo)\b",
    )
    for pattern in obligation_patterns:
        if re.search(pattern, normalized_text):
            return True

    action_tokens = {
        "enviar",
        "enviamos",
        "enviarle",
        "preparar",
        "prepara",
        "revisar",
        "revisa",
        "crear",
        "crea",
        "actualizar",
        "actualiza",
        "compartir",
        "comparte",
        "coordinar",
        "coordina",
        "agendar",
        "agenda",
        "programar",
        "programa",
        "documentar",
        "documenta",
        "definir",
        "define",
        "confirmar",
        "confirma",
        "validar",
        "valida",
        "investigar",
        "investiga",
        "resolver",
        "resuelve",
        "entregar",
        "entrega",
        "completar",
        "completa",
        "terminar",
        "termina",
        "redactar",
        "redacta",
        "llamar",
        "llama",
        "contactar",
        "contacta",
        "escribir",
        "escribe",
        "subir",
        "sube",
        "priorizar",
        "prioriza",
        "send",
        "prepare",
        "review",
        "create",
        "update",
        "share",
        "schedule",
        "call",
        "deliver",
        "finish",
        "complete",
    }
    for token in re.findall(r"[a-z0-9]+", normalized_text):
        if token in action_tokens:
            return True
    return False


def _is_trivially_non_action_text(normalized_title: str) -> bool:
    if not normalized_title:
        return True

    if re.fullmatch(
        r"(ok|okay|si|claro|gracias|perfecto|entendido|listo|de acuerdo)",
        normalized_title,
    ):
        return True

    tokens = re.findall(r"[a-z0-9]+", normalized_title)
    if not tokens:
        return True
    if len(tokens) == 1 and tokens[0].isdigit():
        return True

    return False


def _is_non_generic_title(normalized_title: str) -> bool:
    tokens = re.findall(r"[a-z0-9]+", normalized_title)
    if len(tokens) < 2:
        return False

    date_only_tokens = {
        "hoy",
        "manana",
        "ayer",
        "pasado",
        "semana",
        "semanas",
        "mes",
        "meses",
        "ano",
        "anos",
        "este",
        "actual",
        "enero",
        "febrero",
        "marzo",
        "abril",
        "abrir",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "setiembre",
        "octubre",
        "noviembre",
        "diciembre",
    }
    if all(token.isdigit() or token in date_only_tokens for token in tokens):
        return False
    return True
