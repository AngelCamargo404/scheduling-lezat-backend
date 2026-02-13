from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import re
from typing import Any
import unicodedata


@dataclass
class ActionItem:
    title: str
    assignee_email: str | None = None
    assignee_name: str | None = None
    due_date: str | None = None
    details: str | None = None
    source_sentence: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "assignee_email": self.assignee_email,
            "assignee_name": self.assignee_name,
            "due_date": self.due_date,
            "details": self.details,
            "source_sentence": self.source_sentence,
        }

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

        assignee_email = _normalize_email(payload.get("assignee_email"))
        assignee_name = _normalize_optional_text(payload.get("assignee_name"))
        due_date = _normalize_due_date(
            payload.get("due_date"),
            reference_date=reference_date,
        )
        if not due_date:
            due_date = _infer_due_date_from_context(payload, reference_date=reference_date)
        details = _normalize_optional_text(payload.get("details"))
        source_sentence = _normalize_optional_text(payload.get("source_sentence"))
        return cls(
            title=title,
            assignee_email=assignee_email,
            assignee_name=assignee_name,
            due_date=due_date,
            details=details,
            source_sentence=source_sentence,
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
        parsed_relative = _parse_relative_due_date(
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
        parsed = _parse_relative_due_date(text=text, reference_date=base_date)
        if parsed:
            return parsed.isoformat()
    return None


def _parse_relative_due_date(*, text: str, reference_date: date) -> date | None:
    normalized = _normalize_for_matching(text)

    direct_tokens = (
        ("pasado manana", 2),
        ("manana", 1),
        ("hoy", 0),
        ("ayer", -1),
        ("anteayer", -2),
    )
    for token, delta in direct_tokens:
        if re.search(rf"\b{token}\b", normalized):
            return reference_date + timedelta(days=delta)

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
        r"\b(?:el|este|esta|proximo|proxima)\s+(lunes|martes|miercoles|jueves|viernes|sabado|domingo)\b",
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
