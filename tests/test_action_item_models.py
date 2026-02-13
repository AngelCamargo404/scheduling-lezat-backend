from datetime import date

from app.services.action_item_models import ActionItem


def test_action_item_parses_relative_due_date_tomorrow() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Enviar propuesta",
            "due_date": "mañana",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-02-14"


def test_action_item_parses_relative_due_date_yesterday() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Enviar reporte",
            "due_date": "ayer",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-02-12"


def test_action_item_parses_relative_due_date_in_two_weeks() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Preparar roadmap",
            "due_date": "dentro de 2 semanas",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-02-27"


def test_action_item_parses_relative_due_date_in_one_month() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Cierre mensual",
            "due_date": "dentro de 1 mes",
        },
        reference_date=date(2026, 1, 31),
    )

    assert item is not None
    assert item.due_date == "2026-02-28"


def test_action_item_infers_due_date_from_context_when_due_date_is_empty() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Enviar reporte financiero",
            "due_date": None,
            "source_sentence": "Perfecto, esto lo cerramos dentro de 2 semanas.",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-02-27"

