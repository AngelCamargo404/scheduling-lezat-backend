from datetime import date

from app.services.action_item_models import ActionItem


def test_action_item_parses_relative_due_date_tomorrow() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Enviar propuesta",
            "due_date": "manana",
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


def test_action_item_parses_absolute_due_date_with_current_year() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Preparar informe trimestral",
            "due_date": "23 de febrero",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-02-23"


def test_action_item_parses_absolute_due_date_with_common_month_typo() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Subir entregable final",
            "due_date": "30 de abrir",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-04-30"


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


def test_action_item_infers_due_date_from_this_month_expression() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Enviar reporte financiero",
            "due_date": None,
            "source_sentence": "Dejalo listo para el 25 de este mes.",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-02-25"


def test_action_item_rejects_non_actionable_text() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Resumen de la reunion",
            "details": "Se revisaron avances y riesgos.",
            "source_sentence": "Hablamos del roadmap.",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is None
