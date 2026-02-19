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


def test_action_item_infers_schedule_recurrence_and_google_meet_from_context() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Agendar seguimiento semanal",
            "source_sentence": (
                "Agendemos una reunion todos los jueves a las 11 de la manana por Google Meet."
            ),
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-02-19"
    assert item.scheduled_start is not None
    assert item.scheduled_start.startswith("2026-02-19T11:00:00")
    assert item.recurrence_rule == "FREQ=WEEKLY;INTERVAL=1;BYDAY=TH"
    assert item.online_meeting_platform == "google_meet"


def test_action_item_infers_monthly_recurrence_due_date_and_teams() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Programar kickoff mensual",
            "source_sentence": "Programar reunion a principio de cada mes por Microsoft Teams.",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-03-01"
    assert item.scheduled_start is not None
    assert item.scheduled_start.startswith("2026-03-01T09:00:00")
    assert item.recurrence_rule == "FREQ=MONTHLY;INTERVAL=1;BYMONTHDAY=1"
    assert item.online_meeting_platform == "microsoft_teams"


def test_action_item_sets_auto_meeting_platform_when_task_is_meeting() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Reunion con cliente potencial",
            "source_sentence": "Necesito que coloquemos una reunion para el 20 de marzo de 2026.",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-03-20"
    assert item.online_meeting_platform == "auto"
    assert item.scheduled_start is not None
    assert item.scheduled_start.startswith("2026-03-20T09:00:00")


def test_action_item_keeps_non_meeting_task_without_online_platform() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Enviar resumen de reunion",
            "source_sentence": "En la reunion acordamos enviar el resumen el 20 de marzo de 2026.",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-03-20"
    assert item.online_meeting_platform is None


def test_action_item_does_not_set_auto_for_meeting_notes_titles() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Reunion semanal - enviar minuta",
            "source_sentence": "Enviar la minuta de la reunion del 5 de marzo de 2026.",
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-03-05"
    assert item.online_meeting_platform is None


def test_action_item_infers_event_timezone_from_transcription_context() -> None:
    item = ActionItem.from_payload(
        {
            "title": "Agendar sesion con cliente",
            "source_sentence": (
                "Agendemos la reunion para el 12 de noviembre de 2026 a las 9 am hora de Mexico."
            ),
        },
        reference_date=date(2026, 2, 13),
    )

    assert item is not None
    assert item.due_date == "2026-11-12"
    assert item.scheduled_start is not None
    assert item.scheduled_start.startswith("2026-11-12T09:00:00")
    assert item.event_timezone == "America/Mexico_City"
