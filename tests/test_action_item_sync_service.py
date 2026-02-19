from app.core.config import Settings
from app.services.action_item_models import ActionItem
from app.services.action_item_sync_service import ActionItemSyncService


class _FakeGeminiClient:
    def extract_action_items(
        self,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, object]],
        participant_emails: list[str],
    ) -> list[ActionItem]:
        return [
            ActionItem(
                title="Enviar resumen",
                details="Compartir acuerdos en el canal interno.",
            ),
        ]


class _FakeMondayClient:
    def create_kanban_item(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
    ) -> str:
        assert item.title in {
            "Enviar resumen",
            "Agendar seguimiento",
            "Reunion para desarrollar proyecto",
            "Reunion de seguimiento cliente",
            "Reunion sin link de Teams",
        }
        assert meeting_id is not None
        return "monday-item-1"


class _FakeGoogleCalendarClient:
    def __init__(self) -> None:
        self.calls = 0

    def create_due_date_event(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
    ) -> str:
        self.calls += 1
        assert meeting_id == "meeting-3"
        assert item.scheduled_start == "2026-02-19T11:00:00"
        return "google-event-1"

    def create_due_date_event_with_details(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
    ) -> dict[str, str | None]:
        event_id = self.create_due_date_event(item=item, meeting_id=meeting_id)
        return {
            "event_id": event_id,
            "google_meet_link": "https://meet.google.com/fake-link",
        }


class _FakeOutlookCalendarClient:
    def __init__(self) -> None:
        self.calls = 0

    def create_due_date_event(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
    ) -> str:
        self.calls += 1
        assert meeting_id == "meeting-4"
        assert item.online_meeting_platform == "microsoft_teams"
        assert item.scheduled_start == "2026-03-20T09:00:00"
        return "outlook-event-1"

    def create_due_date_event_with_details(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
    ) -> dict[str, str | bool | None]:
        event_id = self.create_due_date_event(item=item, meeting_id=meeting_id)
        return {
            "event_id": event_id,
            "teams_join_url": "https://teams.live.com/meet/fake-link",
            "is_online_meeting": True,
        }


def test_sync_creates_items_in_monday_when_notion_is_not_configured() -> None:
    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiClient(),
        notion_client=None,
        monday_kanban_client=_FakeMondayClient(),
        google_calendar_client=None,
        outlook_calendar_client=None,
    )

    result = service.sync(
        meeting_id="meeting-1",
        transcript_text="Debemos enviar un resumen al cierre.",
        transcript_sentences=[],
        participant_emails=[],
    )

    assert result["status"] == "completed"
    assert result["created_count"] == 1
    assert result["monday_status"] == "completed"
    assert result["monday_created_count"] == 1
    assert result["items"][0]["status"] == "created"
    assert result["items"][0]["monday_status"] == "created"
    assert result["items"][0]["monday_item_id"] == "monday-item-1"


def test_sync_requires_at_least_one_notes_output() -> None:
    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiClient(),
        notion_client=None,
        monday_kanban_client=None,
        google_calendar_client=None,
        outlook_calendar_client=None,
    )

    result = service.sync(
        meeting_id="meeting-2",
        transcript_text="Debemos enviar un resumen al cierre.",
        transcript_sentences=[],
        participant_emails=[],
    )

    assert result["status"] == "skipped_missing_configuration"
    assert result["created_count"] == 0
    assert result["monday_status"] == "skipped_missing_configuration"


def test_sync_uses_scheduled_start_for_calendar_creation() -> None:
    class _FakeGeminiMeetingClient:
        def extract_action_items(
            self,
            *,
            meeting_id: str | None,
            transcript_text: str | None,
            transcript_sentences: list[dict[str, object]],
            participant_emails: list[str],
        ) -> list[ActionItem]:
            return [
                ActionItem(
                    title="Agendar seguimiento",
                    scheduled_start="2026-02-19T11:00:00",
                    recurrence_rule="FREQ=WEEKLY;INTERVAL=1;BYDAY=TH",
                    online_meeting_platform="google_meet",
                ),
            ]

    fake_google = _FakeGoogleCalendarClient()
    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiMeetingClient(),
        notion_client=None,
        monday_kanban_client=_FakeMondayClient(),
        google_calendar_client=fake_google,
        outlook_calendar_client=None,
    )

    result = service.sync(
        meeting_id="meeting-3",
        transcript_text="Agendar seguimiento todos los jueves a las 11 por Google Meet.",
        transcript_sentences=[],
        participant_emails=[],
    )

    assert result["status"] == "completed"
    assert result["google_calendar_status"] == "completed"
    assert result["google_calendar_created_count"] == 1
    assert fake_google.calls == 1


def test_sync_creates_events_for_explicit_teams_meeting_when_both_calendars_enabled() -> None:
    class _FakeGeminiTeamsMeetingClient:
        def extract_action_items(
            self,
            *,
            meeting_id: str | None,
            transcript_text: str | None,
            transcript_sentences: list[dict[str, object]],
            participant_emails: list[str],
        ) -> list[ActionItem]:
            return [
                ActionItem(
                    title="Reunion para desarrollar proyecto",
                    due_date="2026-03-20",
                    scheduled_start="2026-03-20T09:00:00",
                    online_meeting_platform="microsoft_teams",
                ),
            ]

    class _FakeGoogleCalendarClientForTeams:
        def __init__(self) -> None:
            self.calls = 0

        def create_due_date_event(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
        ) -> str:
            self.calls += 1
            assert meeting_id == "meeting-4"
            assert item.online_meeting_platform == "microsoft_teams"
            return "google-event-2"

        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
        ) -> dict[str, str | None]:
            event_id = self.create_due_date_event(item=item, meeting_id=meeting_id)
            return {
                "event_id": event_id,
                "google_meet_link": None,
            }

    fake_google = _FakeGoogleCalendarClientForTeams()
    fake_outlook = _FakeOutlookCalendarClient()
    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiTeamsMeetingClient(),
        notion_client=None,
        monday_kanban_client=_FakeMondayClient(),
        google_calendar_client=fake_google,
        outlook_calendar_client=fake_outlook,
    )

    result = service.sync(
        meeting_id="meeting-4",
        transcript_text="Necesito agendar una reunion con cliente para el 20 de marzo.",
        transcript_sentences=[],
        participant_emails=[],
    )

    assert result["status"] == "completed"
    assert result["google_calendar_status"] == "completed"
    assert result["outlook_calendar_status"] == "completed"
    assert result["google_calendar_created_count"] == 1
    assert result["outlook_calendar_created_count"] == 1
    assert fake_google.calls == 1
    assert fake_outlook.calls == 1


def test_sync_creates_only_google_meet_when_platform_is_google_meet_and_only_google_is_configured() -> None:
    class _FakeGeminiGoogleMeetClient:
        def extract_action_items(
            self,
            *,
            meeting_id: str | None,
            transcript_text: str | None,
            transcript_sentences: list[dict[str, object]],
            participant_emails: list[str],
        ) -> list[ActionItem]:
            return [
                ActionItem(
                    title="Reunion de seguimiento cliente",
                    due_date="2026-03-21",
                    scheduled_start="2026-03-21T10:00:00",
                    online_meeting_platform="google_meet",
                ),
            ]

    class _FakeGoogleCalendarClientWithMeet:
        def __init__(self) -> None:
            self.calls = 0

        def create_due_date_event(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
        ) -> str:
            self.calls += 1
            assert meeting_id == "meeting-5"
            assert item.online_meeting_platform == "google_meet"
            return "google-event-3"

        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
        ) -> dict[str, str | None]:
            event_id = self.create_due_date_event(item=item, meeting_id=meeting_id)
            return {
                "event_id": event_id,
                "google_meet_link": "https://meet.google.com/fake-explicit-link",
            }

    fake_google = _FakeGoogleCalendarClientWithMeet()
    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiGoogleMeetClient(),
        notion_client=None,
        monday_kanban_client=_FakeMondayClient(),
        google_calendar_client=fake_google,
        outlook_calendar_client=None,
    )

    result = service.sync(
        meeting_id="meeting-5",
        transcript_text="Necesito agendar reunion de seguimiento para el 21 de marzo.",
        transcript_sentences=[],
        participant_emails=[],
    )

    assert result["status"] == "completed"
    assert result["google_calendar_status"] == "completed"
    assert result["google_calendar_created_count"] == 1
    assert result["outlook_calendar_status"] == "skipped_missing_configuration"
    assert result["outlook_calendar_created_count"] == 0
    assert fake_google.calls == 1


def test_sync_marks_outlook_error_when_teams_link_is_missing_for_explicit_teams_meeting() -> None:
    class _FakeGeminiTeamsMeetingClient:
        def extract_action_items(
            self,
            *,
            meeting_id: str | None,
            transcript_text: str | None,
            transcript_sentences: list[dict[str, object]],
            participant_emails: list[str],
        ) -> list[ActionItem]:
            return [
                ActionItem(
                    title="Reunion sin link de Teams",
                    due_date="2026-03-22",
                    scheduled_start="2026-03-22T09:00:00",
                    online_meeting_platform="microsoft_teams",
                ),
            ]

    class _FakeGoogleCalendarClientWithLink:
        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
        ) -> dict[str, str | None]:
            return {
                "event_id": "google-event-4",
                "google_meet_link": None,
            }

    class _FakeOutlookCalendarClientWithoutLink:
        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
        ) -> dict[str, str | bool | None]:
            return {
                "event_id": "outlook-event-4",
                "teams_join_url": None,
                "is_online_meeting": False,
            }

    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiTeamsMeetingClient(),
        notion_client=None,
        monday_kanban_client=_FakeMondayClient(),
        google_calendar_client=_FakeGoogleCalendarClientWithLink(),
        outlook_calendar_client=_FakeOutlookCalendarClientWithoutLink(),
    )

    result = service.sync(
        meeting_id="meeting-6",
        transcript_text="Agendar reunion con cliente para el domingo.",
        transcript_sentences=[],
        participant_emails=[],
    )

    assert result["status"] == "completed_with_errors"
    assert result["google_calendar_status"] == "completed"
    assert result["outlook_calendar_status"] == "failed_sync"
    assert result["google_calendar_created_count"] == 1
    assert result["outlook_calendar_created_count"] == 0
    assert result["items"][0]["google_meet_link"] is None
    assert result["items"][0]["outlook_calendar_status"] == "failed_missing_teams_link"
    assert result["items"][0]["outlook_teams_link"] is None


def test_sync_skips_meeting_calendar_creation_when_shared_team_event_flags_are_enabled() -> None:
    class _FakeGeminiMeetingClient:
        def extract_action_items(
            self,
            *,
            meeting_id: str | None,
            transcript_text: str | None,
            transcript_sentences: list[dict[str, object]],
            participant_emails: list[str],
        ) -> list[ActionItem]:
            return [
                ActionItem(
                    title="Reunion con cliente",
                    due_date="2026-04-15",
                    scheduled_start="2026-04-15T10:00:00",
                    online_meeting_platform="google_meet",
                ),
            ]

    class _FakeGoogleClient:
        def __init__(self) -> None:
            self.calls = 0

        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
            attendee_emails: list[str] | None = None,
        ) -> dict[str, str | None]:
            self.calls += 1
            return {
                "event_id": "google-event-should-not-create",
                "google_meet_link": "https://meet.google.com/unexpected",
            }

    class _FakeOutlookClient:
        def __init__(self) -> None:
            self.calls = 0

        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
            attendee_emails: list[str] | None = None,
        ) -> dict[str, str | bool | None]:
            self.calls += 1
            return {
                "event_id": "outlook-event-should-not-create",
                "teams_join_url": "https://teams.live.com/meet/unexpected",
                "is_online_meeting": True,
            }

    fake_google = _FakeGoogleClient()
    fake_outlook = _FakeOutlookClient()
    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiMeetingClient(),
        notion_client=None,
        monday_kanban_client=_FakeMondayClient(),
        google_calendar_client=fake_google,
        outlook_calendar_client=fake_outlook,
    )

    result = service.sync(
        meeting_id="meeting-shared-1",
        transcript_text="Agendar reunion con cliente.",
        transcript_sentences=[],
        participant_emails=[],
        skip_google_meeting_items=True,
        skip_outlook_meeting_items=True,
    )

    assert result["status"] == "completed"
    assert result["google_calendar_status"] == "shared_from_team_event"
    assert result["outlook_calendar_status"] == "shared_from_team_event"
    assert result["google_calendar_created_count"] == 0
    assert result["outlook_calendar_created_count"] == 0
    assert result["items"][0]["google_calendar_status"] == "skipped_shared_team_meeting_event"
    assert result["items"][0]["outlook_calendar_status"] == "skipped_shared_team_meeting_event"
    assert fake_google.calls == 0
    assert fake_outlook.calls == 0


def test_sync_does_not_skip_non_meeting_calendar_items_when_shared_team_flags_are_enabled() -> None:
    class _FakeGeminiNonMeetingClient:
        def extract_action_items(
            self,
            *,
            meeting_id: str | None,
            transcript_text: str | None,
            transcript_sentences: list[dict[str, object]],
            participant_emails: list[str],
        ) -> list[ActionItem]:
            return [
                ActionItem(
                    title="Enviar propuesta",
                    due_date="2026-04-16",
                    scheduled_start="2026-04-16T09:00:00",
                    online_meeting_platform=None,
                ),
            ]

    class _FakeGoogleClient:
        def __init__(self) -> None:
            self.calls = 0

        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
            attendee_emails: list[str] | None = None,
        ) -> dict[str, str | None]:
            self.calls += 1
            return {
                "event_id": "google-event-created",
                "google_meet_link": None,
            }

    class _FakeOutlookClient:
        def __init__(self) -> None:
            self.calls = 0

        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
            attendee_emails: list[str] | None = None,
        ) -> dict[str, str | bool | None]:
            self.calls += 1
            return {
                "event_id": "outlook-event-created",
                "teams_join_url": None,
                "is_online_meeting": False,
            }

    fake_google = _FakeGoogleClient()
    fake_outlook = _FakeOutlookClient()
    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiNonMeetingClient(),
        notion_client=None,
        monday_kanban_client=_FakeMondayClient(),
        google_calendar_client=fake_google,
        outlook_calendar_client=fake_outlook,
    )

    result = service.sync(
        meeting_id="meeting-shared-2",
        transcript_text="Enviar propuesta al cliente.",
        transcript_sentences=[],
        participant_emails=[],
        skip_google_meeting_items=True,
        skip_outlook_meeting_items=True,
    )

    assert result["status"] == "completed"
    assert result["google_calendar_status"] == "completed"
    assert result["outlook_calendar_status"] == "completed"
    assert result["google_calendar_created_count"] == 1
    assert result["outlook_calendar_created_count"] == 1
    assert result["items"][0]["google_calendar_status"] == "created"
    assert result["items"][0]["outlook_calendar_status"] == "created"
    assert fake_google.calls == 1
    assert fake_outlook.calls == 1


def test_sync_does_not_send_attendees_for_non_meeting_calendar_items() -> None:
    class _FakeGeminiNonMeetingClient:
        def extract_action_items(
            self,
            *,
            meeting_id: str | None,
            transcript_text: str | None,
            transcript_sentences: list[dict[str, object]],
            participant_emails: list[str],
        ) -> list[ActionItem]:
            return [
                ActionItem(
                    title="Enviar minuta",
                    due_date="2026-04-20",
                    scheduled_start="2026-04-20T09:30:00",
                    online_meeting_platform=None,
                ),
            ]

    captured_google_attendees: list[str] = []
    captured_outlook_attendees: list[str] = []

    class _FakeGoogleClient:
        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
            attendee_emails: list[str] | None = None,
        ) -> dict[str, str | None]:
            captured_google_attendees.extend(attendee_emails or [])
            return {"event_id": "google-event-non-meeting", "google_meet_link": None}

    class _FakeOutlookClient:
        def create_due_date_event_with_details(
            self,
            *,
            item: ActionItem,
            meeting_id: str | None,
            attendee_emails: list[str] | None = None,
        ) -> dict[str, str | bool | None]:
            captured_outlook_attendees.extend(attendee_emails or [])
            return {
                "event_id": "outlook-event-non-meeting",
                "teams_join_url": None,
                "is_online_meeting": False,
            }

    service = ActionItemSyncService(
        settings=Settings(),
        gemini_client=_FakeGeminiNonMeetingClient(),
        notion_client=None,
        monday_kanban_client=_FakeMondayClient(),
        google_calendar_client=_FakeGoogleClient(),
        outlook_calendar_client=_FakeOutlookClient(),
    )
    result = service.sync(
        meeting_id="meeting-7",
        transcript_text="Enviar minuta al equipo.",
        transcript_sentences=[],
        participant_emails=["a@example.com", "b@example.com"],
    )

    assert result["status"] == "completed"
    assert result["google_calendar_status"] == "completed"
    assert result["outlook_calendar_status"] == "completed"
    assert captured_google_attendees == []
    assert captured_outlook_attendees == []
