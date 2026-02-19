from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from app.core.config import Settings
from app.services.action_item_models import ActionItem
from app.services.gemini_action_items_client import GeminiActionItemsClient, GeminiActionItemsError
from app.services.google_calendar_client import GoogleCalendarClient, GoogleCalendarError
from app.services.monday_kanban_client import MondayKanbanClient, MondayKanbanError
from app.services.notion_kanban_client import NotionKanbanClient, NotionKanbanError
from app.services.outlook_calendar_client import OutlookCalendarClient, OutlookCalendarError


class ActionItemSyncService:
    def __init__(
        self,
        settings: Settings,
        gemini_client: GeminiActionItemsClient | None = None,
        notion_client: NotionKanbanClient | None = None,
        monday_kanban_client: MondayKanbanClient | None = None,
        google_calendar_client: GoogleCalendarClient | None = None,
        outlook_calendar_client: OutlookCalendarClient | None = None,
    ) -> None:
        self.settings = settings
        self.gemini_client = gemini_client or self._create_gemini_client()
        self.notion_client = notion_client or self._create_notion_client()
        self.monday_kanban_client = monday_kanban_client or self._create_monday_kanban_client()
        self.google_calendar_client = (
            google_calendar_client or self._create_google_calendar_client()
        )
        self.outlook_calendar_client = (
            outlook_calendar_client or self._create_outlook_calendar_client()
        )

    def sync(
        self,
        *,
        meeting_id: str | None,
        transcript_text: str | None,
        transcript_sentences: list[dict[str, Any]],
        participant_emails: list[str],
        calendar_attendee_emails: list[str] | None = None,
        skip_google_meeting_items: bool = False,
        skip_outlook_meeting_items: bool = False,
    ) -> dict[str, Any]:
        notes_outputs_description = self._describe_enabled_notes_outputs()
        normalized_calendar_attendees = sanitize_action_item_participants(
            calendar_attendee_emails or participant_emails,
        )
        if not transcript_text:
            return self._build_result(
                status="skipped_no_transcript",
                extracted_count=0,
                created_count=0,
                monday_status="not_required_no_action_items",
                monday_created_count=0,
                monday_error=None,
                google_calendar_status="not_required_no_due_dates",
                google_calendar_created_count=0,
                google_calendar_error=None,
                outlook_calendar_status="not_required_no_due_dates",
                outlook_calendar_created_count=0,
                outlook_calendar_error=None,
                items=[],
                error=None,
            )
        if self.settings.action_items_test_mode_enabled:
            if not self.notion_client and not self.monday_kanban_client:
                return self._build_result(
                    status="skipped_missing_configuration",
                    extracted_count=0,
                    created_count=0,
                    monday_status="skipped_missing_configuration",
                    monday_created_count=0,
                    monday_error=(
                        "MONDAY_API_TOKEN, MONDAY_BOARD_ID or MONDAY_GROUP_ID is missing."
                    ),
                    google_calendar_status="not_required_no_due_dates",
                    google_calendar_created_count=0,
                    google_calendar_error=None,
                    outlook_calendar_status="not_required_no_due_dates",
                    outlook_calendar_created_count=0,
                    outlook_calendar_error=None,
                    items=[],
                    error=(
                        "Enable at least one notes output: "
                        "NOTION_API_TOKEN + NOTION_TASKS_DATABASE_ID, or "
                        "MONDAY_API_TOKEN + MONDAY_BOARD_ID + MONDAY_GROUP_ID."
                    ),
                )
            action_items = self._build_test_action_items(
                meeting_id=meeting_id,
                transcript_text=transcript_text,
                transcript_sentences=transcript_sentences,
                participant_emails=participant_emails,
            )
        elif not self.gemini_client or (not self.notion_client and not self.monday_kanban_client):
            return self._build_result(
                status="skipped_missing_configuration",
                extracted_count=0,
                created_count=0,
                monday_status="skipped_missing_configuration",
                monday_created_count=0,
                monday_error=(
                    "MONDAY_API_TOKEN, MONDAY_BOARD_ID or MONDAY_GROUP_ID is missing."
                ),
                google_calendar_status="not_required_no_due_dates",
                google_calendar_created_count=0,
                google_calendar_error=None,
                outlook_calendar_status="not_required_no_due_dates",
                outlook_calendar_created_count=0,
                outlook_calendar_error=None,
                items=[],
                error=(
                    "GEMINI_API_KEY and at least one notes output are required "
                    "(Notion or Monday)."
                ),
            )

        if not self.settings.action_items_test_mode_enabled:
            try:
                action_items = self.gemini_client.extract_action_items(
                    meeting_id=meeting_id,
                    transcript_text=transcript_text,
                    transcript_sentences=transcript_sentences,
                    participant_emails=participant_emails,
                )
            except GeminiActionItemsError as exc:
                return self._build_result(
                    status="failed_analysis",
                    extracted_count=0,
                    created_count=0,
                    monday_status="not_required_no_action_items",
                    monday_created_count=0,
                    monday_error=None,
                    google_calendar_status="not_required_no_due_dates",
                    google_calendar_created_count=0,
                    google_calendar_error=None,
                    outlook_calendar_status="not_required_no_due_dates",
                    outlook_calendar_created_count=0,
                    outlook_calendar_error=None,
                    items=[],
                    error=str(exc),
                )

        if not action_items:
            return self._build_result(
                status="skipped_no_action_items",
                extracted_count=0,
                created_count=0,
                monday_status="not_required_no_action_items",
                monday_created_count=0,
                monday_error=None,
                google_calendar_status="not_required_no_due_dates",
                google_calendar_created_count=0,
                google_calendar_error=None,
                outlook_calendar_status="not_required_no_due_dates",
                outlook_calendar_created_count=0,
                outlook_calendar_error=None,
                items=[],
                error=None,
            )
        action_items = self._apply_test_due_date(action_items)

        created_count = 0
        notion_created_count = 0
        monday_created_count = 0
        google_calendar_created_count = 0
        outlook_calendar_created_count = 0
        serialized_items: list[dict[str, Any]] = []
        for action_item in action_items:
            serialized_item = action_item.to_dict()
            serialized_item["notion_page_id"] = None
            serialized_item["monday_item_id"] = None
            serialized_item["google_calendar_event_id"] = None
            serialized_item["outlook_calendar_event_id"] = None
            serialized_item["google_meet_link"] = None
            serialized_item["outlook_teams_link"] = None
            serialized_item["status"] = "pending"
            serialized_item["error"] = None
            serialized_item["notion_status"] = "not_configured"
            serialized_item["notion_error"] = None
            serialized_item["monday_status"] = "not_configured"
            serialized_item["monday_error"] = None
            serialized_item["google_calendar_status"] = "not_required_no_due_date"
            serialized_item["google_calendar_error"] = None
            serialized_item["outlook_calendar_status"] = "not_required_no_due_date"
            serialized_item["outlook_calendar_error"] = None
            notes_errors: list[str] = []
            notes_created = False

            if self.notion_client:
                try:
                    page_id = self.notion_client.create_kanban_task(
                        item=action_item,
                        meeting_id=meeting_id,
                    )
                except NotionKanbanError as exc:
                    serialized_item["notion_status"] = "failed"
                    serialized_item["notion_error"] = str(exc)
                    notes_errors.append(str(exc))
                else:
                    serialized_item["notion_page_id"] = page_id
                    serialized_item["notion_status"] = "created"
                    notion_created_count += 1
                    notes_created = True
            else:
                serialized_item["notion_status"] = "skipped_missing_configuration"
                serialized_item["notion_error"] = "NOTION_API_TOKEN or NOTION_TASKS_DATABASE_ID is missing."

            if self.monday_kanban_client:
                try:
                    monday_item_id = self.monday_kanban_client.create_kanban_item(
                        item=action_item,
                        meeting_id=meeting_id,
                    )
                except MondayKanbanError as exc:
                    serialized_item["monday_status"] = "failed"
                    serialized_item["monday_error"] = str(exc)
                    notes_errors.append(str(exc))
                else:
                    serialized_item["monday_item_id"] = monday_item_id
                    serialized_item["monday_status"] = "created"
                    monday_created_count += 1
                    notes_created = True
            else:
                serialized_item["monday_status"] = "skipped_missing_configuration"
                serialized_item["monday_error"] = (
                    "MONDAY_API_TOKEN, MONDAY_BOARD_ID or MONDAY_GROUP_ID is missing."
                )

            if not notes_created:
                serialized_item["status"] = "failed"
                serialized_item["error"] = (
                    "; ".join(notes_errors[:3])
                    if notes_errors
                    else "No action item could be created in configured note outputs."
                )
                serialized_items.append(serialized_item)
                continue

            created_count += 1
            serialized_item["status"] = "created"

            if action_item.has_calendar_schedule():
                attendee_emails_for_calendar = (
                    normalized_calendar_attendees
                    if self._is_explicit_online_meeting(action_item)
                    else []
                )
                if skip_google_meeting_items and self._is_explicit_online_meeting(action_item):
                    serialized_item["google_calendar_status"] = "skipped_shared_team_meeting_event"
                    serialized_item["google_calendar_error"] = None
                elif not self.google_calendar_client:
                    serialized_item["google_calendar_status"] = "skipped_missing_configuration"
                    serialized_item["google_calendar_error"] = (
                        "GOOGLE_CALENDAR_API_TOKEN is missing."
                    )
                else:
                    try:
                        event_id, google_meet_link = self._create_google_calendar_event(
                            item=action_item,
                            meeting_id=meeting_id,
                            attendee_emails=attendee_emails_for_calendar,
                        )
                    except GoogleCalendarError as exc:
                        serialized_item["google_calendar_status"] = "failed"
                        serialized_item["google_calendar_error"] = str(exc)
                    else:
                        serialized_item["google_calendar_event_id"] = event_id
                        serialized_item["google_meet_link"] = google_meet_link
                        if self._requires_google_meet_link(action_item) and not google_meet_link:
                            serialized_item["google_calendar_status"] = "failed_missing_meet_link"
                            serialized_item["google_calendar_error"] = (
                                "Google Calendar event was created without Google Meet link."
                            )
                        else:
                            serialized_item["google_calendar_status"] = "created"
                            google_calendar_created_count += 1

                if skip_outlook_meeting_items and self._is_explicit_online_meeting(action_item):
                    serialized_item["outlook_calendar_status"] = "skipped_shared_team_meeting_event"
                    serialized_item["outlook_calendar_error"] = None
                elif not self.outlook_calendar_client:
                    serialized_item["outlook_calendar_status"] = "skipped_missing_configuration"
                    serialized_item["outlook_calendar_error"] = (
                        "OUTLOOK_CALENDAR_API_TOKEN is missing."
                    )
                else:
                    try:
                        event_id, teams_join_url = self._create_outlook_calendar_event(
                            item=action_item,
                            meeting_id=meeting_id,
                            attendee_emails=attendee_emails_for_calendar,
                        )
                    except OutlookCalendarError as exc:
                        serialized_item["outlook_calendar_status"] = "failed"
                        serialized_item["outlook_calendar_error"] = str(exc)
                    else:
                        serialized_item["outlook_calendar_event_id"] = event_id
                        serialized_item["outlook_teams_link"] = teams_join_url
                        if self._requires_teams_link(action_item) and not teams_join_url:
                            serialized_item["outlook_calendar_status"] = "failed_missing_teams_link"
                            serialized_item["outlook_calendar_error"] = (
                                "Outlook Calendar event was created without Microsoft Teams link."
                            )
                        else:
                            serialized_item["outlook_calendar_status"] = "created"
                            outlook_calendar_created_count += 1

            serialized_items.append(serialized_item)

        status = "completed"
        error = None
        if created_count == 0:
            status = "failed_notes_sync"
            error = f"No action item could be created in {notes_outputs_description}."
        elif created_count < len(action_items):
            status = "completed_with_errors"
            error = f"Some action items could not be created in {notes_outputs_description}."

        monday_status, monday_error = self._summarize_monday_sync(
            action_items=action_items,
            serialized_items=serialized_items,
            created_count=monday_created_count,
        )
        calendar_status, calendar_error = self._summarize_calendar_sync(
            action_items=action_items,
            serialized_items=serialized_items,
            created_count=google_calendar_created_count,
        )
        outlook_calendar_status, outlook_calendar_error = self._summarize_outlook_calendar_sync(
            action_items=action_items,
            serialized_items=serialized_items,
            created_count=outlook_calendar_created_count,
        )

        if calendar_status in {"failed_sync", "completed_with_errors"} and status == "completed":
            status = "completed_with_errors"
            error = "Some action items could not be synced to Google Calendar."
        if (
            outlook_calendar_status in {"failed_sync", "completed_with_errors"}
            and status == "completed"
        ):
            status = "completed_with_errors"
            error = "Some action items could not be synced to Outlook Calendar."
        if monday_status in {"failed_sync", "completed_with_errors"} and status == "completed":
            status = "completed_with_errors"
            error = "Some action items could not be synced to Monday."

        return self._build_result(
            status=status,
            extracted_count=len(action_items),
            created_count=created_count,
            monday_status=monday_status,
            monday_created_count=monday_created_count,
            monday_error=monday_error,
            google_calendar_status=calendar_status,
            google_calendar_created_count=google_calendar_created_count,
            google_calendar_error=calendar_error,
            outlook_calendar_status=outlook_calendar_status,
            outlook_calendar_created_count=outlook_calendar_created_count,
            outlook_calendar_error=outlook_calendar_error,
            items=serialized_items,
            error=error,
        )

    def _build_test_action_items(
        self,
        *,
        meeting_id: str | None,
        transcript_text: str,
        transcript_sentences: list[dict[str, Any]],
        participant_emails: list[str],
    ) -> list[ActionItem]:
        assignee_email = participant_emails[0] if participant_emails else None
        source_sentence = None
        for sentence in transcript_sentences:
            if not isinstance(sentence, Mapping):
                continue
            raw_text = sentence.get("text")
            if not isinstance(raw_text, str):
                continue
            cleaned = raw_text.strip()
            if cleaned:
                source_sentence = cleaned
                break

        context_meeting_id = (meeting_id or "sin-meeting-id").strip() or "sin-meeting-id"
        summary = transcript_text.strip().replace("\n", " ")
        if len(summary) > 240:
            summary = f"{summary[:237]}..."

        return [
            ActionItem(
                title=f"[TEST] Revisar acuerdos - {context_meeting_id}",
                assignee_email=assignee_email,
                assignee_name=None,
                due_date=None,
                details=f"Tarea sintetica para validar sync a Notion. Contexto: {summary}",
                source_sentence=source_sentence,
            ),
        ]

    def _describe_enabled_notes_outputs(self) -> str:
        outputs: list[str] = []
        if self.notion_client:
            outputs.append("Notion")
        if self.monday_kanban_client:
            outputs.append("Monday")
        if not outputs:
            return "configured notes outputs"
        if len(outputs) == 1:
            return outputs[0]
        return ", ".join(outputs[:-1]) + f" and {outputs[-1]}"

    def _create_gemini_client(self) -> GeminiActionItemsClient | None:
        if not self.settings.gemini_api_key.strip():
            return None
        model = self.settings.gemini_model.strip()
        if not model:
            return None
        return GeminiActionItemsClient(
            api_key=self.settings.gemini_api_key,
            model=model,
            timeout_seconds=self.settings.gemini_api_timeout_seconds,
        )

    def _create_notion_client(self) -> NotionKanbanClient | None:
        api_token = self.settings.notion_api_token.strip()
        database_id = self.settings.notion_tasks_database_id.strip()
        if not api_token or not database_id:
            return None
        return NotionKanbanClient(
            api_token=api_token,
            database_id=database_id,
            timeout_seconds=self.settings.notion_api_timeout_seconds,
            api_version=self.settings.notion_api_version,
            todo_status_name=self.settings.notion_kanban_todo_status,
            title_property=self.settings.notion_task_title_property,
            assignee_property=self.settings.notion_task_assignee_property,
            status_property=self.settings.notion_task_status_property,
            due_date_property=self.settings.notion_task_due_date_property,
            details_property=self.settings.notion_task_details_property,
            meeting_id_property=self.settings.notion_task_meeting_id_property,
        )

    def _create_monday_kanban_client(self) -> MondayKanbanClient | None:
        api_token = self.settings.monday_api_token.strip()
        board_id = self.settings.monday_board_id.strip()
        group_id = self.settings.monday_group_id.strip()
        if not api_token or not board_id or not group_id:
            return None
        return MondayKanbanClient(
            api_token=api_token,
            board_id=board_id,
            group_id=group_id,
            timeout_seconds=self.settings.monday_api_timeout_seconds,
            status_column_id=self.settings.monday_status_column_id,
            todo_status_label=self.settings.monday_kanban_todo_status,
            assignee_column_id=self.settings.monday_assignee_column_id,
            due_date_column_id=self.settings.monday_due_date_column_id,
            details_column_id=self.settings.monday_details_column_id,
            meeting_id_column_id=self.settings.monday_meeting_id_column_id,
            api_base_url=self.settings.monday_api_url,
        )

    def _create_google_calendar_client(self) -> GoogleCalendarClient | None:
        access_token = self.settings.google_calendar_api_token.strip()
        refresh_token = self.settings.google_calendar_refresh_token.strip()
        if not access_token and not refresh_token:
            return None
        return GoogleCalendarClient(
            access_token=access_token,
            refresh_token=refresh_token,
            client_id=self.settings.google_calendar_client_id,
            client_secret=self.settings.google_calendar_client_secret,
            calendar_id=self.settings.google_calendar_id,
            timeout_seconds=self.settings.google_calendar_api_timeout_seconds,
            default_timezone=self.settings.google_calendar_event_timezone,
        )

    def _create_outlook_calendar_client(self) -> OutlookCalendarClient | None:
        access_token = self.settings.outlook_calendar_api_token.strip()
        refresh_token = self.settings.outlook_calendar_refresh_token.strip()
        if not access_token and not refresh_token:
            return None
        return OutlookCalendarClient(
            access_token=access_token,
            refresh_token=refresh_token,
            client_id=self.settings.outlook_client_id,
            client_secret=self.settings.outlook_client_secret,
            tenant_id=self.settings.outlook_tenant_id,
            timeout_seconds=self.settings.google_calendar_api_timeout_seconds,
            default_timezone=self.settings.outlook_calendar_event_timezone,
        )

    def _create_google_calendar_event(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
        attendee_emails: list[str],
    ) -> tuple[str, str | None]:
        if not self.google_calendar_client:
            raise GoogleCalendarError("GOOGLE_CALENDAR_API_TOKEN is missing.")

        create_with_details = getattr(
            self.google_calendar_client,
            "create_due_date_event_with_details",
            None,
        )
        if callable(create_with_details):
            try:
                raw_details = create_with_details(
                    item=item,
                    meeting_id=meeting_id,
                    attendee_emails=attendee_emails,
                )
            except TypeError:
                raw_details = create_with_details(item=item, meeting_id=meeting_id)
            if isinstance(raw_details, Mapping):
                event_id = str(raw_details.get("event_id") or "").strip()
                if not event_id:
                    raise GoogleCalendarError("Google Calendar create event response missing id.")
                meet_link = self._normalize_calendar_link(raw_details.get("google_meet_link"))
                return event_id, meet_link

        try:
            event_id = self.google_calendar_client.create_due_date_event(
                item=item,
                meeting_id=meeting_id,
                attendee_emails=attendee_emails,
            )
        except TypeError:
            event_id = self.google_calendar_client.create_due_date_event(
                item=item,
                meeting_id=meeting_id,
            )
        if not isinstance(event_id, str) or not event_id.strip():
            raise GoogleCalendarError("Google Calendar create event response missing id.")
        meet_link = None
        get_meet_link = getattr(self.google_calendar_client, "get_event_google_meet_link", None)
        if callable(get_meet_link):
            try:
                meet_link = self._normalize_calendar_link(get_meet_link(event_id))
            except GoogleCalendarError:
                meet_link = None
        return event_id, meet_link

    def _create_outlook_calendar_event(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
        attendee_emails: list[str],
    ) -> tuple[str, str | None]:
        if not self.outlook_calendar_client:
            raise OutlookCalendarError("OUTLOOK_CALENDAR_API_TOKEN is missing.")

        create_with_details = getattr(
            self.outlook_calendar_client,
            "create_due_date_event_with_details",
            None,
        )
        if callable(create_with_details):
            try:
                raw_details = create_with_details(
                    item=item,
                    meeting_id=meeting_id,
                    attendee_emails=attendee_emails,
                )
            except TypeError:
                raw_details = create_with_details(item=item, meeting_id=meeting_id)
            if isinstance(raw_details, Mapping):
                event_id = str(raw_details.get("event_id") or "").strip()
                if not event_id:
                    raise OutlookCalendarError("Outlook Calendar create event response missing id.")
                teams_link = self._normalize_calendar_link(raw_details.get("teams_join_url"))
                return event_id, teams_link

        try:
            event_id = self.outlook_calendar_client.create_due_date_event(
                item=item,
                meeting_id=meeting_id,
                attendee_emails=attendee_emails,
            )
        except TypeError:
            event_id = self.outlook_calendar_client.create_due_date_event(
                item=item,
                meeting_id=meeting_id,
            )
        if not isinstance(event_id, str) or not event_id.strip():
            raise OutlookCalendarError("Outlook Calendar create event response missing id.")
        teams_link = None
        get_teams_link = getattr(self.outlook_calendar_client, "get_event_teams_join_url", None)
        if callable(get_teams_link):
            try:
                teams_link = self._normalize_calendar_link(get_teams_link(event_id))
            except OutlookCalendarError:
                teams_link = None
        return event_id, teams_link

    def _requires_google_meet_link(self, item: ActionItem) -> bool:
        platform = (item.online_meeting_platform or "").strip().lower()
        return platform == "google_meet"

    def _requires_teams_link(self, item: ActionItem) -> bool:
        platform = (item.online_meeting_platform or "").strip().lower()
        return platform == "microsoft_teams"

    def _is_explicit_online_meeting(self, item: ActionItem) -> bool:
        platform = (item.online_meeting_platform or "").strip().lower()
        return platform in {"google_meet", "microsoft_teams"}

    def _normalize_calendar_link(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        return cleaned or None

    def _summarize_calendar_sync(
        self,
        *,
        action_items: list[Any],
        serialized_items: list[dict[str, Any]],
        created_count: int,
    ) -> tuple[str, str | None]:
        schedulable_items_count = sum(
            1
            for item in action_items
            if hasattr(item, "has_calendar_schedule") and item.has_calendar_schedule()
        )
        if schedulable_items_count == 0:
            return "not_required_no_due_dates", None
        if not self.google_calendar_client:
            return "skipped_missing_configuration", "GOOGLE_CALENDAR_API_TOKEN is missing."

        failed_count = 0
        shared_skip_count = 0
        for item in serialized_items:
            status = item.get("google_calendar_status")
            if isinstance(status, str) and status.startswith("failed"):
                failed_count += 1
            if status == "skipped_shared_team_meeting_event":
                shared_skip_count += 1

        if created_count == 0:
            if failed_count == 0 and shared_skip_count > 0:
                return "shared_from_team_event", None
            return "failed_sync", "No due-date action item could be created in Google Calendar."
        if failed_count > 0:
            return "completed_with_errors", "Some due-date action items could not be synced."
        return "completed", None

    def _summarize_outlook_calendar_sync(
        self,
        *,
        action_items: list[Any],
        serialized_items: list[dict[str, Any]],
        created_count: int,
    ) -> tuple[str, str | None]:
        schedulable_items_count = sum(
            1
            for item in action_items
            if hasattr(item, "has_calendar_schedule") and item.has_calendar_schedule()
        )
        if schedulable_items_count == 0:
            return "not_required_no_due_dates", None
        if not self.outlook_calendar_client:
            return "skipped_missing_configuration", "OUTLOOK_CALENDAR_API_TOKEN is missing."

        failed_count = 0
        shared_skip_count = 0
        for item in serialized_items:
            status = item.get("outlook_calendar_status")
            if isinstance(status, str) and status.startswith("failed"):
                failed_count += 1
            if status == "skipped_shared_team_meeting_event":
                shared_skip_count += 1

        if created_count == 0:
            if failed_count == 0 and shared_skip_count > 0:
                return "shared_from_team_event", None
            return "failed_sync", "No due-date action item could be created in Outlook Calendar."
        if failed_count > 0:
            return "completed_with_errors", "Some due-date action items could not be synced."
        return "completed", None

    def _summarize_monday_sync(
        self,
        *,
        action_items: list[Any],
        serialized_items: list[dict[str, Any]],
        created_count: int,
    ) -> tuple[str, str | None]:
        if not action_items:
            return "not_required_no_action_items", None
        if not self.monday_kanban_client:
            return "skipped_missing_configuration", "MONDAY_API_TOKEN, MONDAY_BOARD_ID or MONDAY_GROUP_ID is missing."

        failed_count = 0
        for item in serialized_items:
            status = item.get("monday_status")
            if status == "failed":
                failed_count += 1

        if created_count == 0:
            return "failed_sync", "No action item could be created in Monday."
        if failed_count > 0:
            return "completed_with_errors", "Some action items could not be synced to Monday."
        return "completed", None

    def _apply_test_due_date(self, action_items: list[Any]) -> list[Any]:
        raw_due_date = self.settings.action_items_test_due_date.strip()
        if not raw_due_date:
            return action_items
        try:
            normalized_due_date = date.fromisoformat(raw_due_date).isoformat()
        except ValueError:
            return action_items

        for action_item in action_items:
            if getattr(action_item, "due_date", None):
                continue
            action_item.due_date = normalized_due_date
        return action_items

    def _build_result(
        self,
        *,
        status: str,
        extracted_count: int,
        created_count: int,
        monday_status: str,
        monday_created_count: int,
        monday_error: str | None,
        google_calendar_status: str,
        google_calendar_created_count: int,
        google_calendar_error: str | None,
        outlook_calendar_status: str,
        outlook_calendar_created_count: int,
        outlook_calendar_error: str | None,
        items: list[dict[str, Any]],
        error: str | None,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "extracted_count": extracted_count,
            "created_count": created_count,
            "monday_status": monday_status,
            "monday_created_count": monday_created_count,
            "monday_error": monday_error,
            "google_calendar_status": google_calendar_status,
            "google_calendar_created_count": google_calendar_created_count,
            "google_calendar_error": google_calendar_error,
            "outlook_calendar_status": outlook_calendar_status,
            "outlook_calendar_created_count": outlook_calendar_created_count,
            "outlook_calendar_error": outlook_calendar_error,
            "items": items,
            "error": error,
            "synced_at": datetime.now(UTC),
        }


def extract_sentences_for_action_items(
    fireflies_transcript: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if not fireflies_transcript:
        return []
    raw_sentences = fireflies_transcript.get("sentences")
    if not isinstance(raw_sentences, list):
        return []

    sentences: list[dict[str, Any]] = []
    for raw_sentence in raw_sentences:
        if not isinstance(raw_sentence, Mapping):
            continue
        text = raw_sentence.get("text")
        if not isinstance(text, str):
            continue
        cleaned_text = text.strip()
        if not cleaned_text:
            continue
        sentence: dict[str, Any] = {"text": cleaned_text}
        speaker_name = raw_sentence.get("speaker_name")
        if isinstance(speaker_name, str) and speaker_name.strip():
            sentence["speaker_name"] = speaker_name.strip()
        start_time = raw_sentence.get("start_time")
        if isinstance(start_time, int | float):
            sentence["start_time"] = float(start_time)
        sentences.append(sentence)
    return sentences


def sanitize_action_item_participants(participant_emails: list[str]) -> list[str]:
    unique: set[str] = set()
    for email in participant_emails:
        cleaned = email.strip().lower()
        if not cleaned or "@" not in cleaned:
            continue
        unique.add(cleaned)
    return sorted(unique)
