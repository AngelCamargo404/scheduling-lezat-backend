from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from app.core.config import Settings
from app.services.gemini_action_items_client import GeminiActionItemsClient, GeminiActionItemsError
from app.services.google_calendar_client import GoogleCalendarClient, GoogleCalendarError
from app.services.notion_kanban_client import NotionKanbanClient, NotionKanbanError
from app.services.outlook_calendar_client import OutlookCalendarClient, OutlookCalendarError


class ActionItemSyncService:
    def __init__(
        self,
        settings: Settings,
        gemini_client: GeminiActionItemsClient | None = None,
        notion_client: NotionKanbanClient | None = None,
        google_calendar_client: GoogleCalendarClient | None = None,
        outlook_calendar_client: OutlookCalendarClient | None = None,
    ) -> None:
        self.settings = settings
        self.gemini_client = gemini_client or self._create_gemini_client()
        self.notion_client = notion_client or self._create_notion_client()
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
    ) -> dict[str, Any]:
        if not transcript_text:
            return self._build_result(
                status="skipped_no_transcript",
                extracted_count=0,
                created_count=0,
                google_calendar_status="not_required_no_due_dates",
                google_calendar_created_count=0,
                google_calendar_error=None,
                outlook_calendar_status="not_required_no_due_dates",
                outlook_calendar_created_count=0,
                outlook_calendar_error=None,
                items=[],
                error=None,
            )
        if not self.gemini_client or not self.notion_client:
            return self._build_result(
                status="skipped_missing_configuration",
                extracted_count=0,
                created_count=0,
                google_calendar_status="not_required_no_due_dates",
                google_calendar_created_count=0,
                google_calendar_error=None,
                outlook_calendar_status="not_required_no_due_dates",
                outlook_calendar_created_count=0,
                outlook_calendar_error=None,
                items=[],
                error="GEMINI_API_KEY, NOTION_API_TOKEN or NOTION_TASKS_DATABASE_ID is missing.",
            )

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
        google_calendar_created_count = 0
        outlook_calendar_created_count = 0
        serialized_items: list[dict[str, Any]] = []
        for action_item in action_items:
            serialized_item = action_item.to_dict()
            serialized_item["notion_page_id"] = None
            serialized_item["google_calendar_event_id"] = None
            serialized_item["outlook_calendar_event_id"] = None
            serialized_item["status"] = "pending"
            serialized_item["error"] = None
            serialized_item["google_calendar_status"] = "not_required_no_due_date"
            serialized_item["google_calendar_error"] = None
            serialized_item["outlook_calendar_status"] = "not_required_no_due_date"
            serialized_item["outlook_calendar_error"] = None
            try:
                page_id = self.notion_client.create_kanban_task(
                    item=action_item,
                    meeting_id=meeting_id,
                )
            except NotionKanbanError as exc:
                serialized_item["status"] = "failed"
                serialized_item["error"] = str(exc)
                serialized_items.append(serialized_item)
                continue

            created_count += 1
            serialized_item["notion_page_id"] = page_id
            serialized_item["status"] = "created"

            if action_item.due_date:
                if not self.google_calendar_client:
                    serialized_item["google_calendar_status"] = "skipped_missing_configuration"
                    serialized_item["google_calendar_error"] = (
                        "GOOGLE_CALENDAR_API_TOKEN is missing."
                    )
                else:
                    try:
                        event_id = self.google_calendar_client.create_due_date_event(
                            item=action_item,
                            meeting_id=meeting_id,
                        )
                    except GoogleCalendarError as exc:
                        serialized_item["google_calendar_status"] = "failed"
                        serialized_item["google_calendar_error"] = str(exc)
                    else:
                        serialized_item["google_calendar_status"] = "created"
                        serialized_item["google_calendar_event_id"] = event_id
                        google_calendar_created_count += 1

                if not self.outlook_calendar_client:
                    serialized_item["outlook_calendar_status"] = "skipped_missing_configuration"
                    serialized_item["outlook_calendar_error"] = (
                        "OUTLOOK_CALENDAR_API_TOKEN is missing."
                    )
                else:
                    try:
                        event_id = self.outlook_calendar_client.create_due_date_event(
                            item=action_item,
                            meeting_id=meeting_id,
                        )
                    except OutlookCalendarError as exc:
                        serialized_item["outlook_calendar_status"] = "failed"
                        serialized_item["outlook_calendar_error"] = str(exc)
                    else:
                        serialized_item["outlook_calendar_status"] = "created"
                        serialized_item["outlook_calendar_event_id"] = event_id
                        outlook_calendar_created_count += 1
            serialized_items.append(serialized_item)

        status = "completed"
        error = None
        if created_count == 0:
            status = "failed_notion_sync"
            error = "No action item could be created in Notion."
        elif created_count < len(action_items):
            status = "completed_with_errors"
            error = "Some action items could not be created in Notion."

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

        return self._build_result(
            status=status,
            extracted_count=len(action_items),
            created_count=created_count,
            google_calendar_status=calendar_status,
            google_calendar_created_count=google_calendar_created_count,
            google_calendar_error=calendar_error,
            outlook_calendar_status=outlook_calendar_status,
            outlook_calendar_created_count=outlook_calendar_created_count,
            outlook_calendar_error=outlook_calendar_error,
            items=serialized_items,
            error=error,
        )

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
        if not access_token:
            return None
        return OutlookCalendarClient(
            access_token=access_token,
            timeout_seconds=self.settings.google_calendar_api_timeout_seconds,
            default_timezone=self.settings.outlook_calendar_event_timezone,
        )

    def _summarize_calendar_sync(
        self,
        *,
        action_items: list[Any],
        serialized_items: list[dict[str, Any]],
        created_count: int,
    ) -> tuple[str, str | None]:
        due_date_items_count = sum(1 for item in action_items if getattr(item, "due_date", None))
        if due_date_items_count == 0:
            return "not_required_no_due_dates", None
        if not self.google_calendar_client:
            return "skipped_missing_configuration", "GOOGLE_CALENDAR_API_TOKEN is missing."

        failed_count = 0
        for item in serialized_items:
            status = item.get("google_calendar_status")
            if status == "failed":
                failed_count += 1

        if created_count == 0:
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
        due_date_items_count = sum(1 for item in action_items if getattr(item, "due_date", None))
        if due_date_items_count == 0:
            return "not_required_no_due_dates", None
        if not self.outlook_calendar_client:
            return "skipped_missing_configuration", "OUTLOOK_CALENDAR_API_TOKEN is missing."

        failed_count = 0
        for item in serialized_items:
            status = item.get("outlook_calendar_status")
            if status == "failed":
                failed_count += 1

        if created_count == 0:
            return "failed_sync", "No due-date action item could be created in Outlook Calendar."
        if failed_count > 0:
            return "completed_with_errors", "Some due-date action items could not be synced."
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
