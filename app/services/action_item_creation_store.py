from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any


class ActionItemCreationStore(ABC):
    @abstractmethod
    def save_many(self, records: Sequence[Mapping[str, Any]]) -> int:
        raise NotImplementedError

    @abstractmethod
    def list_recent(
        self,
        *,
        limit: int,
        meeting_id: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError


class InMemoryActionItemCreationStore(ActionItemCreationStore):
    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def save_many(self, records: Sequence[Mapping[str, Any]]) -> int:
        inserted_count = 0
        for raw_record in records:
            record = dict(raw_record)
            record["_id"] = f"memory-action-item-creation-{len(self._records) + 1}"
            if "created_at" not in record:
                record["created_at"] = datetime.now(UTC)
            self._records.append(record)
            inserted_count += 1
        return inserted_count

    def list_recent(
        self,
        *,
        limit: int,
        meeting_id: str | None = None,
    ) -> list[dict[str, Any]]:
        items = self._records
        if meeting_id:
            items = [record for record in items if record.get("meeting_id") == meeting_id]
        return list(reversed(items[-limit:]))


class MongoActionItemCreationStore(ActionItemCreationStore):
    def __init__(
        self,
        *,
        uri: str,
        db_name: str,
        collection_name: str,
        connect_timeout_ms: int = 2000,
    ) -> None:
        from pymongo import DESCENDING, MongoClient

        self._desc = DESCENDING
        self._client = MongoClient(
            uri,
            serverSelectionTimeoutMS=connect_timeout_ms,
            connectTimeoutMS=connect_timeout_ms,
        )
        self._collection = self._client[db_name][collection_name]
        self._collection.create_index([("created_at", self._desc)])
        self._collection.create_index([("meeting_id", self._desc), ("created_at", self._desc)])
        self._collection.create_index([("notion_page_id", self._desc)])

    def save_many(self, records: Sequence[Mapping[str, Any]]) -> int:
        payload = [dict(record) for record in records]
        if not payload:
            return 0
        insert_result = self._collection.insert_many(payload)
        return len(insert_result.inserted_ids)

    def list_recent(
        self,
        *,
        limit: int,
        meeting_id: str | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if meeting_id:
            query["meeting_id"] = meeting_id
        cursor = self._collection.find(query).sort("created_at", self._desc).limit(limit)
        return list(cursor)


def create_action_item_creation_store(
    *,
    store_name: str,
    mongodb_uri: str,
    mongodb_db_name: str,
    mongodb_collection_name: str,
    mongodb_connect_timeout_ms: int,
) -> ActionItemCreationStore:
    return _create_action_item_creation_store_cached(
        store_name=store_name,
        mongodb_uri=mongodb_uri,
        mongodb_db_name=mongodb_db_name,
        mongodb_collection_name=mongodb_collection_name,
        mongodb_connect_timeout_ms=mongodb_connect_timeout_ms,
    )


@lru_cache
def _create_action_item_creation_store_cached(
    *,
    store_name: str,
    mongodb_uri: str,
    mongodb_db_name: str,
    mongodb_collection_name: str,
    mongodb_connect_timeout_ms: int,
) -> ActionItemCreationStore:
    if store_name == "memory":
        return InMemoryActionItemCreationStore()

    if store_name == "mongodb":
        return MongoActionItemCreationStore(
            uri=mongodb_uri,
            db_name=mongodb_db_name,
            collection_name=mongodb_collection_name,
            connect_timeout_ms=mongodb_connect_timeout_ms,
        )

    return InMemoryActionItemCreationStore()


def clear_action_item_creation_store_cache() -> None:
    _create_action_item_creation_store_cached.cache_clear()


def build_action_item_creation_record(
    *,
    source: str,
    provider: str,
    meeting_id: str | None,
    transcript_id: str | None,
    client_reference_id: str | None,
    transcription_record_id: str | None,
    action_item_index: int,
    action_item: Mapping[str, Any],
    participant_emails: Sequence[str],
    synced_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "provider": provider,
        "meeting_id": meeting_id,
        "transcript_id": transcript_id,
        "client_reference_id": client_reference_id,
        "transcription_record_id": transcription_record_id,
        "action_item_index": action_item_index,
        "title": action_item.get("title"),
        "assignee_email": action_item.get("assignee_email"),
        "assignee_name": action_item.get("assignee_name"),
        "due_date": action_item.get("due_date"),
        "details": action_item.get("details"),
        "source_sentence": action_item.get("source_sentence"),
        "notion_status": action_item.get("status"),
        "notion_page_id": action_item.get("notion_page_id"),
        "notion_error": action_item.get("error"),
        "google_calendar_status": action_item.get("google_calendar_status"),
        "google_calendar_event_id": action_item.get("google_calendar_event_id"),
        "google_calendar_error": action_item.get("google_calendar_error"),
        "outlook_calendar_status": action_item.get("outlook_calendar_status"),
        "outlook_calendar_event_id": action_item.get("outlook_calendar_event_id"),
        "outlook_calendar_error": action_item.get("outlook_calendar_error"),
        "participant_emails": list(participant_emails),
        "synced_at": synced_at or datetime.now(UTC),
        "created_at": datetime.now(UTC),
    }
