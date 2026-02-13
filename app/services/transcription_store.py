from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any


class TranscriptionStore(ABC):
    @abstractmethod
    def save(self, record: Mapping[str, Any]) -> str:
        raise NotImplementedError

    @abstractmethod
    def list_recent(self, limit: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_by_id(self, record_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def get_latest_by_meeting_id(self, meeting_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def update_by_meeting_id(self, meeting_id: str, updates: Mapping[str, Any]) -> int:
        raise NotImplementedError


class InMemoryTranscriptionStore(TranscriptionStore):
    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def save(self, record: Mapping[str, Any]) -> str:
        record_id = f"memory-{len(self._records) + 1}"
        stored_record = dict(record)
        stored_record["_id"] = record_id
        self._records.append(stored_record)
        return record_id

    def list_recent(self, limit: int) -> list[dict[str, Any]]:
        return list(reversed(self._records[-limit:]))

    def get_by_id(self, record_id: str) -> dict[str, Any] | None:
        for record in self._records:
            if str(record.get("_id")) == record_id:
                return record
        return None

    def get_latest_by_meeting_id(self, meeting_id: str) -> dict[str, Any] | None:
        for record in reversed(self._records):
            if record.get("meeting_id") == meeting_id:
                return record
        return None

    def update_by_meeting_id(self, meeting_id: str, updates: Mapping[str, Any]) -> int:
        updated_count = 0
        for record in self._records:
            if record.get("meeting_id") != meeting_id:
                continue
            record.update(dict(updates))
            updated_count += 1
        return updated_count


class MongoTranscriptionStore(TranscriptionStore):
    def __init__(
        self,
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
        self._collection.create_index([("received_at", self._desc)])
        self._collection.create_index([("meeting_id", self._desc), ("received_at", self._desc)])

    def save(self, record: Mapping[str, Any]) -> str:
        insert_result = self._collection.insert_one(dict(record))
        return str(insert_result.inserted_id)

    def list_recent(self, limit: int) -> list[dict[str, Any]]:
        cursor = self._collection.find().sort("received_at", self._desc).limit(limit)
        return list(cursor)

    def get_by_id(self, record_id: str) -> dict[str, Any] | None:
        from bson import ObjectId
        from bson.errors import InvalidId

        try:
            object_id = ObjectId(record_id)
        except InvalidId:
            return None
        return self._collection.find_one({"_id": object_id})

    def get_latest_by_meeting_id(self, meeting_id: str) -> dict[str, Any] | None:
        return self._collection.find_one(
            {"meeting_id": meeting_id},
            sort=[("received_at", self._desc)],
        )

    def update_by_meeting_id(self, meeting_id: str, updates: Mapping[str, Any]) -> int:
        result = self._collection.update_many(
            {"meeting_id": meeting_id},
            {"$set": dict(updates)},
        )
        return int(result.matched_count)


def create_transcription_store(
    store_name: str,
    mongodb_uri: str,
    mongodb_db_name: str,
    mongodb_collection_name: str,
    mongodb_connect_timeout_ms: int,
) -> TranscriptionStore:
    return _create_transcription_store_cached(
        store_name=store_name,
        mongodb_uri=mongodb_uri,
        mongodb_db_name=mongodb_db_name,
        mongodb_collection_name=mongodb_collection_name,
        mongodb_connect_timeout_ms=mongodb_connect_timeout_ms,
    )


@lru_cache
def _create_transcription_store_cached(
    store_name: str,
    mongodb_uri: str,
    mongodb_db_name: str,
    mongodb_collection_name: str,
    mongodb_connect_timeout_ms: int,
) -> TranscriptionStore:
    if store_name == "memory":
        return InMemoryTranscriptionStore()

    if store_name == "mongodb":
        return MongoTranscriptionStore(
            uri=mongodb_uri,
            db_name=mongodb_db_name,
            collection_name=mongodb_collection_name,
            connect_timeout_ms=mongodb_connect_timeout_ms,
        )

    # Safety fallback to keep service operational with unknown values.
    return InMemoryTranscriptionStore()


def clear_transcription_store_cache() -> None:
    _create_transcription_store_cached.cache_clear()


def build_transcription_document(
    *,
    provider: str,
    event_type: str | None,
    meeting_id: str | None,
    client_reference_id: str | None,
    transcript_id: str | None,
    meeting_platform: str | None,
    is_google_meet: bool,
    transcript_text_available: bool,
    transcript_text: str | None,
    enrichment_status: str,
    enrichment_error: str | None,
    action_items_sync: Mapping[str, Any] | None,
    fireflies_transcript: Mapping[str, Any] | None,
    raw_payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "provider": provider,
        "event_type": event_type,
        "meeting_id": meeting_id,
        "client_reference_id": client_reference_id,
        "transcript_id": transcript_id,
        "meeting_platform": meeting_platform,
        "is_google_meet": is_google_meet,
        "transcript_text_available": transcript_text_available,
        "transcript_text": transcript_text,
        "enrichment_status": enrichment_status,
        "enrichment_error": enrichment_error,
        "action_items_sync": dict(action_items_sync) if action_items_sync else None,
        "fireflies_transcript": dict(fireflies_transcript) if fireflies_transcript else None,
        "raw_payload": dict(raw_payload),
        "received_at": datetime.now(UTC),
    }
