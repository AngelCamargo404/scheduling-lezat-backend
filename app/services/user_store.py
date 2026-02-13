from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

from app.core.config import Settings


class UserStore(ABC):
    @abstractmethod
    def get_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def create_user(
        self,
        *,
        email: str,
        full_name: str,
        password_hash: str,
        role: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_user_settings_values(self, user_id: str) -> dict[str, str]:
        raise NotImplementedError

    @abstractmethod
    def upsert_user_settings_values(self, user_id: str, updates: Mapping[str, str]) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def has_user_settings(self, user_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def replace_user_settings_values(self, user_id: str, values: Mapping[str, str]) -> None:
        raise NotImplementedError


class InMemoryUserStore(UserStore):
    def __init__(self) -> None:
        self._next_id = 1
        self._users_by_id: dict[str, dict[str, Any]] = {}
        self._user_id_by_email: dict[str, str] = {}
        self._settings_by_user_id: dict[str, dict[str, str]] = {}

    def get_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        user = self._users_by_id.get(user_id)
        if not user:
            return None
        return dict(user)

    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        normalized_email = _normalize_email(email)
        user_id = self._user_id_by_email.get(normalized_email)
        if not user_id:
            return None
        return self.get_user_by_id(user_id)

    def create_user(
        self,
        *,
        email: str,
        full_name: str,
        password_hash: str,
        role: str,
    ) -> dict[str, Any]:
        normalized_email = _normalize_email(email)
        if normalized_email in self._user_id_by_email:
            raise ValueError("email_already_exists")

        user_id = str(self._next_id)
        self._next_id += 1
        now = datetime.now(UTC)
        user = {
            "_id": user_id,
            "email": normalized_email,
            "full_name": full_name.strip(),
            "password_hash": password_hash,
            "role": role.strip().lower(),
            "created_at": now,
            "updated_at": now,
        }
        self._users_by_id[user_id] = user
        self._user_id_by_email[normalized_email] = user_id
        return dict(user)

    def get_user_settings_values(self, user_id: str) -> dict[str, str]:
        values = self._settings_by_user_id.get(user_id, {})
        return dict(values)

    def upsert_user_settings_values(self, user_id: str, updates: Mapping[str, str]) -> list[str]:
        current_values = dict(self._settings_by_user_id.get(user_id, {}))
        updated_env_vars: list[str] = []
        for env_var, raw_value in updates.items():
            normalized_value = (raw_value or "").strip()
            if normalized_value:
                current_values[env_var] = normalized_value
            else:
                current_values.pop(env_var, None)
            updated_env_vars.append(env_var)
        self._settings_by_user_id[user_id] = current_values
        return sorted(updated_env_vars)

    def has_user_settings(self, user_id: str) -> bool:
        return user_id in self._settings_by_user_id

    def replace_user_settings_values(self, user_id: str, values: Mapping[str, str]) -> None:
        normalized_values: dict[str, str] = {}
        for env_var, raw_value in values.items():
            cleaned_value = (raw_value or "").strip()
            if cleaned_value:
                normalized_values[env_var] = cleaned_value
        self._settings_by_user_id[user_id] = normalized_values


class MongoUserStore(UserStore):
    def __init__(
        self,
        *,
        uri: str,
        db_name: str,
        users_collection_name: str,
        user_settings_collection_name: str,
        connect_timeout_ms: int = 2000,
    ) -> None:
        from pymongo import MongoClient

        self._client = MongoClient(
            uri,
            serverSelectionTimeoutMS=connect_timeout_ms,
            connectTimeoutMS=connect_timeout_ms,
        )
        database = self._client[db_name]
        self._users = database[users_collection_name]
        self._settings = database[user_settings_collection_name]

        self._users.create_index("email", unique=True)
        self._settings.create_index("user_id", unique=True)

    def get_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        from bson import ObjectId
        from bson.errors import InvalidId

        try:
            object_id = ObjectId(user_id)
        except InvalidId:
            return None
        record = self._users.find_one({"_id": object_id})
        return _serialize_user_record(record)

    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        normalized_email = _normalize_email(email)
        record = self._users.find_one({"email": normalized_email})
        return _serialize_user_record(record)

    def create_user(
        self,
        *,
        email: str,
        full_name: str,
        password_hash: str,
        role: str,
    ) -> dict[str, Any]:
        from pymongo.errors import DuplicateKeyError

        normalized_email = _normalize_email(email)
        now = datetime.now(UTC)
        payload = {
            "email": normalized_email,
            "full_name": full_name.strip(),
            "password_hash": password_hash,
            "role": role.strip().lower(),
            "created_at": now,
            "updated_at": now,
        }
        try:
            insert_result = self._users.insert_one(payload)
        except DuplicateKeyError as exc:
            raise ValueError("email_already_exists") from exc
        created = self._users.find_one({"_id": insert_result.inserted_id})
        serialized = _serialize_user_record(created)
        if not serialized:
            raise RuntimeError("Unable to read created user.")
        return serialized

    def get_user_settings_values(self, user_id: str) -> dict[str, str]:
        record = self._settings.find_one({"user_id": user_id})
        if not record:
            return {}
        raw_values = record.get("values")
        if not isinstance(raw_values, Mapping):
            return {}
        values: dict[str, str] = {}
        for key, value in raw_values.items():
            if not isinstance(key, str):
                continue
            if not isinstance(value, str):
                continue
            normalized_value = value.strip()
            if normalized_value:
                values[key] = normalized_value
        return values

    def upsert_user_settings_values(self, user_id: str, updates: Mapping[str, str]) -> list[str]:
        current_values = self.get_user_settings_values(user_id)
        updated_env_vars: list[str] = []
        for env_var, raw_value in updates.items():
            normalized_value = (raw_value or "").strip()
            if normalized_value:
                current_values[env_var] = normalized_value
            else:
                current_values.pop(env_var, None)
            updated_env_vars.append(env_var)

        now = datetime.now(UTC)
        self._settings.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "values": current_values,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                },
            },
            upsert=True,
        )
        return sorted(updated_env_vars)

    def has_user_settings(self, user_id: str) -> bool:
        return self._settings.find_one({"user_id": user_id}, {"_id": 1}) is not None

    def replace_user_settings_values(self, user_id: str, values: Mapping[str, str]) -> None:
        normalized_values: dict[str, str] = {}
        for env_var, raw_value in values.items():
            normalized_value = (raw_value or "").strip()
            if normalized_value:
                normalized_values[env_var] = normalized_value

        now = datetime.now(UTC)
        self._settings.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "values": normalized_values,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                },
            },
            upsert=True,
        )


def _serialize_user_record(record: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    serialized = dict(record)
    serialized["_id"] = str(record.get("_id", ""))
    return serialized


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def create_user_store(settings: Settings) -> UserStore:
    return _create_user_store_cached(
        user_data_store=settings.user_data_store,
        mongodb_uri=settings.mongodb_uri,
        mongodb_db_name=settings.mongodb_db_name,
        mongodb_users_collection=settings.mongodb_users_collection,
        mongodb_user_settings_collection=settings.mongodb_user_settings_collection,
        mongodb_connect_timeout_ms=settings.mongodb_connect_timeout_ms,
    )


@lru_cache
def _create_user_store_cached(
    *,
    user_data_store: str,
    mongodb_uri: str,
    mongodb_db_name: str,
    mongodb_users_collection: str,
    mongodb_user_settings_collection: str,
    mongodb_connect_timeout_ms: int,
) -> UserStore:
    if user_data_store == "memory":
        return InMemoryUserStore()

    if user_data_store == "mongodb":
        return MongoUserStore(
            uri=mongodb_uri,
            db_name=mongodb_db_name,
            users_collection_name=mongodb_users_collection,
            user_settings_collection_name=mongodb_user_settings_collection,
            connect_timeout_ms=mongodb_connect_timeout_ms,
        )

    return InMemoryUserStore()


def clear_user_store_cache() -> None:
    _create_user_store_cached.cache_clear()
