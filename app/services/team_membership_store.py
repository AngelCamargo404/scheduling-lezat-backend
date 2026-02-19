from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

from app.core.config import Settings


class TeamMembershipStore(ABC):
    @abstractmethod
    def create_team(
        self,
        *,
        name: str,
        created_by_user_id: str,
        recipient_user_ids: list[str],
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_team(self, team_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def set_team_recipients(self, team_id: str, recipient_user_ids: list[str]) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def list_teams_by_ids(self, team_ids: list[str]) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_memberships_for_user(
        self,
        user_id: str,
        *,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_memberships_for_team(
        self,
        team_id: str,
        *,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def upsert_membership(
        self,
        *,
        team_id: str,
        user_id: str,
        role: str,
        status: str,
        invited_by_user_id: str | None = None,
        is_active: bool | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def set_membership_activation(
        self,
        *,
        team_id: str,
        user_id: str,
        is_active: bool,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def create_or_get_pending_invitation(
        self,
        *,
        team_id: str,
        invited_email: str,
        invited_by_user_id: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def list_pending_invitations_for_email(self, invited_email: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_pending_invitations_for_team(self, team_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_invitation(self, invitation_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def update_invitation_status(
        self,
        *,
        invitation_id: str,
        status: str,
        responded_by_user_id: str | None = None,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def list_teams_matching_participants(
        self,
        *,
        participant_user_ids: list[str],
        lead_user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError


class InMemoryTeamMembershipStore(TeamMembershipStore):
    def __init__(self) -> None:
        self._next_team_id = 1
        self._next_membership_id = 1
        self._next_invitation_id = 1

        self._teams_by_id: dict[str, dict[str, Any]] = {}
        self._memberships_by_id: dict[str, dict[str, Any]] = {}
        self._membership_id_by_team_user: dict[tuple[str, str], str] = {}
        self._invitations_by_id: dict[str, dict[str, Any]] = {}

    def create_team(
        self,
        *,
        name: str,
        created_by_user_id: str,
        recipient_user_ids: list[str],
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        team_id = str(self._next_team_id)
        self._next_team_id += 1
        normalized_recipients = sorted(
            {
                user_id.strip()
                for user_id in recipient_user_ids
                if user_id and user_id.strip()
            },
        )
        team = {
            "_id": team_id,
            "name": name.strip(),
            "created_by_user_id": created_by_user_id.strip(),
            "recipient_user_ids": normalized_recipients,
            "created_at": now,
            "updated_at": now,
        }
        self._teams_by_id[team_id] = team
        return dict(team)

    def get_team(self, team_id: str) -> dict[str, Any] | None:
        team = self._teams_by_id.get(team_id)
        if not team:
            return None
        return dict(team)

    def set_team_recipients(self, team_id: str, recipient_user_ids: list[str]) -> dict[str, Any] | None:
        team = self._teams_by_id.get(team_id)
        if not team:
            return None
        normalized_recipients = sorted(
            {
                user_id.strip()
                for user_id in recipient_user_ids
                if user_id and user_id.strip()
            },
        )
        team["recipient_user_ids"] = normalized_recipients
        team["updated_at"] = datetime.now(UTC)
        return dict(team)

    def list_teams_by_ids(self, team_ids: list[str]) -> list[dict[str, Any]]:
        team_by_id = {
            team_id: dict(team)
            for team_id, team in self._teams_by_id.items()
            if team_id in set(team_ids)
        }
        ordered: list[dict[str, Any]] = []
        for team_id in team_ids:
            team = team_by_id.get(team_id)
            if team:
                ordered.append(team)
        return ordered

    def list_memberships_for_user(
        self,
        user_id: str,
        *,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_user_id = user_id.strip()
        memberships: list[dict[str, Any]] = []
        for membership in self._memberships_by_id.values():
            if membership.get("user_id") != normalized_user_id:
                continue
            if status and membership.get("status") != status:
                continue
            normalized_membership = _normalize_membership_record(membership)
            if normalized_membership:
                memberships.append(normalized_membership)
        return memberships

    def list_memberships_for_team(
        self,
        team_id: str,
        *,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_team_id = team_id.strip()
        memberships: list[dict[str, Any]] = []
        for membership in self._memberships_by_id.values():
            if membership.get("team_id") != normalized_team_id:
                continue
            if status and membership.get("status") != status:
                continue
            normalized_membership = _normalize_membership_record(membership)
            if normalized_membership:
                memberships.append(normalized_membership)
        return memberships

    def upsert_membership(
        self,
        *,
        team_id: str,
        user_id: str,
        role: str,
        status: str,
        invited_by_user_id: str | None = None,
        is_active: bool | None = None,
    ) -> dict[str, Any]:
        normalized_team_id = team_id.strip()
        normalized_user_id = user_id.strip()
        now = datetime.now(UTC)
        key = (normalized_team_id, normalized_user_id)
        existing_membership_id = self._membership_id_by_team_user.get(key)
        if existing_membership_id:
            existing = self._memberships_by_id[existing_membership_id]
            existing["role"] = role.strip().lower()
            existing["status"] = status.strip().lower()
            existing["updated_at"] = now
            existing["invited_by_user_id"] = (invited_by_user_id or "").strip() or None
            if is_active is not None:
                existing["is_active"] = bool(is_active)
            elif "is_active" not in existing:
                existing["is_active"] = True
            return _normalize_membership_record(existing) or {}

        membership_id = str(self._next_membership_id)
        self._next_membership_id += 1
        created = {
            "_id": membership_id,
            "team_id": normalized_team_id,
            "user_id": normalized_user_id,
            "role": role.strip().lower(),
            "status": status.strip().lower(),
            "invited_by_user_id": (invited_by_user_id or "").strip() or None,
            "is_active": True if is_active is None else bool(is_active),
            "created_at": now,
            "updated_at": now,
        }
        self._memberships_by_id[membership_id] = created
        self._membership_id_by_team_user[key] = membership_id
        return _normalize_membership_record(created) or {}

    def set_membership_activation(
        self,
        *,
        team_id: str,
        user_id: str,
        is_active: bool,
    ) -> dict[str, Any] | None:
        normalized_team_id = team_id.strip()
        normalized_user_id = user_id.strip()
        membership_id = self._membership_id_by_team_user.get((normalized_team_id, normalized_user_id))
        if not membership_id:
            return None
        membership = self._memberships_by_id.get(membership_id)
        if not membership:
            return None
        membership["is_active"] = bool(is_active)
        membership["updated_at"] = datetime.now(UTC)
        return _normalize_membership_record(membership)

    def create_or_get_pending_invitation(
        self,
        *,
        team_id: str,
        invited_email: str,
        invited_by_user_id: str,
    ) -> dict[str, Any]:
        normalized_team_id = team_id.strip()
        normalized_invited_email = invited_email.strip().lower()
        normalized_invited_by_user_id = invited_by_user_id.strip()
        for invitation in self._invitations_by_id.values():
            if invitation.get("team_id") != normalized_team_id:
                continue
            if invitation.get("invited_email") != normalized_invited_email:
                continue
            if invitation.get("status") != "pending":
                continue
            invitation["invited_by_user_id"] = normalized_invited_by_user_id
            invitation["updated_at"] = datetime.now(UTC)
            return dict(invitation)

        now = datetime.now(UTC)
        invitation_id = str(self._next_invitation_id)
        self._next_invitation_id += 1
        invitation = {
            "_id": invitation_id,
            "team_id": normalized_team_id,
            "invited_email": normalized_invited_email,
            "invited_by_user_id": normalized_invited_by_user_id,
            "status": "pending",
            "responded_by_user_id": None,
            "created_at": now,
            "updated_at": now,
        }
        self._invitations_by_id[invitation_id] = invitation
        return dict(invitation)

    def list_pending_invitations_for_email(self, invited_email: str) -> list[dict[str, Any]]:
        normalized_email = invited_email.strip().lower()
        invitations: list[dict[str, Any]] = []
        for invitation in self._invitations_by_id.values():
            if invitation.get("invited_email") != normalized_email:
                continue
            if invitation.get("status") != "pending":
                continue
            invitations.append(dict(invitation))
        invitations.sort(key=lambda invitation: invitation.get("created_at", datetime.now(UTC)))
        return invitations

    def list_pending_invitations_for_team(self, team_id: str) -> list[dict[str, Any]]:
        normalized_team_id = team_id.strip()
        invitations: list[dict[str, Any]] = []
        for invitation in self._invitations_by_id.values():
            if invitation.get("team_id") != normalized_team_id:
                continue
            if invitation.get("status") != "pending":
                continue
            invitations.append(dict(invitation))
        invitations.sort(key=lambda invitation: invitation.get("created_at", datetime.now(UTC)))
        return invitations

    def get_invitation(self, invitation_id: str) -> dict[str, Any] | None:
        invitation = self._invitations_by_id.get(invitation_id)
        if not invitation:
            return None
        return dict(invitation)

    def update_invitation_status(
        self,
        *,
        invitation_id: str,
        status: str,
        responded_by_user_id: str | None = None,
    ) -> dict[str, Any] | None:
        invitation = self._invitations_by_id.get(invitation_id)
        if not invitation:
            return None
        invitation["status"] = status.strip().lower()
        invitation["responded_by_user_id"] = (responded_by_user_id or "").strip() or None
        invitation["updated_at"] = datetime.now(UTC)
        return dict(invitation)

    def list_teams_matching_participants(
        self,
        *,
        participant_user_ids: list[str],
        lead_user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_participant_user_ids = {
            user_id.strip()
            for user_id in participant_user_ids
            if user_id and user_id.strip()
        }
        if not normalized_participant_user_ids:
            return []

        matched_team_ids: set[str] = set()
        for membership in self._memberships_by_id.values():
            if membership.get("status") != "accepted":
                continue
            user_id = str(membership.get("user_id", "")).strip()
            if user_id not in normalized_participant_user_ids:
                continue
            team_id = str(membership.get("team_id", "")).strip()
            if team_id:
                matched_team_ids.add(team_id)

        normalized_lead_user_id = (lead_user_id or "").strip()
        if normalized_lead_user_id:
            lead_team_ids: set[str] = set()
            for membership in self._memberships_by_id.values():
                if membership.get("status") != "accepted":
                    continue
                if membership.get("role") != "lead":
                    continue
                if membership.get("user_id") != normalized_lead_user_id:
                    continue
                lead_team_id = str(membership.get("team_id", "")).strip()
                if lead_team_id:
                    lead_team_ids.add(lead_team_id)
            matched_team_ids &= lead_team_ids

        teams = self.list_teams_by_ids(sorted(matched_team_ids))
        teams.sort(key=lambda team: str(team.get("name", "")).lower())
        return teams


class MongoTeamMembershipStore(TeamMembershipStore):
    def __init__(
        self,
        *,
        uri: str,
        db_name: str,
        teams_collection_name: str,
        memberships_collection_name: str,
        invitations_collection_name: str,
        connect_timeout_ms: int = 2000,
    ) -> None:
        from pymongo import MongoClient

        self._client = MongoClient(
            uri,
            serverSelectionTimeoutMS=connect_timeout_ms,
            connectTimeoutMS=connect_timeout_ms,
        )
        database = self._client[db_name]
        self._teams = database[teams_collection_name]
        self._memberships = database[memberships_collection_name]
        self._invitations = database[invitations_collection_name]

        self._teams.create_index("created_by_user_id")
        self._memberships.create_index([("team_id", 1), ("user_id", 1)], unique=True)
        self._memberships.create_index([("user_id", 1), ("status", 1)])
        self._memberships.create_index([("team_id", 1), ("status", 1)])
        self._invitations.create_index([("team_id", 1), ("invited_email", 1), ("status", 1)], unique=True)
        self._invitations.create_index([("invited_email", 1), ("status", 1)])

    def create_team(
        self,
        *,
        name: str,
        created_by_user_id: str,
        recipient_user_ids: list[str],
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        normalized_recipients = sorted(
            {
                user_id.strip()
                for user_id in recipient_user_ids
                if user_id and user_id.strip()
            },
        )
        payload = {
            "name": name.strip(),
            "created_by_user_id": created_by_user_id.strip(),
            "recipient_user_ids": normalized_recipients,
            "created_at": now,
            "updated_at": now,
        }
        insert_result = self._teams.insert_one(payload)
        return self.get_team(str(insert_result.inserted_id)) or {}

    def get_team(self, team_id: str) -> dict[str, Any] | None:
        object_id = _to_object_id(team_id)
        if not object_id:
            return None
        record = self._teams.find_one({"_id": object_id})
        return _serialize_record(record)

    def set_team_recipients(self, team_id: str, recipient_user_ids: list[str]) -> dict[str, Any] | None:
        object_id = _to_object_id(team_id)
        if not object_id:
            return None
        normalized_recipients = sorted(
            {
                user_id.strip()
                for user_id in recipient_user_ids
                if user_id and user_id.strip()
            },
        )
        self._teams.update_one(
            {"_id": object_id},
            {
                "$set": {
                    "recipient_user_ids": normalized_recipients,
                    "updated_at": datetime.now(UTC),
                },
            },
        )
        return self.get_team(team_id)

    def list_teams_by_ids(self, team_ids: list[str]) -> list[dict[str, Any]]:
        object_ids = []
        for team_id in team_ids:
            object_id = _to_object_id(team_id)
            if object_id:
                object_ids.append(object_id)
        if not object_ids:
            return []
        records = list(self._teams.find({"_id": {"$in": object_ids}}))
        by_id = {
            str(record.get("_id")): _serialize_record(record)
            for record in records
        }
        ordered: list[dict[str, Any]] = []
        for team_id in team_ids:
            team = by_id.get(team_id)
            if team:
                ordered.append(team)
        return ordered

    def list_memberships_for_user(
        self,
        user_id: str,
        *,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"user_id": user_id.strip()}
        if status:
            query["status"] = status.strip().lower()
        records = list(self._memberships.find(query))
        memberships: list[dict[str, Any]] = []
        for record in records:
            normalized = _normalize_membership_record(_serialize_record(record))
            if normalized:
                memberships.append(normalized)
        return memberships

    def list_memberships_for_team(
        self,
        team_id: str,
        *,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"team_id": team_id.strip()}
        if status:
            query["status"] = status.strip().lower()
        records = list(self._memberships.find(query))
        memberships: list[dict[str, Any]] = []
        for record in records:
            normalized = _normalize_membership_record(_serialize_record(record))
            if normalized:
                memberships.append(normalized)
        return memberships

    def upsert_membership(
        self,
        *,
        team_id: str,
        user_id: str,
        role: str,
        status: str,
        invited_by_user_id: str | None = None,
        is_active: bool | None = None,
    ) -> dict[str, Any]:
        query = {
            "team_id": team_id.strip(),
            "user_id": user_id.strip(),
        }
        now = datetime.now(UTC)
        set_payload = {
            "role": role.strip().lower(),
            "status": status.strip().lower(),
            "updated_at": now,
            "invited_by_user_id": (invited_by_user_id or "").strip() or None,
        }
        set_on_insert_payload: dict[str, Any] = {"created_at": now}
        if is_active is None:
            set_on_insert_payload["is_active"] = True
        else:
            set_payload["is_active"] = bool(is_active)
        self._memberships.update_one(
            query,
            {
                "$set": set_payload,
                "$setOnInsert": set_on_insert_payload,
            },
            upsert=True,
        )
        record = self._memberships.find_one(query)
        return _normalize_membership_record(_serialize_record(record)) or {}

    def set_membership_activation(
        self,
        *,
        team_id: str,
        user_id: str,
        is_active: bool,
    ) -> dict[str, Any] | None:
        query = {
            "team_id": team_id.strip(),
            "user_id": user_id.strip(),
        }
        self._memberships.update_one(
            query,
            {
                "$set": {
                    "is_active": bool(is_active),
                    "updated_at": datetime.now(UTC),
                },
            },
        )
        record = self._memberships.find_one(query)
        return _normalize_membership_record(_serialize_record(record))

    def create_or_get_pending_invitation(
        self,
        *,
        team_id: str,
        invited_email: str,
        invited_by_user_id: str,
    ) -> dict[str, Any]:
        from pymongo.errors import DuplicateKeyError

        normalized_team_id = team_id.strip()
        normalized_invited_email = invited_email.strip().lower()
        existing = self._invitations.find_one(
            {
                "team_id": normalized_team_id,
                "invited_email": normalized_invited_email,
                "status": "pending",
            },
        )
        if existing:
            self._invitations.update_one(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        "invited_by_user_id": invited_by_user_id.strip(),
                        "updated_at": datetime.now(UTC),
                    },
                },
            )
            refreshed = self._invitations.find_one({"_id": existing["_id"]})
            return _serialize_record(refreshed) or {}

        now = datetime.now(UTC)
        payload = {
            "team_id": normalized_team_id,
            "invited_email": normalized_invited_email,
            "invited_by_user_id": invited_by_user_id.strip(),
            "status": "pending",
            "responded_by_user_id": None,
            "created_at": now,
            "updated_at": now,
        }
        try:
            insert_result = self._invitations.insert_one(payload)
            created = self._invitations.find_one({"_id": insert_result.inserted_id})
            return _serialize_record(created) or {}
        except DuplicateKeyError:
            existing = self._invitations.find_one(
                {
                    "team_id": normalized_team_id,
                    "invited_email": normalized_invited_email,
                    "status": "pending",
                },
            )
            return _serialize_record(existing) or {}

    def list_pending_invitations_for_email(self, invited_email: str) -> list[dict[str, Any]]:
        query = {
            "invited_email": invited_email.strip().lower(),
            "status": "pending",
        }
        records = list(self._invitations.find(query).sort("created_at", 1))
        return [_serialize_record(record) for record in records]

    def list_pending_invitations_for_team(self, team_id: str) -> list[dict[str, Any]]:
        query = {
            "team_id": team_id.strip(),
            "status": "pending",
        }
        records = list(self._invitations.find(query).sort("created_at", 1))
        return [_serialize_record(record) for record in records]

    def get_invitation(self, invitation_id: str) -> dict[str, Any] | None:
        object_id = _to_object_id(invitation_id)
        if not object_id:
            return None
        record = self._invitations.find_one({"_id": object_id})
        return _serialize_record(record)

    def update_invitation_status(
        self,
        *,
        invitation_id: str,
        status: str,
        responded_by_user_id: str | None = None,
    ) -> dict[str, Any] | None:
        object_id = _to_object_id(invitation_id)
        if not object_id:
            return None
        self._invitations.update_one(
            {"_id": object_id},
            {
                "$set": {
                    "status": status.strip().lower(),
                    "responded_by_user_id": (responded_by_user_id or "").strip() or None,
                    "updated_at": datetime.now(UTC),
                },
            },
        )
        record = self._invitations.find_one({"_id": object_id})
        return _serialize_record(record)

    def list_teams_matching_participants(
        self,
        *,
        participant_user_ids: list[str],
        lead_user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_participant_user_ids = [
            user_id.strip()
            for user_id in participant_user_ids
            if user_id and user_id.strip()
        ]
        if not normalized_participant_user_ids:
            return []

        participant_memberships = self._memberships.find(
            {
                "status": "accepted",
                "user_id": {"$in": normalized_participant_user_ids},
            },
            {"team_id": 1},
        )
        team_ids = {str(record.get("team_id", "")).strip() for record in participant_memberships}
        team_ids.discard("")

        normalized_lead_user_id = (lead_user_id or "").strip()
        if normalized_lead_user_id:
            lead_memberships = self._memberships.find(
                {
                    "status": "accepted",
                    "role": "lead",
                    "user_id": normalized_lead_user_id,
                },
                {"team_id": 1},
            )
            lead_team_ids = {str(record.get("team_id", "")).strip() for record in lead_memberships}
            lead_team_ids.discard("")
            team_ids &= lead_team_ids

        teams = self.list_teams_by_ids(sorted(team_ids))
        teams.sort(key=lambda team: str(team.get("name", "")).lower())
        return teams


def _to_object_id(record_id: str) -> Any | None:
    from bson import ObjectId
    from bson.errors import InvalidId

    try:
        return ObjectId(record_id)
    except InvalidId:
        return None


def _serialize_record(record: Any) -> dict[str, Any] | None:
    if not record:
        return None
    payload = dict(record)
    payload["_id"] = str(record.get("_id", ""))
    return payload


def _normalize_membership_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    payload = dict(record)
    payload["is_active"] = bool(payload.get("is_active", True))
    return payload


def create_team_membership_store(settings: Settings) -> TeamMembershipStore:
    return _create_team_membership_store_cached(
        user_data_store=settings.user_data_store,
        mongodb_uri=settings.mongodb_uri,
        mongodb_db_name=settings.mongodb_db_name,
        mongodb_teams_collection=settings.mongodb_teams_collection,
        mongodb_team_memberships_collection=settings.mongodb_team_memberships_collection,
        mongodb_team_invitations_collection=settings.mongodb_team_invitations_collection,
        mongodb_connect_timeout_ms=settings.mongodb_connect_timeout_ms,
    )


@lru_cache
def _create_team_membership_store_cached(
    *,
    user_data_store: str,
    mongodb_uri: str,
    mongodb_db_name: str,
    mongodb_teams_collection: str,
    mongodb_team_memberships_collection: str,
    mongodb_team_invitations_collection: str,
    mongodb_connect_timeout_ms: int,
) -> TeamMembershipStore:
    if user_data_store == "memory":
        return InMemoryTeamMembershipStore()

    if user_data_store == "mongodb":
        return MongoTeamMembershipStore(
            uri=mongodb_uri,
            db_name=mongodb_db_name,
            teams_collection_name=mongodb_teams_collection,
            memberships_collection_name=mongodb_team_memberships_collection,
            invitations_collection_name=mongodb_team_invitations_collection,
            connect_timeout_ms=mongodb_connect_timeout_ms,
        )

    return InMemoryTeamMembershipStore()


def clear_team_membership_store_cache() -> None:
    _create_team_membership_store_cached.cache_clear()
