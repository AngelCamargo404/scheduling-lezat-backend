import json
from collections.abc import Mapping
from typing import Any
from urllib import error, parse, request

from app.services.action_item_models import ActionItem


class NotionKanbanError(Exception):
    pass


class NotionKanbanClient:
    def __init__(
        self,
        *,
        api_token: str,
        database_id: str = "",
        timeout_seconds: float = 10.0,
        api_version: str = "2022-06-28",
        todo_status_name: str = "Por hacer",
        title_property: str = "Name",
        assignee_property: str = "Assignee",
        status_property: str = "Status",
        due_date_property: str = "Due date",
        details_property: str = "Details",
        meeting_id_property: str = "Meeting ID",
        api_base_url: str = "https://api.notion.com/v1",
    ) -> None:
        self.api_token = api_token
        self.database_id = database_id
        self.timeout_seconds = timeout_seconds
        self.api_version = api_version
        self.todo_status_name = todo_status_name
        self.title_property = title_property
        self.assignee_property = assignee_property
        self.status_property = status_property
        self.due_date_property = due_date_property
        self.details_property = details_property
        self.meeting_id_property = meeting_id_property
        self.api_base_url = api_base_url.rstrip("/")
        self._database_properties_cache: dict[str, dict[str, Any]] | None = None
        self._users_by_email_cache: dict[str, str] | None = None

    def list_accessible_databases(self) -> list[dict[str, Any]]:
        """
        Lists all databases accessible by the integration token.
        Returns a simplified list of dicts with id, title, and url.
        """
        body = {
            "filter": {"value": "database", "property": "object"},
            "sort": {"direction": "descending", "timestamp": "last_edited_time"},
            "page_size": 100,
        }
        try:
            response = self._request_json("POST", "/search", body)
        except Exception:
            # If search fails, we just return empty list to avoid blocking UI
            return []

        results = response.get("results", [])
        databases = []
        for db in results:
            if db.get("object") != "database":
                continue
            
            title_blocks = db.get("title", [])
            title_text = "Untitled"
            if title_blocks:
                title_text = "".join(t.get("plain_text", "") for t in title_blocks)
            
            databases.append({
                "id": db.get("id"),
                "title": title_text,
                "url": db.get("url"),
            })
        return databases

    def create_kanban_task(self, *, item: ActionItem, meeting_id: str | None) -> str:
        database_properties = self._get_database_properties()
        task_properties = self._build_task_properties(
            item=item,
            meeting_id=meeting_id,
            database_properties=database_properties,
        )
        payload: dict[str, Any] = {
            "parent": {"database_id": self.database_id},
            "properties": task_properties,
        }
        blocks = self._build_description_blocks(item)
        if blocks:
            payload["children"] = blocks

        response_payload = self._request_json("POST", "/pages", payload=payload)
        page_id = response_payload.get("id")
        if not isinstance(page_id, str) or not page_id.strip():
            raise NotionKanbanError("Notion API create page response missing id.")
        return page_id

    def _build_task_properties(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
        database_properties: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {}

        title_property_name = self._resolve_title_property_name(database_properties)
        if title_property_name:
            properties[title_property_name] = {
                "title": [{"text": {"content": self._truncate(item.title, 2000)}}],
            }

        self._set_status_property(properties, database_properties)
        self._set_due_date_property(properties, item, database_properties)
        self._set_assignee_property(properties, item, database_properties)
        self._set_text_property(
            properties=properties,
            database_properties=database_properties,
            configured_name=self.details_property,
            value=item.details,
        )
        self._set_text_property(
            properties=properties,
            database_properties=database_properties,
            configured_name=self.meeting_id_property,
            value=meeting_id,
        )
        return properties

    def _set_status_property(
        self,
        properties: dict[str, Any],
        database_properties: dict[str, dict[str, Any]],
    ) -> None:
        db_property = database_properties.get(self.status_property)
        if not db_property:
            return

        property_type = db_property.get("type")
        if property_type == "status":
            properties[self.status_property] = {"status": {"name": self.todo_status_name}}
            return
        if property_type == "select":
            properties[self.status_property] = {"select": {"name": self.todo_status_name}}

    def _set_assignee_property(
        self,
        properties: dict[str, Any],
        item: ActionItem,
        database_properties: dict[str, dict[str, Any]],
    ) -> None:
        db_property = database_properties.get(self.assignee_property)
        if not db_property:
            return

        property_type = db_property.get("type")
        if property_type == "people" and item.assignee_email:
            user_id = self._find_user_id_by_email(item.assignee_email)
            if user_id:
                properties[self.assignee_property] = {"people": [{"id": user_id}]}
            return

        fallback_text = item.assignee_name or item.assignee_email
        if not fallback_text:
            return

        if property_type == "rich_text":
            properties[self.assignee_property] = {
                "rich_text": [{"text": {"content": self._truncate(fallback_text, 2000)}}],
            }
            return
        if property_type == "email" and item.assignee_email:
            properties[self.assignee_property] = {"email": item.assignee_email}
            return
        if property_type == "select":
            properties[self.assignee_property] = {
                "select": {"name": self._truncate(fallback_text, 100)},
            }

    def _set_due_date_property(
        self,
        properties: dict[str, Any],
        item: ActionItem,
        database_properties: dict[str, dict[str, Any]],
    ) -> None:
        if not item.due_date:
            return
        db_property = database_properties.get(self.due_date_property)
        if not db_property:
            return

        property_type = db_property.get("type")
        if property_type == "date":
            properties[self.due_date_property] = {"date": {"start": item.due_date}}
            return
        if property_type == "rich_text":
            properties[self.due_date_property] = {
                "rich_text": [{"text": {"content": item.due_date}}],
            }

    def _set_text_property(
        self,
        *,
        properties: dict[str, Any],
        database_properties: dict[str, dict[str, Any]],
        configured_name: str,
        value: str | None,
    ) -> None:
        if not value:
            return
        db_property = database_properties.get(configured_name)
        if not db_property:
            return
        property_type = db_property.get("type")
        if property_type == "rich_text":
            properties[configured_name] = {
                "rich_text": [{"text": {"content": self._truncate(value, 2000)}}],
            }
            return
        if property_type == "url":
            properties[configured_name] = {"url": self._truncate(value, 2000)}

    def _resolve_title_property_name(
        self,
        database_properties: dict[str, dict[str, Any]],
    ) -> str | None:
        configured_property = database_properties.get(self.title_property)
        if configured_property and configured_property.get("type") == "title":
            return self.title_property

        for property_name, property_payload in database_properties.items():
            if property_payload.get("type") == "title":
                return property_name
        return None

    def _build_description_blocks(self, item: ActionItem) -> list[dict[str, Any]]:
        lines: list[str] = []
        if item.details:
            lines.append(f"Detalles: {item.details}")
        if item.source_sentence:
            lines.append(f"Evidencia: {item.source_sentence}")
        if not lines:
            return []

        merged_text = "\n".join(lines)
        return [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": self._truncate(merged_text, 1900)},
                        },
                    ],
                },
            },
        ]

    def _find_user_id_by_email(self, email: str) -> str | None:
        email_map = self._list_users_by_email()
        return email_map.get(email.lower())

    def _list_users_by_email(self) -> dict[str, str]:
        if self._users_by_email_cache is not None:
            return self._users_by_email_cache

        users_by_email: dict[str, str] = {}
        next_cursor: str | None = None
        while True:
            query_params = {"page_size": 100}
            if next_cursor:
                query_params["start_cursor"] = next_cursor
            query = parse.urlencode(query_params)
            response = self._request_json("GET", f"/users?{query}")
            results = response.get("results")
            if isinstance(results, list):
                for user_payload in results:
                    if not isinstance(user_payload, Mapping):
                        continue
                    user_id = user_payload.get("id")
                    person_payload = user_payload.get("person")
                    if not isinstance(user_id, str) or not isinstance(person_payload, Mapping):
                        continue
                    email = person_payload.get("email")
                    if not isinstance(email, str) or not email.strip():
                        continue
                    users_by_email[email.strip().lower()] = user_id

            has_more = bool(response.get("has_more"))
            next_cursor = response.get("next_cursor")
            if not has_more or not isinstance(next_cursor, str):
                break

        self._users_by_email_cache = users_by_email
        return users_by_email

    def _get_database_properties(self) -> dict[str, dict[str, Any]]:
        if self._database_properties_cache is not None:
            return self._database_properties_cache

        payload = self._request_json("GET", f"/databases/{self.database_id}")
        raw_properties = payload.get("properties")
        if not isinstance(raw_properties, Mapping):
            raise NotionKanbanError("Notion database response missing properties.")

        normalized: dict[str, dict[str, Any]] = {}
        for property_name, property_payload in raw_properties.items():
            if not isinstance(property_name, str) or not isinstance(property_payload, Mapping):
                continue
            normalized[property_name] = dict(property_payload)
        self._database_properties_cache = normalized
        return normalized

    def _request_json(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        target = f"{self.api_base_url}{path}"
        raw_payload: bytes | None = None
        if payload is not None:
            raw_payload = json.dumps(payload).encode("utf-8")

        req = request.Request(
            target,
            data=raw_payload,
            method=method,
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Notion-Version": self.api_version,
                "Content-Type": "application/json",
            },
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_body = response.read()
        except TimeoutError as exc:
            raise NotionKanbanError("Notion API request timed out.") from exc
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise NotionKanbanError(
                f"Notion API HTTP {exc.code}: {body or 'empty response body'}",
            ) from exc
        except error.URLError as exc:
            raise NotionKanbanError(f"Notion API connection error: {exc.reason}") from exc

        try:
            parsed_body = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise NotionKanbanError("Notion API returned invalid JSON.") from exc

        if not isinstance(parsed_body, dict):
            raise NotionKanbanError("Notion API response is not a JSON object.")
        return parsed_body

    def _truncate(self, value: str, limit: int) -> str:
        if len(value) <= limit:
            return value
        return value[: limit - 3] + "..."
