import json
from collections.abc import Mapping
from typing import Any
from urllib import error, parse, request

from app.services.action_item_models import ActionItem


class NotionCalendarError(Exception):
    pass


class NotionCalendarClient:
    def __init__(
        self,
        *,
        api_token: str,
        database_id: str,
        timeout_seconds: float = 10.0,
        api_version: str = "2022-06-28",
        title_property: str = "Name",
        date_property: str = "Date",
        details_property: str = "Description",
        meeting_id_property: str = "Meeting ID",
        api_base_url: str = "https://api.notion.com/v1",
    ) -> None:
        self.api_token = api_token
        self.database_id = database_id
        self.timeout_seconds = timeout_seconds
        self.api_version = api_version
        self.title_property = title_property
        self.date_property = date_property
        self.details_property = details_property
        self.meeting_id_property = meeting_id_property
        self.api_base_url = api_base_url.rstrip("/")
        self._database_properties_cache: dict[str, dict[str, Any]] | None = None

    def create_event(self, *, item: ActionItem, meeting_id: str | None) -> str:
        if not item.due_date:
            raise NotionCalendarError("Action item does not include due_date.")

        database_properties = self._get_database_properties()
        page_properties = self._build_page_properties(
            item=item,
            meeting_id=meeting_id,
            database_properties=database_properties,
        )
        payload: dict[str, Any] = {
            "parent": {"database_id": self.database_id},
            "properties": page_properties,
        }
        blocks = self._build_description_blocks(item)
        if blocks:
            payload["children"] = blocks

        response_payload = self._request_json("POST", "/pages", payload=payload)
        page_id = response_payload.get("id")
        if not isinstance(page_id, str) or not page_id.strip():
            raise NotionCalendarError("Notion API create page response missing id.")
        return page_id

    def _build_page_properties(
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

        self._set_date_property(properties, item, database_properties)
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

    def _set_date_property(
        self,
        properties: dict[str, Any],
        item: ActionItem,
        database_properties: dict[str, dict[str, Any]],
    ) -> None:
        if not item.due_date:
            return
        
        target_property = self._find_property_by_name_or_type(
            database_properties,
            self.date_property,
            "date",
        )
        if target_property:
            properties[target_property] = {"date": {"start": item.due_date}}

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
        target_property = self._find_property_by_name_or_type(
            database_properties,
            configured_name,
            "rich_text",
        )
        if target_property:
            properties[target_property] = {
                "rich_text": [{"text": {"content": self._truncate(value, 2000)}}],
            }

    def _resolve_title_property_name(
        self,
        database_properties: dict[str, dict[str, Any]],
    ) -> str | None:
        # Check if configured name exists and is title
        if self.title_property in database_properties:
            prop = database_properties[self.title_property]
            if prop.get("type") == "title":
                return self.title_property
        
        # Fallback: search for any title property
        for name, prop in database_properties.items():
            if prop.get("type") == "title":
                return name
        return None

    def _find_property_by_name_or_type(
        self,
        database_properties: dict[str, dict[str, Any]],
        name: str,
        type_name: str,
    ) -> str | None:
        if name in database_properties:
             prop = database_properties[name]
             if prop.get("type") == type_name:
                 return name
        return None

    def _get_database_properties(self) -> dict[str, dict[str, Any]]:
        if self._database_properties_cache is not None:
            return self._database_properties_cache

        response = self._request_json("GET", f"/databases/{self.database_id}")
        properties = response.get("properties")
        if not isinstance(properties, dict):
             raise NotionCalendarError("Notion API response missing properties.")
        
        self._database_properties_cache = properties
        return properties

    def _request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
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
            raise NotionCalendarError("Notion API request timed out.") from exc
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise NotionCalendarError(
                f"Notion API HTTP {exc.code}: {body or 'empty response body'}",
            ) from exc
        except error.URLError as exc:
             raise NotionCalendarError(
                f"Notion API connection error: {exc.reason}",
            ) from exc

        try:
            parsed_body = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise NotionCalendarError("Notion API returned invalid JSON.") from exc
        if not isinstance(parsed_body, dict):
            raise NotionCalendarError("Notion API response is not a JSON object.")
        return parsed_body

    def _build_description_blocks(self, item: ActionItem) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []
        if item.details:
            blocks.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": item.details}}],
                    },
                },
            )
        return blocks

    def _truncate(self, text: str, length: int) -> str:
        if len(text) <= length:
            return text
        return text[:length]
