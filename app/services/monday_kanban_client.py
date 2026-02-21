import json
from collections.abc import Mapping
from typing import Any
from urllib import error, request

from app.services.action_item_models import ActionItem


class MondayKanbanError(Exception):
    pass


class MondayKanbanClient:
    def __init__(
        self,
        *,
        api_token: str,
        board_id: str = "",
        group_id: str = "",
        timeout_seconds: float = 10.0,
        status_column_id: str = "status",
        todo_status_label: str = "Working on it",
        assignee_column_id: str = "person",
        due_date_column_id: str = "date",
        details_column_id: str = "long_text",
        meeting_id_column_id: str = "text",
        api_base_url: str = "https://api.monday.com/v2",
    ) -> None:
        self.api_token = api_token.strip()
        self.board_id = board_id.strip()
        self.group_id = group_id.strip()
        self.timeout_seconds = timeout_seconds
        self.status_column_id = status_column_id.strip()
        self.todo_status_label = todo_status_label.strip()
        self.assignee_column_id = assignee_column_id.strip()
        self.due_date_column_id = due_date_column_id.strip()
        self.details_column_id = details_column_id.strip()
        self.meeting_id_column_id = meeting_id_column_id.strip()
        self.api_base_url = api_base_url.rstrip("/")
        self._boards_cache: list[dict[str, Any]] | None = None
        self._board_details_cache: dict[str, dict[str, Any]] = {}
        self._users_by_email_cache: dict[str, str] | None = None

    def list_accessible_boards(self) -> list[dict[str, Any]]:
        if self._boards_cache is not None:
            return self._boards_cache

        query = """
        query {
          boards(limit: 100) {
            id
            name
            url
          }
        }
        """
        data = self._request_graphql(query=query)
        raw_boards = data.get("boards", [])
        if not isinstance(raw_boards, list):
            return []

        boards: list[dict[str, Any]] = []
        for raw_board in raw_boards:
            if not isinstance(raw_board, Mapping):
                continue
            board_id = str(raw_board.get("id", "")).strip()
            if not board_id:
                continue
            boards.append(
                {
                    "id": board_id,
                    "name": str(raw_board.get("name", "")).strip(),
                    "url": str(raw_board.get("url", "")).strip(),
                },
            )
        self._boards_cache = boards
        return boards

    def list_board_groups(self, *, board_id: str | None = None) -> list[dict[str, str]]:
        board_details = self._get_board_details(self._resolve_board_id(board_id))
        raw_groups = board_details.get("groups", [])
        if not isinstance(raw_groups, list):
            return []

        groups: list[dict[str, str]] = []
        for raw_group in raw_groups:
            if not isinstance(raw_group, Mapping):
                continue
            group_id = str(raw_group.get("id", "")).strip()
            if not group_id:
                continue
            groups.append(
                {
                    "id": group_id,
                    "title": str(raw_group.get("title", "")).strip() or group_id,
                },
            )
        return groups

    def list_board_columns(self, *, board_id: str | None = None) -> list[dict[str, str]]:
        board_details = self._get_board_details(self._resolve_board_id(board_id))
        raw_columns = board_details.get("columns", [])
        if not isinstance(raw_columns, list):
            return []

        columns: list[dict[str, str]] = []
        for raw_column in raw_columns:
            if not isinstance(raw_column, Mapping):
                continue
            column_id = str(raw_column.get("id", "")).strip()
            if not column_id:
                continue
            columns.append(
                {
                    "id": column_id,
                    "title": str(raw_column.get("title", "")).strip() or column_id,
                    "type": str(raw_column.get("type", "")).strip(),
                },
            )
        return columns

    def list_board_status_options(
        self,
        *,
        board_id: str | None = None,
        status_column_id: str | None = None,
    ) -> dict[str, Any]:
        normalized_board_id = self._resolve_board_id(board_id)
        board_details = self._get_board_details(normalized_board_id)
        raw_columns = board_details.get("columns", [])
        if not isinstance(raw_columns, list):
            return {
                "selected_column_id": None,
                "options": [],
                "available_status_columns": [],
            }

        available_status_columns: list[dict[str, str]] = []
        selected_column: Mapping[str, Any] | None = None
        target_column_id = (status_column_id or "").strip() or self.status_column_id

        for raw_column in raw_columns:
            if not isinstance(raw_column, Mapping):
                continue
            column_id = str(raw_column.get("id", "")).strip()
            column_type = str(raw_column.get("type", "")).strip().lower()
            if column_type not in {"status", "dropdown"}:
                continue
            column_title = str(raw_column.get("title", "")).strip() or column_id
            available_status_columns.append(
                {
                    "id": column_id,
                    "title": column_title,
                    "type": column_type,
                },
            )
            if column_id and column_id == target_column_id:
                selected_column = raw_column

        if selected_column is None and available_status_columns:
            fallback_id = available_status_columns[0]["id"]
            selected_column = next(
                (
                    raw_column
                    for raw_column in raw_columns
                    if isinstance(raw_column, Mapping)
                    and str(raw_column.get("id", "")).strip() == fallback_id
                ),
                None,
            )

        options = self._extract_status_options(selected_column or {})
        selected_column_id = (
            str(selected_column.get("id", "")).strip()
            if isinstance(selected_column, Mapping)
            else None
        )
        return {
            "selected_column_id": selected_column_id or None,
            "options": options,
            "available_status_columns": available_status_columns,
        }

    def create_kanban_item(self, *, item: ActionItem, meeting_id: str | None) -> str:
        board_id = self._resolve_board_id(None)
        group_id = self.group_id.strip()
        if not group_id:
            raise MondayKanbanError("Monday group_id is missing.")

        board_details = self._get_board_details(board_id)
        board_columns = board_details.get("columns", [])
        if not isinstance(board_columns, list):
            board_columns = []

        column_values = self._build_column_values(
            item=item,
            meeting_id=meeting_id,
            raw_columns=board_columns,
        )
        variables: dict[str, Any] = {
            "board_id": board_id,
            "group_id": group_id,
            "item_name": self._truncate(item.title, 255),
        }
        if column_values:
            variables["column_values"] = json.dumps(column_values, ensure_ascii=False)

        mutation = """
        mutation ($board_id: ID!, $group_id: String!, $item_name: String!, $column_values: JSON) {
          create_item(
            board_id: $board_id,
            group_id: $group_id,
            item_name: $item_name,
            column_values: $column_values
          ) {
            id
          }
        }
        """
        payload = self._request_graphql(query=mutation, variables=variables)
        create_item_payload = payload.get("create_item")
        if not isinstance(create_item_payload, Mapping):
            raise MondayKanbanError("Monday API create_item response missing payload.")
        item_id = str(create_item_payload.get("id", "")).strip()
        if not item_id:
            raise MondayKanbanError("Monday API create_item response missing id.")
        return item_id

    def _build_column_values(
        self,
        *,
        item: ActionItem,
        meeting_id: str | None,
        raw_columns: list[Any],
    ) -> dict[str, Any]:
        column_values: dict[str, Any] = {}
        columns_by_id = self._index_columns_by_id(raw_columns)

        self._set_status_column_value(column_values, columns_by_id)
        self._set_assignee_column_value(column_values, columns_by_id, item)
        self._set_due_date_column_value(column_values, columns_by_id, item)
        self._set_details_column_value(column_values, columns_by_id, item)
        self._set_meeting_id_column_value(column_values, columns_by_id, meeting_id)
        return column_values

    def _set_status_column_value(
        self,
        values: dict[str, Any],
        columns_by_id: dict[str, Mapping[str, Any]],
    ) -> None:
        if not self.status_column_id or not self.todo_status_label:
            return
        column = columns_by_id.get(self.status_column_id)
        if not column:
            return
        column_type = str(column.get("type", "")).strip().lower()
        if column_type not in {"status", "dropdown"}:
            return
        values[self.status_column_id] = {"label": self.todo_status_label}

    def _set_assignee_column_value(
        self,
        values: dict[str, Any],
        columns_by_id: dict[str, Mapping[str, Any]],
        item: ActionItem,
    ) -> None:
        if not self.assignee_column_id or not item.assignee_email:
            return
        column = columns_by_id.get(self.assignee_column_id)
        if not column:
            return
        column_type = str(column.get("type", "")).strip().lower()
        if column_type not in {"people", "multiple-person", "person"}:
            return

        try:
            # Best effort: if the account token cannot read `users`, still create the item.
            user_id = self._find_user_id_by_email(item.assignee_email)
        except MondayKanbanError:
            return
        if not user_id:
            return
        try:
            normalized_user_id = int(user_id)
        except ValueError:
            return
        values[self.assignee_column_id] = {
            "personsAndTeams": [
                {
                    "id": normalized_user_id,
                    "kind": "person",
                },
            ],
        }

    def _set_due_date_column_value(
        self,
        values: dict[str, Any],
        columns_by_id: dict[str, Mapping[str, Any]],
        item: ActionItem,
    ) -> None:
        if not self.due_date_column_id or not item.due_date:
            return
        column = columns_by_id.get(self.due_date_column_id)
        if not column:
            return
        column_type = str(column.get("type", "")).strip().lower()
        if column_type != "date":
            return
        values[self.due_date_column_id] = {"date": item.due_date}

    def _set_details_column_value(
        self,
        values: dict[str, Any],
        columns_by_id: dict[str, Mapping[str, Any]],
        item: ActionItem,
    ) -> None:
        if not self.details_column_id:
            return
        column = columns_by_id.get(self.details_column_id)
        if not column:
            return
        detail_lines: list[str] = []
        if item.details:
            detail_lines.append(item.details)
        if item.source_sentence:
            detail_lines.append(f"Evidencia: {item.source_sentence}")
        if not detail_lines:
            return
        details = self._truncate("\n".join(detail_lines), 2000)
        encoded_value = self._encode_text_by_column_type(
            column_type=str(column.get("type", "")).strip().lower(),
            value=details,
        )
        if encoded_value is None:
            return
        values[self.details_column_id] = encoded_value

    def _set_meeting_id_column_value(
        self,
        values: dict[str, Any],
        columns_by_id: dict[str, Mapping[str, Any]],
        meeting_id: str | None,
    ) -> None:
        if not self.meeting_id_column_id or not meeting_id:
            return
        column = columns_by_id.get(self.meeting_id_column_id)
        if not column:
            return
        normalized_meeting_id = meeting_id.strip()
        if not normalized_meeting_id:
            return
        encoded_value = self._encode_text_by_column_type(
            column_type=str(column.get("type", "")).strip().lower(),
            value=self._truncate(normalized_meeting_id, 255),
        )
        if encoded_value is None:
            return
        values[self.meeting_id_column_id] = encoded_value

    def _encode_text_by_column_type(self, *, column_type: str, value: str) -> Any | None:
        if not value:
            return None
        if column_type in {"text", "name"}:
            return value
        if column_type in {"long-text", "long_text"}:
            return {"text": value}
        return None

    def _find_user_id_by_email(self, email: str) -> str | None:
        users_by_email = self._list_users_by_email()
        return users_by_email.get(email.strip().lower())

    def _list_users_by_email(self) -> dict[str, str]:
        if self._users_by_email_cache is not None:
            return self._users_by_email_cache

        query = """
        query {
          users(limit: 200) {
            id
            email
          }
        }
        """
        data = self._request_graphql(query=query)
        raw_users = data.get("users", [])
        users_by_email: dict[str, str] = {}
        if isinstance(raw_users, list):
            for raw_user in raw_users:
                if not isinstance(raw_user, Mapping):
                    continue
                user_id = str(raw_user.get("id", "")).strip()
                email = str(raw_user.get("email", "")).strip().lower()
                if not user_id or not email:
                    continue
                users_by_email[email] = user_id

        self._users_by_email_cache = users_by_email
        return users_by_email

    def _resolve_board_id(self, board_id: str | None) -> str:
        normalized = (board_id or "").strip() or self.board_id
        normalized = normalized.strip()
        if not normalized:
            raise MondayKanbanError("Monday board_id is missing.")
        return normalized

    def _get_board_details(self, board_id: str) -> dict[str, Any]:
        cached = self._board_details_cache.get(board_id)
        if cached is not None:
            return cached

        query = """
        query ($board_ids: [ID!]) {
          boards(ids: $board_ids) {
            id
            name
            url
            groups {
              id
              title
            }
            columns {
              id
              title
              type
              settings_str
            }
          }
        }
        """
        data = self._request_graphql(
            query=query,
            variables={"board_ids": [board_id]},
        )
        raw_boards = data.get("boards", [])
        if not isinstance(raw_boards, list) or not raw_boards:
            raise MondayKanbanError("Monday board was not found or is not accessible.")

        board_payload = raw_boards[0]
        if not isinstance(board_payload, Mapping):
            raise MondayKanbanError("Monday board response is invalid.")

        normalized_board = dict(board_payload)
        self._board_details_cache[board_id] = normalized_board
        return normalized_board

    def _extract_status_options(self, column_payload: Mapping[str, Any]) -> list[str]:
        raw_settings = column_payload.get("settings_str")
        if not isinstance(raw_settings, str) or not raw_settings.strip():
            return []

        try:
            parsed_settings = json.loads(raw_settings)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed_settings, Mapping):
            return []

        options: list[str] = []
        seen: set[str] = set()

        labels = parsed_settings.get("labels")
        if isinstance(labels, Mapping):
            for label_value in labels.values():
                if not isinstance(label_value, str):
                    continue
                normalized_label = label_value.strip()
                if not normalized_label or normalized_label in seen:
                    continue
                seen.add(normalized_label)
                options.append(normalized_label)

        labels_positions_v2 = parsed_settings.get("labels_positions_v2")
        if isinstance(labels_positions_v2, Mapping):
            for option_payload in labels_positions_v2.values():
                if not isinstance(option_payload, Mapping):
                    continue
                label_value = option_payload.get("label")
                if not isinstance(label_value, str):
                    continue
                normalized_label = label_value.strip()
                if not normalized_label or normalized_label in seen:
                    continue
                seen.add(normalized_label)
                options.append(normalized_label)

        return options

    def _index_columns_by_id(self, raw_columns: list[Any]) -> dict[str, Mapping[str, Any]]:
        columns_by_id: dict[str, Mapping[str, Any]] = {}
        for raw_column in raw_columns:
            if not isinstance(raw_column, Mapping):
                continue
            column_id = str(raw_column.get("id", "")).strip()
            if not column_id:
                continue
            columns_by_id[column_id] = raw_column
        return columns_by_id

    def _request_graphql(
        self,
        *,
        query: str,
        variables: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = dict(variables)

        req = request.Request(
            self.api_base_url,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": self.api_token,
                "Content-Type": "application/json",
            },
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_body = response.read()
        except TimeoutError as exc:
            raise MondayKanbanError("Monday API request timed out.") from exc
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise MondayKanbanError(
                f"Monday API HTTP {exc.code}: {body or 'empty response body'}",
            ) from exc
        except error.URLError as exc:
            raise MondayKanbanError(
                f"Monday API connection error: {exc.reason}",
            ) from exc

        try:
            parsed_payload = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise MondayKanbanError("Monday API returned invalid JSON.") from exc
        if not isinstance(parsed_payload, Mapping):
            raise MondayKanbanError("Monday API response is not a JSON object.")

        raw_errors = parsed_payload.get("errors")
        if isinstance(raw_errors, list) and raw_errors:
            messages: list[str] = []
            for raw_error in raw_errors:
                if not isinstance(raw_error, Mapping):
                    continue
                message = raw_error.get("message")
                if not isinstance(message, str) or not message.strip():
                    continue
                messages.append(message.strip())
            if messages:
                raise MondayKanbanError("Monday API error: " + "; ".join(messages[:3]))
            raise MondayKanbanError("Monday API returned GraphQL errors.")

        data = parsed_payload.get("data")
        if not isinstance(data, Mapping):
            raise MondayKanbanError("Monday API response missing data payload.")
        return dict(data)

    def _truncate(self, value: str, limit: int) -> str:
        if len(value) <= limit:
            return value
        return value[: limit - 3] + "..."
