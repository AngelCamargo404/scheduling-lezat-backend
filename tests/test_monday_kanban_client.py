import json

import pytest

from app.services.action_item_models import ActionItem
from app.services.monday_kanban_client import MondayKanbanClient, MondayKanbanError


class _MockResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "_MockResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
        return False

    def read(self) -> bytes:
        return self._payload


def _parse_graphql_request(req) -> tuple[str, dict[str, object]]:  # type: ignore[no-untyped-def]
    raw_body = req.data.decode("utf-8")
    payload = json.loads(raw_body)
    query = str(payload.get("query", ""))
    variables = payload.get("variables", {})
    return query, variables if isinstance(variables, dict) else {}


def test_monday_kanban_client_creates_item_with_column_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_create_variables: dict[str, object] = {}

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        query, variables = _parse_graphql_request(req)
        if "boards(ids" in query:
            return _MockResponse(
                {
                    "data": {
                        "boards": [
                            {
                                "id": "1234567890",
                                "groups": [{"id": "topics", "title": "Main"}],
                                "columns": [
                                    {
                                        "id": "status",
                                        "title": "Status",
                                        "type": "status",
                                        "settings_str": "{\"labels\":{\"0\":\"Working on it\"}}",
                                    },
                                    {
                                        "id": "long_text",
                                        "title": "Details",
                                        "type": "long_text",
                                        "settings_str": "{}",
                                    },
                                    {
                                        "id": "text",
                                        "title": "Meeting",
                                        "type": "text",
                                        "settings_str": "{}",
                                    },
                                ],
                            },
                        ],
                    },
                },
            )
        if "create_item" in query:
            captured_create_variables.update(variables)
            return _MockResponse({"data": {"create_item": {"id": "item-1"}}})
        raise AssertionError(f"Unexpected Monday query: {query}")

    monkeypatch.setattr("app.services.monday_kanban_client.request.urlopen", fake_urlopen)

    client = MondayKanbanClient(
        api_token="monday-token",
        board_id="1234567890",
        group_id="topics",
        status_column_id="status",
        todo_status_label="Working on it",
        details_column_id="long_text",
        meeting_id_column_id="text",
    )
    item_id = client.create_kanban_item(
        item=ActionItem(
            title="Enviar propuesta comercial",
            details="Incluir tiempos de implementacion.",
        ),
        meeting_id="meeting-123",
    )

    assert item_id == "item-1"
    assert captured_create_variables["board_id"] == "1234567890"
    assert captured_create_variables["group_id"] == "topics"
    column_values = json.loads(str(captured_create_variables["column_values"]))
    assert column_values["status"] == {"label": "Working on it"}
    assert column_values["long_text"] == {"text": "Incluir tiempos de implementacion."}
    assert column_values["text"] == "meeting-123"


def test_monday_kanban_client_lists_status_options_from_board_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        query, _variables = _parse_graphql_request(req)
        if "boards(ids" in query:
            return _MockResponse(
                {
                    "data": {
                        "boards": [
                            {
                                "id": "1234567890",
                                "groups": [{"id": "topics", "title": "Main"}],
                                "columns": [
                                    {
                                        "id": "status",
                                        "title": "Status",
                                        "type": "status",
                                        "settings_str": json.dumps(
                                            {"labels": {"0": "Por hacer", "1": "En curso"}},
                                        ),
                                    },
                                ],
                            },
                        ],
                    },
                },
            )
        raise AssertionError(f"Unexpected Monday query: {query}")

    monkeypatch.setattr("app.services.monday_kanban_client.request.urlopen", fake_urlopen)

    client = MondayKanbanClient(
        api_token="monday-token",
        board_id="1234567890",
        group_id="topics",
    )
    payload = client.list_board_status_options(
        board_id="1234567890",
        status_column_id="status",
    )

    assert payload["selected_column_id"] == "status"
    assert payload["options"] == ["Por hacer", "En curso"]


def test_monday_kanban_client_skips_assignee_when_users_query_is_not_authorized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_create_variables: dict[str, object] = {}

    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        query, variables = _parse_graphql_request(req)
        if "boards(ids" in query:
            return _MockResponse(
                {
                    "data": {
                        "boards": [
                            {
                                "id": "1234567890",
                                "groups": [{"id": "topics", "title": "Main"}],
                                "columns": [
                                    {
                                        "id": "status",
                                        "title": "Status",
                                        "type": "status",
                                        "settings_str": "{\"labels\":{\"0\":\"Working on it\"}}",
                                    },
                                    {
                                        "id": "person",
                                        "title": "Owner",
                                        "type": "people",
                                        "settings_str": "{}",
                                    },
                                ],
                            },
                        ],
                    },
                },
            )
        if "users(limit" in query:
            return _MockResponse(
                {
                    "errors": [
                        {
                            "message": "User is not authorized to perform this action.",
                        },
                    ],
                },
            )
        if "create_item" in query:
            captured_create_variables.update(variables)
            return _MockResponse({"data": {"create_item": {"id": "item-2"}}})
        raise AssertionError(f"Unexpected Monday query: {query}")

    monkeypatch.setattr("app.services.monday_kanban_client.request.urlopen", fake_urlopen)

    client = MondayKanbanClient(
        api_token="monday-token",
        board_id="1234567890",
        group_id="topics",
        assignee_column_id="person",
    )
    item_id = client.create_kanban_item(
        item=ActionItem(
            title="Asignar seguimiento",
            assignee_email="owner@example.com",
        ),
        meeting_id="meeting-456",
    )

    assert item_id == "item-2"
    column_values = json.loads(str(captured_create_variables["column_values"]))
    assert "person" not in column_values


def test_monday_kanban_client_raises_error_when_graphql_returns_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(req, timeout=10):  # type: ignore[no-untyped-def]
        query, _variables = _parse_graphql_request(req)
        if "boards(ids" in query:
            return _MockResponse(
                {
                    "data": {
                        "boards": [
                            {
                                "id": "1234567890",
                                "groups": [{"id": "topics", "title": "Main"}],
                                "columns": [],
                            },
                        ],
                    },
                },
            )
        if "create_item" in query:
            return _MockResponse(
                {
                    "errors": [
                        {
                            "message": "User is not authorized to perform this action.",
                        },
                    ],
                },
            )
        raise AssertionError(f"Unexpected Monday query: {query}")

    monkeypatch.setattr("app.services.monday_kanban_client.request.urlopen", fake_urlopen)

    client = MondayKanbanClient(
        api_token="monday-token",
        board_id="1234567890",
        group_id="topics",
    )
    with pytest.raises(MondayKanbanError, match="not authorized"):
        client.create_kanban_item(
            item=ActionItem(title="Tarea Monday"),
            meeting_id="meeting-unauthorized",
        )
