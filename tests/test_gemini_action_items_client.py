import json
from http.client import RemoteDisconnected

import pytest

from app.services.gemini_action_items_client import GeminiActionItemsClient, GeminiActionItemsError


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    def read(self) -> bytes:
        return self._body


def test_extract_action_items_retries_timeout_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def fake_urlopen(*args: object, **kwargs: object) -> _FakeResponse:
        calls["count"] += 1
        if calls["count"] < 3:
            raise TimeoutError("request timed out")
        return _FakeResponse(
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {"text": '{"action_items":[{"title":"Enviar propuesta"}]}'},
                            ],
                        },
                    },
                ],
            },
        )

    monkeypatch.setattr("app.services.gemini_action_items_client.sleep", lambda _: None)
    monkeypatch.setattr("app.services.gemini_action_items_client.request.urlopen", fake_urlopen)

    client = GeminiActionItemsClient(
        api_key="fake-api-key",
        model="gemini-3-flash-preview",
        timeout_seconds=0.1,
    )
    items = client.extract_action_items(
        meeting_id="meeting-1",
        transcript_text="Enviar propuesta.",
        transcript_sentences=[],
        participant_emails=[],
    )

    assert len(items) == 1
    assert calls["count"] == 3


def test_extract_action_items_fails_after_retries_on_remote_disconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def fake_urlopen(*args: object, **kwargs: object) -> _FakeResponse:
        calls["count"] += 1
        raise RemoteDisconnected("closed")

    monkeypatch.setattr("app.services.gemini_action_items_client.sleep", lambda _: None)
    monkeypatch.setattr("app.services.gemini_action_items_client.request.urlopen", fake_urlopen)

    client = GeminiActionItemsClient(
        api_key="fake-api-key",
        model="gemini-3-flash-preview",
        timeout_seconds=0.1,
    )
    with pytest.raises(
        GeminiActionItemsError,
        match="closed before sending a response",
    ):
        client.extract_action_items(
            meeting_id="meeting-2",
            transcript_text="Enviar propuesta.",
            transcript_sentences=[],
            participant_emails=[],
        )

    assert calls["count"] == 3


def test_extract_action_items_filters_non_actionable_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(*args: object, **kwargs: object) -> _FakeResponse:
        return _FakeResponse(
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"action_items":['
                                        '{"title":"Resumen general de la reunion"},'
                                        '{"title":"Enviar propuesta"}'
                                        "]}"
                                    ),
                                },
                            ],
                        },
                    },
                ],
            },
        )

    monkeypatch.setattr("app.services.gemini_action_items_client.request.urlopen", fake_urlopen)

    client = GeminiActionItemsClient(
        api_key="fake-api-key",
        model="gemini-3-flash-preview",
        timeout_seconds=0.1,
    )
    items = client.extract_action_items(
        meeting_id="meeting-3",
        transcript_text="Resumen de reunion. Enviar propuesta.",
        transcript_sentences=[],
        participant_emails=[],
    )

    assert len(items) == 1
    assert items[0].title == "Enviar propuesta"
