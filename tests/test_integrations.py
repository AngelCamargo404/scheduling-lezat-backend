from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import app
from app.services.user_store import clear_user_store_cache, create_user_store


@pytest.fixture(autouse=True)
def reset_auth_and_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USER_DATA_STORE", "memory")
    monkeypatch.setenv("GEMINI_API_KEY", "")
    monkeypatch.setenv("NOTION_CLIENT_ID", "")
    monkeypatch.setenv("NOTION_CLIENT_SECRET", "")
    monkeypatch.setenv("NOTION_REDIRECT_URI", "")
    monkeypatch.setenv("GOOGLE_CALENDAR_CLIENT_ID", "")
    monkeypatch.setenv("GOOGLE_CALENDAR_CLIENT_SECRET", "")
    monkeypatch.setenv("GOOGLE_CALENDAR_REDIRECT_URI", "")
    monkeypatch.setenv("OUTLOOK_CLIENT_ID", "")
    monkeypatch.setenv("OUTLOOK_CLIENT_SECRET", "")
    monkeypatch.setenv("OUTLOOK_TENANT_ID", "")
    monkeypatch.setenv("OUTLOOK_REDIRECT_URI", "")

    clear_user_store_cache()
    get_settings.cache_clear()
    yield
    clear_user_store_cache()
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _login_admin(client: TestClient) -> str:
    response = client.post(
        "/api/auth/login",
        json={"email": "admin", "password": "admin"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


def _register_and_login(client: TestClient, email: str, password: str = "password123") -> str:
    register_response = client.post(
        "/api/auth/register",
        json={
            "full_name": "Test User",
            "email": email,
            "password": password,
        },
    )
    assert register_response.status_code == 200

    login_response = client.post(
        "/api/auth/login",
        json={"email": email, "password": password},
    )
    assert login_response.status_code == 200
    return login_response.json()["access_token"]


def test_integrations_requires_authentication(client: TestClient) -> None:
    response = client.get("/api/integrations/status")
    assert response.status_code == 401


def test_admin_default_user_can_login(client: TestClient) -> None:
    response = client.post(
        "/api/auth/login",
        json={"email": "admin", "password": "admin"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["access_token"]
    assert payload["user"]["email"] == "admin"
    assert payload["user"]["role"] == "admin"


def test_integrations_status_reports_missing_configuration_for_new_user(client: TestClient) -> None:
    token = _register_and_login(client, "user1@example.com")

    response = client.get(
        "/api/integrations/status",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["credentials"]["fireflies_api_key_configured"] is False
    assert payload["credentials"]["gemini_api_key_configured"] is False
    assert payload["pipelines"]["notion_notes_creation"]["ready"] is False


def test_integrations_settings_for_admin_are_seeded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv(
        "NOTION_REDIRECT_URI",
        "http://localhost:8000/api/integrations/notion/callback",
    )
    clear_user_store_cache()
    get_settings.cache_clear()

    token = _login_admin(client)

    response = client.get(
        "/api/integrations/settings",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    fireflies_group = next(group for group in payload["groups"] if group["id"] == "fireflies")
    fireflies_key_field = next(
        field for field in fireflies_group["fields"] if field["env_var"] == "FIREFLIES_API_KEY"
    )
    assert fireflies_key_field["configured"] is False
    assert fireflies_key_field["sensitive"] is True
    assert fireflies_key_field["value"] is None
    autosync_field = next(
        field
        for field in fireflies_group["fields"]
        if field["env_var"] == "TRANSCRIPTION_AUTOSYNC_ENABLED"
    )
    assert autosync_field["configured"] is True
    assert autosync_field["value"] == "true"

    oauth_notion_group = next(group for group in payload["groups"] if group["id"] == "oauth_notion")
    notion_redirect_field = next(
        field for field in oauth_notion_group["fields"] if field["env_var"] == "NOTION_REDIRECT_URI"
    )
    assert notion_redirect_field["configured"] is True
    assert notion_redirect_field["value"] == "http://localhost:8000/api/integrations/notion/callback"


def test_integrations_settings_patch_updates_only_current_user(client: TestClient) -> None:
    admin_token = _login_admin(client)
    user_token = _register_and_login(client, "user2@example.com")

    patch_response = client.patch(
        "/api/integrations/settings",
        json={
            "values": {
                "NOTION_TASK_STATUS_PROPERTY": "Status",
                "FIREFLIES_API_KEY": "new-token",
            },
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert patch_response.status_code == 200
    assert patch_response.json()["updated_env_vars"] == [
        "FIREFLIES_API_KEY",
        "NOTION_TASK_STATUS_PROPERTY",
    ]

    admin_settings_response = client.get(
        "/api/integrations/settings",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert admin_settings_response.status_code == 200

    notion_group = next(
        group for group in admin_settings_response.json()["groups"] if group["id"] == "notion_sync"
    )
    status_field = next(
        field
        for field in notion_group["fields"]
        if field["env_var"] == "NOTION_TASK_STATUS_PROPERTY"
    )
    assert status_field["value"] == "Status"

    user_settings_response = client.get(
        "/api/integrations/settings",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert user_settings_response.status_code == 200
    user_notion_group = next(
        group for group in user_settings_response.json()["groups"] if group["id"] == "notion_sync"
    )
    user_status_field = next(
        field
        for field in user_notion_group["fields"]
        if field["env_var"] == "NOTION_TASK_STATUS_PROPERTY"
    )
    assert user_status_field["value"] in {"", None}


def test_integrations_settings_patch_validates_values(client: TestClient) -> None:
    token = _login_admin(client)

    managed_env_response = client.patch(
        "/api/integrations/settings",
        json={"values": {"TRANSCRIPTIONS_STORE": "memory"}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert managed_env_response.status_code == 422
    assert "managed by the platform" in managed_env_response.json()["detail"]

    invalid_field_response = client.patch(
        "/api/integrations/settings",
        json={"values": {"NOT_ALLOWED_ENV": "x"}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert invalid_field_response.status_code == 422

    invalid_date_response = client.patch(
        "/api/integrations/settings",
        json={"values": {"ACTION_ITEMS_TEST_DUE_DATE": "2026-31-12"}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert invalid_date_response.status_code == 422


def test_integrations_settings_patch_normalizes_transcription_autosync_toggle(
    client: TestClient,
) -> None:
    token = _login_admin(client)

    patch_response = client.patch(
        "/api/integrations/settings",
        json={"values": {"TRANSCRIPTION_AUTOSYNC_ENABLED": "off"}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert patch_response.status_code == 200

    settings_response = client.get(
        "/api/integrations/settings",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert settings_response.status_code == 200
    fireflies_group = next(
        group for group in settings_response.json()["groups"] if group["id"] == "fireflies"
    )
    autosync_field = next(
        field
        for field in fireflies_group["fields"]
        if field["env_var"] == "TRANSCRIPTION_AUTOSYNC_ENABLED"
    )
    assert autosync_field["value"] == "false"


def test_google_calendar_connect_redirects_to_google_oauth(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("GOOGLE_CALENDAR_CLIENT_ID", "google-client-id")
    monkeypatch.setenv("GOOGLE_CALENDAR_CLIENT_SECRET", "google-client-secret")
    monkeypatch.setenv(
        "GOOGLE_CALENDAR_REDIRECT_URI",
        "http://localhost:8000/api/integrations/google-calendar/callback",
    )
    clear_user_store_cache()
    get_settings.cache_clear()

    token = _login_admin(client)
    response = client.get(
        "/api/integrations/google-calendar/connect",
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith("https://accounts.google.com/o/oauth2/v2/auth?")
    assert "client_id=google-client-id" in location
    assert "scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcalendar.events" in location


def test_google_calendar_callback_stores_access_token_per_user(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("GOOGLE_CALENDAR_CLIENT_ID", "google-client-id")
    monkeypatch.setenv("GOOGLE_CALENDAR_CLIENT_SECRET", "google-client-secret")
    monkeypatch.setenv(
        "GOOGLE_CALENDAR_REDIRECT_URI",
        "http://localhost:8000/api/integrations/google-calendar/callback",
    )
    monkeypatch.setenv("FRONTEND_BASE_URL", "http://localhost:3000")
    clear_user_store_cache()
    get_settings.cache_clear()

    token = _login_admin(client)
    connect_response = client.get(
        "/api/integrations/google-calendar/connect",
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
    )
    assert connect_response.status_code == 302
    google_url = urlparse(connect_response.headers["location"])
    query = parse_qs(google_url.query)
    state = query["state"][0]

    class _MockGoogleTokenResponse:
        def __enter__(self) -> "_MockGoogleTokenResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
            return False

        def read(self) -> bytes:
            return b'{"access_token":"google-access-token","refresh_token":"google-refresh-token"}'

    monkeypatch.setattr(
        "app.api.routes.integrations.urlopen",
        lambda request, timeout=15: _MockGoogleTokenResponse(),
    )

    callback_response = client.get(
        f"/api/integrations/google-calendar/callback?code=test-code&state={state}",
        follow_redirects=False,
    )
    assert callback_response.status_code == 302
    assert callback_response.headers["location"].startswith(
        "http://localhost:3000/configuracion?google_calendar_oauth=success",
    )

    status_response = client.get(
        "/api/integrations/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert status_response.status_code == 200
    assert status_response.json()["credentials"]["google_calendar_api_token_configured"] is True

    user_store = create_user_store(get_settings())
    user_record = user_store.get_user_by_email("admin")
    assert user_record is not None
    user_values = user_store.get_user_settings_values(str(user_record["_id"]))
    assert user_values["GOOGLE_CALENDAR_API_TOKEN"] == "google-access-token"
    assert user_values["GOOGLE_CALENDAR_REFRESH_TOKEN"] == "google-refresh-token"


def test_notion_connect_redirects_to_notion_oauth(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("NOTION_CLIENT_ID", "notion-client-id")
    monkeypatch.setenv("NOTION_CLIENT_SECRET", "notion-client-secret")
    monkeypatch.setenv(
        "NOTION_REDIRECT_URI",
        "http://localhost:8000/api/integrations/notion/callback",
    )
    clear_user_store_cache()
    get_settings.cache_clear()

    token = _login_admin(client)
    response = client.get(
        "/api/integrations/notion/connect",
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith("https://api.notion.com/v1/oauth/authorize?")
    assert "client_id=notion-client-id" in location
    assert "owner=user" in location


def test_notion_callback_stores_access_token_per_user(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("NOTION_CLIENT_ID", "notion-client-id")
    monkeypatch.setenv("NOTION_CLIENT_SECRET", "notion-client-secret")
    monkeypatch.setenv(
        "NOTION_REDIRECT_URI",
        "http://localhost:8000/api/integrations/notion/callback",
    )
    monkeypatch.setenv("FRONTEND_BASE_URL", "http://localhost:3000")
    clear_user_store_cache()
    get_settings.cache_clear()

    token = _login_admin(client)
    connect_response = client.get(
        "/api/integrations/notion/connect",
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
    )
    assert connect_response.status_code == 302
    notion_url = urlparse(connect_response.headers["location"])
    query = parse_qs(notion_url.query)
    state = query["state"][0]

    class _MockNotionTokenResponse:
        def __enter__(self) -> "_MockNotionTokenResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
            return False

        def read(self) -> bytes:
            return b'{"access_token":"notion-access-token"}'

    monkeypatch.setattr(
        "app.api.routes.integrations.urlopen",
        lambda request, timeout=15: _MockNotionTokenResponse(),
    )

    callback_response = client.get(
        f"/api/integrations/notion/callback?code=test-code&state={state}",
        follow_redirects=False,
    )
    assert callback_response.status_code == 302
    assert callback_response.headers["location"].startswith(
        "http://localhost:3000/configuracion?notion_oauth=success",
    )

    status_response = client.get(
        "/api/integrations/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert status_response.status_code == 200
    assert status_response.json()["credentials"]["notion_api_token_configured"] is True


def test_outlook_calendar_connect_redirects_to_microsoft_oauth(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("OUTLOOK_CLIENT_ID", "outlook-client-id")
    monkeypatch.setenv("OUTLOOK_CLIENT_SECRET", "outlook-client-secret")
    monkeypatch.setenv("OUTLOOK_TENANT_ID", "common")
    monkeypatch.setenv(
        "OUTLOOK_REDIRECT_URI",
        "http://localhost:8000/api/integrations/outlook-calendar/callback",
    )
    clear_user_store_cache()
    get_settings.cache_clear()

    token = _login_admin(client)
    response = client.get(
        "/api/integrations/outlook-calendar/connect",
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith("https://login.microsoftonline.com/common/oauth2/v2.0/authorize?")
    assert "client_id=outlook-client-id" in location
    assert "scope=offline_access+Calendars.ReadWrite+User.Read" in location


def test_outlook_calendar_callback_stores_access_token_per_user(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    monkeypatch.setenv("OUTLOOK_CLIENT_ID", "outlook-client-id")
    monkeypatch.setenv("OUTLOOK_CLIENT_SECRET", "outlook-client-secret")
    monkeypatch.setenv("OUTLOOK_TENANT_ID", "common")
    monkeypatch.setenv(
        "OUTLOOK_REDIRECT_URI",
        "http://localhost:8000/api/integrations/outlook-calendar/callback",
    )
    monkeypatch.setenv("FRONTEND_BASE_URL", "http://localhost:3000")
    clear_user_store_cache()
    get_settings.cache_clear()

    token = _login_admin(client)
    connect_response = client.get(
        "/api/integrations/outlook-calendar/connect",
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
    )
    assert connect_response.status_code == 302
    outlook_url = urlparse(connect_response.headers["location"])
    query = parse_qs(outlook_url.query)
    state = query["state"][0]

    class _MockOutlookTokenResponse:
        def __enter__(self) -> "_MockOutlookTokenResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
            return False

        def read(self) -> bytes:
            return b'{"access_token":"outlook-access-token"}'

    monkeypatch.setattr(
        "app.api.routes.integrations.urlopen",
        lambda request, timeout=15: _MockOutlookTokenResponse(),
    )

    callback_response = client.get(
        f"/api/integrations/outlook-calendar/callback?code=test-code&state={state}",
        follow_redirects=False,
    )
    assert callback_response.status_code == 302
    assert callback_response.headers["location"].startswith(
        "http://localhost:3000/configuracion?outlook_calendar_oauth=success",
    )

    status_response = client.get(
        "/api/integrations/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert status_response.status_code == 200
    assert status_response.json()["credentials"]["outlook_calendar_api_token_configured"] is True
