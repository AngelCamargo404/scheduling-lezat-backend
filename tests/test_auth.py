import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import app
from app.services.team_membership_store import clear_team_membership_store_cache
from app.services.user_store import clear_user_store_cache


@pytest.fixture(autouse=True)
def reset_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USER_DATA_STORE", "memory")

    clear_user_store_cache()
    clear_team_membership_store_cache()
    get_settings.cache_clear()
    yield
    clear_user_store_cache()
    clear_team_membership_store_cache()
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_register_login_and_me_flow(client: TestClient) -> None:
    register_response = client.post(
        "/api/auth/register",
        json={
            "full_name": "Test User",
            "email": "test@example.com",
            "password": "password123",
        },
    )

    assert register_response.status_code == 200
    register_payload = register_response.json()
    assert register_payload["access_token"]
    assert register_payload["user"]["email"] == "test@example.com"

    login_response = client.post(
        "/api/auth/login",
        json={"email": "test@example.com", "password": "password123"},
    )
    assert login_response.status_code == 200
    token = login_response.json()["access_token"]

    me_response = client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me_response.status_code == 200
    me_payload = me_response.json()
    assert me_payload["email"] == "test@example.com"
    assert me_payload["role"] == "user"


def test_login_fails_with_invalid_credentials(client: TestClient) -> None:
    response = client.post(
        "/api/auth/login",
        json={"email": "admin", "password": "wrong-password"},
    )
    assert response.status_code == 401


def test_me_requires_authentication(client: TestClient) -> None:
    response = client.get("/api/auth/me")
    assert response.status_code == 401
