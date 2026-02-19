import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import app
from app.services.team_membership_store import clear_team_membership_store_cache
from app.services.user_store import clear_user_store_cache


@pytest.fixture(autouse=True)
def reset_team_membership_state(monkeypatch: pytest.MonkeyPatch) -> None:
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


def _register_user(
    client: TestClient,
    *,
    full_name: str,
    email: str,
    password: str = "password123",
) -> tuple[str, dict[str, object]]:
    register_response = client.post(
        "/api/auth/register",
        json={
            "full_name": full_name,
            "email": email,
            "password": password,
        },
    )
    assert register_response.status_code == 200
    payload = register_response.json()
    return payload["access_token"], payload["user"]


def test_team_membership_invitation_and_recipients_flow(client: TestClient) -> None:
    lead_token, lead_user = _register_user(
        client,
        full_name="Lead User",
        email="lead@example.com",
    )
    member_token, member_user = _register_user(
        client,
        full_name="Member User",
        email="member@example.com",
    )

    create_team_response = client.post(
        "/api/team-memberships/teams",
        json={"name": "Equipo Tecnologia"},
        headers={"Authorization": f"Bearer {lead_token}"},
    )
    assert create_team_response.status_code == 201
    team = create_team_response.json()
    team_id = team["id"]
    assert team["name"] == "Equipo Tecnologia"
    assert team["can_manage"] is True
    assert [recipient["email"] for recipient in team["recipients"]] == ["lead@example.com"]

    invite_response = client.post(
        f"/api/team-memberships/teams/{team_id}/invitations",
        json={"email": "member@example.com"},
        headers={"Authorization": f"Bearer {lead_token}"},
    )
    assert invite_response.status_code == 201
    invitation = invite_response.json()
    invitation_id = invitation["id"]
    assert invitation["status"] == "pending"
    assert invitation["team_id"] == team_id
    assert invitation["invited_email"] == "member@example.com"

    member_configuration_response = client.get(
        "/api/team-memberships/configuration",
        headers={"Authorization": f"Bearer {member_token}"},
    )
    assert member_configuration_response.status_code == 200
    pending_invitations = member_configuration_response.json()["pending_invitations"]
    assert len(pending_invitations) == 1
    assert pending_invitations[0]["id"] == invitation_id

    accept_response = client.post(
        f"/api/team-memberships/invitations/{invitation_id}/accept",
        headers={"Authorization": f"Bearer {member_token}"},
    )
    assert accept_response.status_code == 200
    assert accept_response.json()["status"] == "accepted"

    update_recipients_response = client.patch(
        f"/api/team-memberships/teams/{team_id}/recipients",
        json={"recipient_user_ids": [lead_user["id"], member_user["id"]]},
        headers={"Authorization": f"Bearer {lead_token}"},
    )
    assert update_recipients_response.status_code == 200
    recipients = update_recipients_response.json()["recipients"]
    recipient_emails = sorted(recipient["email"] for recipient in recipients)
    assert recipient_emails == ["lead@example.com", "member@example.com"]

    lead_configuration_response = client.get(
        "/api/team-memberships/configuration",
        headers={"Authorization": f"Bearer {lead_token}"},
    )
    assert lead_configuration_response.status_code == 200
    teams = lead_configuration_response.json()["teams"]
    assert len(teams) == 1
    members = teams[0]["members"]
    member_emails = sorted(member["user"]["email"] for member in members)
    assert member_emails == ["lead@example.com", "member@example.com"]


def test_non_lead_cannot_manage_team_recipients(client: TestClient) -> None:
    lead_token, _ = _register_user(
        client,
        full_name="Lead User",
        email="lead@example.com",
    )
    member_token, _ = _register_user(
        client,
        full_name="Member User",
        email="member@example.com",
    )

    create_team_response = client.post(
        "/api/team-memberships/teams",
        json={"name": "Equipo Producto"},
        headers={"Authorization": f"Bearer {lead_token}"},
    )
    assert create_team_response.status_code == 201
    team_id = create_team_response.json()["id"]

    invite_response = client.post(
        f"/api/team-memberships/teams/{team_id}/invitations",
        json={"email": "member@example.com"},
        headers={"Authorization": f"Bearer {lead_token}"},
    )
    assert invite_response.status_code == 201
    invitation_id = invite_response.json()["id"]

    accept_response = client.post(
        f"/api/team-memberships/invitations/{invitation_id}/accept",
        headers={"Authorization": f"Bearer {member_token}"},
    )
    assert accept_response.status_code == 200

    update_recipients_response = client.patch(
        f"/api/team-memberships/teams/{team_id}/recipients",
        json={"recipient_user_ids": []},
        headers={"Authorization": f"Bearer {member_token}"},
    )
    assert update_recipients_response.status_code == 403
    assert update_recipients_response.json()["detail"] == "Only team leads can manage this team."


def test_user_can_toggle_team_activation_for_own_membership(client: TestClient) -> None:
    lead_token, _ = _register_user(
        client,
        full_name="Lead User",
        email="lead.toggle@example.com",
    )
    member_token, member_user = _register_user(
        client,
        full_name="Member User",
        email="member.toggle@example.com",
    )

    create_team_response = client.post(
        "/api/team-memberships/teams",
        json={"name": "Equipo Activacion"},
        headers={"Authorization": f"Bearer {lead_token}"},
    )
    assert create_team_response.status_code == 201
    team_id = create_team_response.json()["id"]

    invite_response = client.post(
        f"/api/team-memberships/teams/{team_id}/invitations",
        json={"email": "member.toggle@example.com"},
        headers={"Authorization": f"Bearer {lead_token}"},
    )
    assert invite_response.status_code == 201
    invitation_id = invite_response.json()["id"]

    accept_response = client.post(
        f"/api/team-memberships/invitations/{invitation_id}/accept",
        headers={"Authorization": f"Bearer {member_token}"},
    )
    assert accept_response.status_code == 200

    disable_response = client.patch(
        f"/api/team-memberships/teams/{team_id}/activation",
        json={"is_active": False},
        headers={"Authorization": f"Bearer {member_token}"},
    )
    assert disable_response.status_code == 200
    disabled_team = disable_response.json()
    assert disabled_team["current_user_is_active"] is False
    disabled_member = next(
        member
        for member in disabled_team["members"]
        if member["user"]["user_id"] == member_user["id"]
    )
    assert disabled_member["is_active"] is False

    enable_response = client.patch(
        f"/api/team-memberships/teams/{team_id}/activation",
        json={"is_active": True},
        headers={"Authorization": f"Bearer {member_token}"},
    )
    assert enable_response.status_code == 200
    assert enable_response.json()["current_user_is_active"] is True
