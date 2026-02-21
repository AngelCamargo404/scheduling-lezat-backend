from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException, status

from app.core.config import Settings, get_settings
from app.schemas.auth import CurrentUserResponse
from app.schemas.team_membership import (
    TeamConfiguration,
    TeamMembershipActivationUpdateRequest,
    TeamInvitation,
    TeamMembershipConfigurationResponse,
    TeamMember,
    TeamRecipientsUpdateRequest,
    TeamUserSummary,
)
from app.services.team_membership_store import (
    TeamMembershipStore,
    create_team_membership_store,
)
from app.services.user_store import UserStore, create_user_store


class TeamMembershipService:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        user_store: UserStore | None = None,
        team_store: TeamMembershipStore | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.user_store = user_store or create_user_store(self.settings)
        self.team_store = team_store or create_team_membership_store(self.settings)

    def get_configuration(self, current_user: CurrentUserResponse) -> TeamMembershipConfigurationResponse:
        memberships = self.team_store.list_memberships_for_user(
            current_user.id,
            status="accepted",
        )
        team_ids = sorted(
            {
                str(membership.get("team_id", "")).strip()
                for membership in memberships
                if str(membership.get("team_id", "")).strip()
            },
        )
        teams = self.team_store.list_teams_by_ids(team_ids)
        team_models = [
            self._build_team_configuration(
                team=team,
                current_user_id=current_user.id,
            )
            for team in teams
        ]
        pending_invitations = self.team_store.list_pending_invitations_for_email(current_user.email)
        pending_invitation_models = [
            self._map_invitation(invitation)
            for invitation in pending_invitations
        ]
        return TeamMembershipConfigurationResponse(
            teams=team_models,
            pending_invitations=pending_invitation_models,
        )

    def create_team(self, *, current_user: CurrentUserResponse, name: str) -> TeamConfiguration:
        normalized_name = name.strip()
        if len(normalized_name) < 2:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Team name must contain at least 2 characters.",
            )

        team = self.team_store.create_team(
            name=normalized_name,
            created_by_user_id=current_user.id,
            recipient_user_ids=[current_user.id],
        )
        self.team_store.upsert_membership(
            team_id=str(team.get("_id", "")),
            user_id=current_user.id,
            role="lead",
            status="accepted",
        )
        return self._build_team_configuration(
            team=team,
            current_user_id=current_user.id,
        )

    def invite_member(
        self,
        *,
        current_user: CurrentUserResponse,
        team_id: str,
        invited_email: str,
    ) -> TeamInvitation:
        normalized_team_id = team_id.strip()
        team = self.team_store.get_team(normalized_team_id)
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found.",
            )
        self._assert_user_can_manage_team(current_user.id, normalized_team_id)

        normalized_email = invited_email.strip().lower()
        if "@" not in normalized_email:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid invitation email.",
            )

        existing_user = self.user_store.get_user_by_email(normalized_email)
        if existing_user:
            existing_user_id = str(existing_user.get("_id", "")).strip()
            if existing_user_id:
                memberships = self.team_store.list_memberships_for_team(normalized_team_id, status="accepted")
                for membership in memberships:
                    if str(membership.get("user_id", "")).strip() != existing_user_id:
                        continue
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="User is already a member of this team.",
                    )

        invitation = self.team_store.create_or_get_pending_invitation(
            team_id=normalized_team_id,
            invited_email=normalized_email,
            invited_by_user_id=current_user.id,
        )
        return self._map_invitation(invitation)

    def accept_invitation(
        self,
        *,
        current_user: CurrentUserResponse,
        invitation_id: str,
    ) -> TeamInvitation:
        invitation = self.team_store.get_invitation(invitation_id.strip())
        if not invitation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invitation not found.",
            )

        invitation_status = str(invitation.get("status", "")).strip().lower()
        if invitation_status != "pending":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Invitation has already been processed.",
            )

        invited_email = str(invitation.get("invited_email", "")).strip().lower()
        if invited_email != current_user.email.strip().lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only accept invitations sent to your email.",
            )

        team_id = str(invitation.get("team_id", "")).strip()
        team = self.team_store.get_team(team_id)
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team for invitation was not found.",
            )

        self.team_store.upsert_membership(
            team_id=team_id,
            user_id=current_user.id,
            role="member",
            status="accepted",
            invited_by_user_id=str(invitation.get("invited_by_user_id", "")).strip() or None,
        )
        updated_invitation = self.team_store.update_invitation_status(
            invitation_id=str(invitation.get("_id", "")).strip(),
            status="accepted",
            responded_by_user_id=current_user.id,
        )
        if not updated_invitation:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to update invitation status.",
            )
        return self._map_invitation(updated_invitation)

    def decline_invitation(
        self,
        *,
        current_user: CurrentUserResponse,
        invitation_id: str,
    ) -> TeamInvitation:
        invitation = self.team_store.get_invitation(invitation_id.strip())
        if not invitation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invitation not found.",
            )
        invitation_status = str(invitation.get("status", "")).strip().lower()
        if invitation_status != "pending":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Invitation has already been processed.",
            )
        invited_email = str(invitation.get("invited_email", "")).strip().lower()
        if invited_email != current_user.email.strip().lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only decline invitations sent to your email.",
            )

        updated_invitation = self.team_store.update_invitation_status(
            invitation_id=str(invitation.get("_id", "")).strip(),
            status="declined",
            responded_by_user_id=current_user.id,
        )
        if not updated_invitation:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to update invitation status.",
            )
        return self._map_invitation(updated_invitation)

    def update_team_recipients(
        self,
        *,
        current_user: CurrentUserResponse,
        team_id: str,
        payload: TeamRecipientsUpdateRequest,
    ) -> TeamConfiguration:
        normalized_team_id = team_id.strip()
        self._assert_user_can_manage_team(current_user.id, normalized_team_id)

        team = self.team_store.get_team(normalized_team_id)
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found.",
            )

        accepted_memberships = self.team_store.list_memberships_for_team(
            normalized_team_id,
            status="accepted",
        )
        allowed_user_ids = {
            str(membership.get("user_id", "")).strip()
            for membership in accepted_memberships
            if str(membership.get("user_id", "")).strip()
        }
        recipient_user_ids = sorted(
            {
                user_id.strip()
                for user_id in payload.recipient_user_ids
                if user_id and user_id.strip()
            },
        )
        invalid_recipient_user_ids = [
            user_id
            for user_id in recipient_user_ids
            if user_id not in allowed_user_ids
        ]
        if invalid_recipient_user_ids:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "All recipients must be accepted members of the team. "
                    f"Invalid ids: {', '.join(invalid_recipient_user_ids)}"
                ),
            )

        updated_team = self.team_store.set_team_recipients(
            normalized_team_id,
            recipient_user_ids,
        )
        if not updated_team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found.",
            )
        return self._build_team_configuration(
            team=updated_team,
            current_user_id=current_user.id,
        )

    def update_team_activation(
        self,
        *,
        current_user: CurrentUserResponse,
        team_id: str,
        payload: TeamMembershipActivationUpdateRequest,
    ) -> TeamConfiguration:
        normalized_team_id = team_id.strip()
        team = self.team_store.get_team(normalized_team_id)
        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found.",
            )
        self._assert_user_can_manage_team(current_user.id, normalized_team_id)

        updated_team = self.team_store.set_team_activation(
            normalized_team_id,
            payload.is_active,
        )
        if not updated_team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Team not found.",
            )
        return self._build_team_configuration(
            team=updated_team,
            current_user_id=current_user.id,
        )

    def resolve_team_recipients_for_participants(
        self,
        *,
        participant_emails: list[str],
        lead_user_id: str | None = None,
    ) -> tuple[list[str], list[str]]:
        normalized_lead_user_id = (lead_user_id or "").strip()
        normalized_participant_emails = sorted(
            {
                email.strip().lower()
                for email in participant_emails
                if email and "@" in email
            },
        )
        participant_user_ids: set[str] = set()
        if normalized_participant_emails:
            for participant_email in normalized_participant_emails:
                participant_user = self.user_store.get_user_by_email(participant_email)
                if not participant_user:
                    continue
                participant_user_id = str(participant_user.get("_id", "")).strip()
                if participant_user_id:
                    participant_user_ids.add(participant_user_id)

        matched_teams: list[dict[str, object]] = []
        if participant_user_ids:
            matched_teams = self.team_store.list_teams_matching_participants(
                participant_user_ids=sorted(participant_user_ids),
                lead_user_id=normalized_lead_user_id or None,
            )
        if not matched_teams and normalized_lead_user_id:
            # Webhooks scoped by /{user_id} should still route to the lead's team
            # even when participant emails are missing or external-only.
            matched_teams = self._list_teams_led_by_user(normalized_lead_user_id)
        recipient_user_ids, matched_team_ids = self._collect_active_recipients_from_teams(matched_teams)
        if recipient_user_ids or matched_team_ids:
            return recipient_user_ids, matched_team_ids

        if normalized_lead_user_id:
            # If participant matching only produced disabled teams, retry with all
            # teams led by the scoped user so enabled teams still receive notes.
            lead_teams = self._list_teams_led_by_user(normalized_lead_user_id)
            return self._collect_active_recipients_from_teams(lead_teams)

        return [], []

    def _collect_active_recipients_from_teams(
        self,
        teams: list[dict[str, object]],
    ) -> tuple[list[str], list[str]]:
        if not teams:
            return [], []
        matched_team_ids: list[str] = []
        recipient_user_ids: set[str] = set()
        for team in teams:
            team_id = str(team.get("_id", "")).strip()
            if not team_id:
                continue
            if not bool(team.get("is_active", True)):
                continue
            matched_team_ids.append(team_id)
            accepted_memberships = self.team_store.list_memberships_for_team(team_id, status="accepted")
            active_accepted_user_ids = {
                str(membership.get("user_id", "")).strip()
                for membership in accepted_memberships
                if str(membership.get("user_id", "")).strip()
                and bool(membership.get("is_active", True))
            }
            configured_recipient_user_ids = {
                user_id.strip()
                for user_id in team.get("recipient_user_ids", [])
                if isinstance(user_id, str) and user_id.strip()
            }
            configured_recipients = sorted(configured_recipient_user_ids & active_accepted_user_ids)
            if configured_recipients:
                recipient_user_ids.update(configured_recipients)

        return sorted(recipient_user_ids), sorted(set(matched_team_ids))

    def _list_teams_led_by_user(self, lead_user_id: str) -> list[dict[str, object]]:
        memberships = self.team_store.list_memberships_for_user(
            lead_user_id,
            status="accepted",
        )
        lead_team_ids = sorted(
            {
                str(membership.get("team_id", "")).strip()
                for membership in memberships
                if str(membership.get("role", "")).strip().lower() == "lead"
                and str(membership.get("team_id", "")).strip()
            },
        )
        if not lead_team_ids:
            return []
        return self.team_store.list_teams_by_ids(lead_team_ids)

    def _assert_user_can_manage_team(self, user_id: str, team_id: str) -> None:
        normalized_user_id = user_id.strip()
        normalized_team_id = team_id.strip()
        if not normalized_team_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="team_id is required.",
            )
        memberships = self.team_store.list_memberships_for_team(
            normalized_team_id,
            status="accepted",
        )
        for membership in memberships:
            membership_user_id = str(membership.get("user_id", "")).strip()
            if membership_user_id != normalized_user_id:
                continue
            if str(membership.get("role", "")).strip().lower() == "lead":
                return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only team leads can manage this team.",
        )

    def _build_team_configuration(
        self,
        *,
        team: dict[str, object],
        current_user_id: str,
    ) -> TeamConfiguration:
        team_id = str(team.get("_id", "")).strip()
        memberships = self.team_store.list_memberships_for_team(team_id, status="accepted")
        membership_by_user_id = {
            str(membership.get("user_id", "")).strip(): membership
            for membership in memberships
            if str(membership.get("user_id", "")).strip()
        }
        can_manage = False
        current_user_is_active = True
        team_is_active = bool(team.get("is_active", True))
        current_membership = membership_by_user_id.get(current_user_id)
        if current_membership:
            current_user_is_active = bool(current_membership.get("is_active", True))
            if str(current_membership.get("role", "")).strip().lower() == "lead":
                can_manage = True

        recipient_user_ids = sorted(
            {
                user_id.strip()
                for user_id in team.get("recipient_user_ids", [])
                if isinstance(user_id, str) and user_id.strip()
            },
        )
        recipient_set = set(recipient_user_ids)

        members: list[TeamMember] = []
        for member_user_id, membership in membership_by_user_id.items():
            user_summary = self._to_user_summary(member_user_id)
            if not user_summary:
                continue
            members.append(
                TeamMember(
                    user=user_summary,
                    role=str(membership.get("role", "member")),
                    status=str(membership.get("status", "accepted")),
                    is_recipient=member_user_id in recipient_set,
                    is_active=bool(membership.get("is_active", True)),
                ),
            )
        members.sort(key=lambda member: member.user.full_name.lower())

        recipients: list[TeamUserSummary] = []
        for recipient_user_id in recipient_user_ids:
            recipient_user = self._to_user_summary(recipient_user_id)
            if recipient_user:
                recipients.append(recipient_user)

        pending_invitations: list[TeamInvitation] = []
        if can_manage:
            invitations = self.team_store.list_pending_invitations_for_team(team_id)
            pending_invitations = [self._map_invitation(invitation) for invitation in invitations]

        return TeamConfiguration(
            id=team_id,
            name=str(team.get("name", "")).strip(),
            created_by_user_id=str(team.get("created_by_user_id", "")).strip(),
            can_manage=can_manage,
            is_active=team_is_active,
            current_user_is_active=current_user_is_active,
            recipients=recipients,
            members=members,
            pending_invitations=pending_invitations,
        )

    def _to_user_summary(self, user_id: str) -> TeamUserSummary | None:
        normalized_user_id = user_id.strip()
        if not normalized_user_id:
            return None
        user_record = self.user_store.get_user_by_id(normalized_user_id)
        if not user_record:
            return None
        user_email = str(user_record.get("email", "")).strip().lower()
        if not user_email:
            return None
        user_full_name = str(user_record.get("full_name", "")).strip() or user_email
        return TeamUserSummary(
            user_id=normalized_user_id,
            email=user_email,
            full_name=user_full_name,
        )

    def _map_invitation(self, invitation: dict[str, object]) -> TeamInvitation:
        team_id = str(invitation.get("team_id", "")).strip()
        team_name: str | None = None
        team_record = self.team_store.get_team(team_id)
        if team_record:
            normalized_team_name = str(team_record.get("name", "")).strip()
            if normalized_team_name:
                team_name = normalized_team_name

        invited_by_user_id = str(invitation.get("invited_by_user_id", "")).strip()
        invited_by_email: str | None = None
        if invited_by_user_id:
            invited_by_user = self.user_store.get_user_by_id(invited_by_user_id)
            if invited_by_user:
                normalized_email = str(invited_by_user.get("email", "")).strip().lower()
                if normalized_email:
                    invited_by_email = normalized_email

        created_at = invitation.get("created_at")
        if not isinstance(created_at, datetime):
            created_at = datetime.now(UTC)
        updated_at = invitation.get("updated_at")
        if not isinstance(updated_at, datetime):
            updated_at = created_at

        return TeamInvitation(
            id=str(invitation.get("_id", "")).strip(),
            team_id=team_id,
            team_name=team_name,
            invited_email=str(invitation.get("invited_email", "")).strip().lower(),
            invited_by_user_id=invited_by_user_id,
            invited_by_email=invited_by_email,
            status=str(invitation.get("status", "pending")).strip().lower(),
            created_at=created_at,
            updated_at=updated_at,
        )
