from fastapi import APIRouter, Depends, status

from app.schemas.auth import CurrentUserResponse
from app.schemas.team_membership import (
    TeamConfiguration,
    TeamCreateRequest,
    TeamInvitation,
    TeamInviteRequest,
    TeamMembershipActivationUpdateRequest,
    TeamMembershipConfigurationResponse,
    TeamRecipientsUpdateRequest,
)
from app.services.auth_service import require_current_user
from app.services.team_membership_service import TeamMembershipService

router = APIRouter(prefix="/team-memberships", tags=["team-memberships"])


@router.get(
    "/configuration",
    response_model=TeamMembershipConfigurationResponse,
)
def get_team_membership_configuration(
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> TeamMembershipConfigurationResponse:
    service = TeamMembershipService()
    return service.get_configuration(current_user)


@router.post(
    "/teams",
    response_model=TeamConfiguration,
    status_code=status.HTTP_201_CREATED,
)
def create_team(
    payload: TeamCreateRequest,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> TeamConfiguration:
    service = TeamMembershipService()
    return service.create_team(current_user=current_user, name=payload.name)


@router.patch(
    "/teams/{team_id}/recipients",
    response_model=TeamConfiguration,
)
def update_team_recipients(
    team_id: str,
    payload: TeamRecipientsUpdateRequest,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> TeamConfiguration:
    service = TeamMembershipService()
    return service.update_team_recipients(
        current_user=current_user,
        team_id=team_id,
        payload=payload,
    )


@router.patch(
    "/teams/{team_id}/activation",
    response_model=TeamConfiguration,
)
def update_current_user_team_activation(
    team_id: str,
    payload: TeamMembershipActivationUpdateRequest,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> TeamConfiguration:
    service = TeamMembershipService()
    return service.update_current_user_membership_activation(
        current_user=current_user,
        team_id=team_id,
        payload=payload,
    )


@router.post(
    "/teams/{team_id}/invitations",
    response_model=TeamInvitation,
    status_code=status.HTTP_201_CREATED,
)
def invite_member_to_team(
    team_id: str,
    payload: TeamInviteRequest,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> TeamInvitation:
    service = TeamMembershipService()
    return service.invite_member(
        current_user=current_user,
        team_id=team_id,
        invited_email=payload.email,
    )


@router.post(
    "/invitations/{invitation_id}/accept",
    response_model=TeamInvitation,
)
def accept_team_invitation(
    invitation_id: str,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> TeamInvitation:
    service = TeamMembershipService()
    return service.accept_invitation(
        current_user=current_user,
        invitation_id=invitation_id,
    )


@router.post(
    "/invitations/{invitation_id}/decline",
    response_model=TeamInvitation,
)
def decline_team_invitation(
    invitation_id: str,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> TeamInvitation:
    service = TeamMembershipService()
    return service.decline_invitation(
        current_user=current_user,
        invitation_id=invitation_id,
    )
