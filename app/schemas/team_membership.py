from datetime import datetime

from pydantic import BaseModel, Field


class TeamUserSummary(BaseModel):
    user_id: str
    email: str
    full_name: str


class TeamMember(BaseModel):
    user: TeamUserSummary
    role: str
    status: str
    is_recipient: bool = False
    is_active: bool = True


class TeamInvitation(BaseModel):
    id: str
    team_id: str
    team_name: str | None = None
    invited_email: str
    invited_by_user_id: str
    invited_by_email: str | None = None
    status: str
    created_at: datetime
    updated_at: datetime


class TeamConfiguration(BaseModel):
    id: str
    name: str
    created_by_user_id: str
    can_manage: bool
    is_active: bool = True
    current_user_is_active: bool = True
    recipients: list[TeamUserSummary] = Field(default_factory=list)
    members: list[TeamMember] = Field(default_factory=list)
    pending_invitations: list[TeamInvitation] = Field(default_factory=list)


class TeamMembershipConfigurationResponse(BaseModel):
    teams: list[TeamConfiguration] = Field(default_factory=list)
    pending_invitations: list[TeamInvitation] = Field(default_factory=list)


class TeamCreateRequest(BaseModel):
    name: str


class TeamInviteRequest(BaseModel):
    email: str


class TeamRecipientsUpdateRequest(BaseModel):
    recipient_user_ids: list[str] = Field(default_factory=list)


class TeamMembershipActivationUpdateRequest(BaseModel):
    is_active: bool = True
