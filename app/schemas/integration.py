from pydantic import BaseModel, Field


class IntegrationCredentialStatus(BaseModel):
    fireflies_api_key_configured: bool
    fireflies_webhook_secret_configured: bool
    read_ai_api_key_configured: bool
    gemini_api_key_configured: bool
    notion_api_token_configured: bool
    notion_tasks_database_id_configured: bool
    notion_calendar_database_id_configured: bool
    google_calendar_api_token_configured: bool
    notion_client_id_configured: bool
    notion_client_secret_configured: bool
    notion_redirect_uri_configured: bool
    google_calendar_client_id_configured: bool
    google_calendar_client_secret_configured: bool
    google_calendar_redirect_uri_configured: bool
    outlook_calendar_api_token_configured: bool
    outlook_client_id_configured: bool
    outlook_client_secret_configured: bool
    outlook_tenant_id_configured: bool
    outlook_redirect_uri_configured: bool


class IntegrationPipelineStatus(BaseModel):
    ready: bool
    missing_env_vars: list[str] = Field(default_factory=list)


class IntegrationPipelinesStatus(BaseModel):
    fireflies_transcript_enrichment: IntegrationPipelineStatus
    read_ai_transcript_enrichment: IntegrationPipelineStatus
    notion_notes_creation: IntegrationPipelineStatus
    notion_calendar_events_creation: IntegrationPipelineStatus
    google_calendar_due_date_events: IntegrationPipelineStatus
    outlook_calendar_due_date_events: IntegrationPipelineStatus
    notion_oauth_connection: IntegrationPipelineStatus
    google_calendar_oauth_connection: IntegrationPipelineStatus
    outlook_oauth_connection: IntegrationPipelineStatus


class IntegrationsStatusResponse(BaseModel):
    credentials: IntegrationCredentialStatus
    pipelines: IntegrationPipelinesStatus


class IntegrationSettingsField(BaseModel):
    env_var: str
    label: str
    description: str
    sensitive: bool
    configured: bool
    value: str | None = None
    required_for: list[str] = Field(default_factory=list)


class IntegrationSettingsGroup(BaseModel):
    id: str
    title: str
    description: str
    fields: list[IntegrationSettingsField] = Field(default_factory=list)


class IntegrationsSettingsResponse(BaseModel):
    groups: list[IntegrationSettingsGroup] = Field(default_factory=list)


class IntegrationsSettingsUpdateRequest(BaseModel):
    values: dict[str, str] = Field(default_factory=dict)


class IntegrationsSettingsUpdateResponse(BaseModel):
    updated_env_vars: list[str] = Field(default_factory=list)
    status: IntegrationsStatusResponse
    settings: IntegrationsSettingsResponse
