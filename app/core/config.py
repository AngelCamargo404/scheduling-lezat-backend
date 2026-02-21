from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ALLOWED_ENV_FIELD_NAMES = frozenset(
    {
        "app_name",
        "app_env",
        "app_version",
        "api_prefix",
        "allowed_origins",
        "fireflies_api_url",
        "fireflies_api_timeout_seconds",
        "fireflies_api_user_agent",
        "transcriptions_store",
        "mongodb_uri",
        "mongodb_db_name",
        "mongodb_transcriptions_collection",
        "mongodb_connect_timeout_ms",
        "gemini_api_key",
        "gemini_model",
        "gemini_api_timeout_seconds",
        "frontend_base_url",
        "notion_api_timeout_seconds",
        "notion_api_version",
        "notion_client_id",
        "notion_client_secret",
        "notion_redirect_uri",
        "google_calendar_client_id",
        "google_calendar_client_secret",
        "google_calendar_redirect_uri",
        "google_calendar_event_timezone",
        "google_calendar_id",
        "google_calendar_api_timeout_seconds",
        "team_leader_timezone",
        "outlook_client_id",
        "outlook_client_secret",
        "outlook_tenant_id",
        "outlook_redirect_uri",
        "outlook_calendar_refresh_token",
        "outlook_calendar_event_timezone",
        "monday_api_url",
        "monday_api_timeout_seconds",
        "monday_client_id",
        "monday_client_secret",
        "monday_redirect_uri",
        "monday_api_token",
        "monday_board_id",
        "monday_group_id",
        "monday_status_column_id",
        "monday_kanban_todo_status",
        "monday_assignee_column_id",
        "monday_due_date_column_id",
        "monday_details_column_id",
        "monday_meeting_id_column_id",
        "mongodb_user_settings_collection",
        "mongodb_teams_collection",
        "mongodb_team_memberships_collection",
        "mongodb_team_invitations_collection",
        "mongodb_users_collection",
        "user_data_store",
        "auth_google_client_id",
        "auth_google_redirect_uri",
        "auth_google_client_secret",
    },
)


PRODUCTION_FRONTEND_BASE_URL = "https://abundant-balance-production-9587.up.railway.app"
PRODUCTION_BACKEND_BASE_URL = "https://scheduling-lezat-backend-production.up.railway.app"
LOCAL_FRONTEND_BASE_URL = "http://localhost:3000"
LOCAL_FRONTEND_LOOPBACK_BASE_URL = "http://127.0.0.1:3000"
LOCAL_BACKEND_BASE_URL = "http://localhost:8000"
PRODUCTION_APP_ENVS = frozenset({"production", "prod"})


def _normalize_app_env_value(value: str | None) -> str:
    if value is None:
        return "development"
    normalized = value.strip().lower()
    return normalized or "development"


def _is_production_env(app_env: str) -> bool:
    return _normalize_app_env_value(app_env) in PRODUCTION_APP_ENVS


def _default_frontend_base_url(app_env: str) -> str:
    if _is_production_env(app_env):
        return PRODUCTION_FRONTEND_BASE_URL
    return LOCAL_FRONTEND_BASE_URL


def _default_backend_base_url(app_env: str) -> str:
    if _is_production_env(app_env):
        return PRODUCTION_BACKEND_BASE_URL
    return LOCAL_BACKEND_BASE_URL


def _default_allowed_origins(app_env: str) -> list[str]:
    if _is_production_env(app_env):
        return [PRODUCTION_FRONTEND_BASE_URL]
    return [LOCAL_FRONTEND_BASE_URL, LOCAL_FRONTEND_LOOPBACK_BASE_URL]


class Settings(BaseSettings):
    app_name: str = "Lezat Scheduling API"
    app_env: str = "development"
    app_version: str = "0.1.0"
    api_prefix: str = "/api"
    allowed_origins: Annotated[list[str], NoDecode] = Field(default_factory=list)
    fireflies_webhook_secret: str = ""
    fireflies_api_url: str = "https://api.fireflies.ai/graphql"
    fireflies_api_key: str = ""
    fireflies_api_timeout_seconds: float = 10.0
    fireflies_api_user_agent: str = "LezatSchedulingBackend/1.0"
    read_ai_api_url: str = "https://api.read.ai/v1"
    read_ai_api_key: str = ""
    read_ai_api_timeout_seconds: float = 10.0
    read_ai_api_user_agent: str = "LezatSchedulingBackend/1.0"
    transcription_autosync_enabled: bool = True
    transcriptions_store: str = "mongodb"
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db_name: str = "lezat_scheduling"
    mongodb_transcriptions_collection: str = "transcriptions"
    mongodb_action_item_creations_collection: str = "action_item_creations"
    mongodb_users_collection: str = "users"
    mongodb_user_settings_collection: str = "user_integration_settings"
    mongodb_teams_collection: str = "teams"
    mongodb_team_memberships_collection: str = "team_memberships"
    mongodb_team_invitations_collection: str = "team_invitations"
    mongodb_connect_timeout_ms: int = 2000
    user_data_store: str = "mongodb"
    force_user_settings_user_id: str = ""
    auth_secret_key: str = "change-me-in-production"
    auth_token_ttl_minutes: int = 60 * 12
    default_admin_email: str = "admin"
    default_admin_password: str = "admin"
    default_admin_full_name: str = "Administrator"
    auth_google_client_id: str = ""
    auth_google_client_secret: str = ""
    auth_google_redirect_uri: str = ""
    gemini_api_key: str = ""
    gemini_model: str = "gemini-3-flash-preview"
    gemini_api_timeout_seconds: float = 20.0
    frontend_base_url: str = ""
    notion_api_token: str = ""
    notion_tasks_database_id: str = ""
    notion_api_timeout_seconds: float = 10.0
    notion_api_version: str = "2022-06-28"
    notion_kanban_todo_status: str = "Por hacer"
    notion_task_title_property: str = "Name"
    notion_task_assignee_property: str = "Assignee"
    notion_task_status_property: str = "Status"
    notion_task_due_date_property: str = "Due date"
    notion_task_details_property: str = "Details"
    notion_task_meeting_id_property: str = "Meeting ID"
    action_items_test_mode_enabled: bool = False
    action_items_test_due_date: str = ""
    notion_client_id: str = ""
    notion_client_secret: str = ""
    notion_redirect_uri: str = ""
    google_calendar_client_id: str = ""
    google_calendar_client_secret: str = ""
    google_calendar_redirect_uri: str = ""
    google_calendar_api_token: str = ""
    google_calendar_refresh_token: str = ""
    google_calendar_id: str = "primary"
    google_calendar_api_timeout_seconds: float = 10.0
    google_calendar_event_timezone: str = "UTC"
    team_leader_timezone: str = "America/Bogota"
    outlook_client_id: str = ""
    outlook_client_secret: str = ""
    outlook_tenant_id: str = "common"
    outlook_redirect_uri: str = ""
    outlook_calendar_api_token: str = ""
    outlook_calendar_refresh_token: str = ""
    outlook_calendar_event_timezone: str = "UTC"
    monday_api_url: str = "https://api.monday.com/v2"
    monday_api_timeout_seconds: float = 10.0
    monday_client_id: str = ""
    monday_client_secret: str = ""
    monday_redirect_uri: str = ""
    monday_api_token: str = ""
    monday_board_id: str = ""
    monday_group_id: str = ""
    monday_status_column_id: str = "status"
    monday_kanban_todo_status: str = "Working on it"
    monday_assignee_column_id: str = "person"
    monday_due_date_column_id: str = "date"
    monday_details_column_id: str = "long_text"
    monday_meeting_id_column_id: str = "text"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        def _filter_allowed_env_fields(source):
            return {
                field_name: raw_value
                for field_name, raw_value in source().items()
                if field_name in ALLOWED_ENV_FIELD_NAMES
            }

        return (
            init_settings,
            lambda: _filter_allowed_env_fields(env_settings),
            lambda: _filter_allowed_env_fields(dotenv_settings),
            file_secret_settings,
        )

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def parse_allowed_origins(cls, value: str | list[str]) -> list[str]:
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @field_validator("app_env", mode="before")
    @classmethod
    def normalize_app_env(cls, value: str | None) -> str:
        return _normalize_app_env_value(value)

    @field_validator("transcriptions_store", mode="before")
    @classmethod
    def normalize_transcriptions_store(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("user_data_store", mode="before")
    @classmethod
    def normalize_user_data_store(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("fireflies_api_timeout_seconds", mode="before")
    @classmethod
    def normalize_fireflies_timeout(cls, value: float | str) -> float:
        parsed_value = float(value)
        if parsed_value <= 0:
            return 10.0
        return parsed_value

    @field_validator("gemini_api_timeout_seconds", mode="before")
    @classmethod
    def normalize_gemini_timeout(cls, value: float | str) -> float:
        parsed_value = float(value)
        if parsed_value <= 0:
            return 20.0
        return parsed_value

    @field_validator("notion_api_timeout_seconds", mode="before")
    @classmethod
    def normalize_notion_timeout(cls, value: float | str) -> float:
        parsed_value = float(value)
        if parsed_value <= 0:
            return 10.0
        return parsed_value

    @field_validator("google_calendar_api_timeout_seconds", mode="before")
    @classmethod
    def normalize_google_calendar_timeout(cls, value: float | str) -> float:
        parsed_value = float(value)
        if parsed_value <= 0:
            return 10.0
        return parsed_value

    @field_validator("monday_api_timeout_seconds", mode="before")
    @classmethod
    def normalize_monday_timeout(cls, value: float | str) -> float:
        parsed_value = float(value)
        if parsed_value <= 0:
            return 10.0
        return parsed_value

    @field_validator("auth_token_ttl_minutes", mode="before")
    @classmethod
    def normalize_auth_token_ttl(cls, value: int | str) -> int:
        parsed_value = int(value)
        if parsed_value <= 0:
            return 60 * 12
        return parsed_value

    @model_validator(mode="after")
    def apply_environment_url_defaults(self) -> "Settings":
        app_env = self.app_env
        backend_base_url = _default_backend_base_url(app_env)

        if not self.allowed_origins:
            self.allowed_origins = _default_allowed_origins(app_env)

        if not self.frontend_base_url.strip():
            self.frontend_base_url = _default_frontend_base_url(app_env)

        if not self.auth_google_redirect_uri.strip():
            self.auth_google_redirect_uri = f"{backend_base_url}/api/auth/google/callback"

        if not self.notion_redirect_uri.strip():
            self.notion_redirect_uri = f"{backend_base_url}/api/integrations/notion/callback"

        if not self.google_calendar_redirect_uri.strip():
            self.google_calendar_redirect_uri = (
                f"{backend_base_url}/api/integrations/google-calendar/callback"
            )

        if not self.outlook_redirect_uri.strip():
            self.outlook_redirect_uri = (
                f"{backend_base_url}/api/integrations/outlook-calendar/callback"
            )

        if not self.monday_redirect_uri.strip():
            self.monday_redirect_uri = (
                f"{backend_base_url}/api/integrations/monday/callback"
            )

        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
