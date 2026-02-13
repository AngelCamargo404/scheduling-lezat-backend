from functools import lru_cache
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Lezat Scheduling API"
    app_env: str = "development"
    app_version: str = "0.1.0"
    api_prefix: str = "/api"
    allowed_origins: Annotated[list[str], NoDecode] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    fireflies_webhook_secret: str = ""
    fireflies_api_url: str = "https://api.fireflies.ai/graphql"
    fireflies_api_key: str = ""
    fireflies_api_timeout_seconds: float = 10.0
    fireflies_api_user_agent: str = "LezatSchedulingBackend/1.0"
    read_ai_webhook_secret: str = ""
    transcription_autosync_enabled: bool = True
    transcriptions_store: str = "mongodb"
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db_name: str = "lezat_scheduling"
    mongodb_transcriptions_collection: str = "transcriptions"
    mongodb_action_item_creations_collection: str = "action_item_creations"
    mongodb_users_collection: str = "users"
    mongodb_user_settings_collection: str = "user_integration_settings"
    mongodb_connect_timeout_ms: int = 2000
    user_data_store: str = "mongodb"
    auth_secret_key: str = "change-me-in-production"
    auth_token_ttl_minutes: int = 60 * 12
    default_admin_email: str = "admin"
    default_admin_password: str = "admin"
    default_admin_full_name: str = "Administrator"
    auth_google_client_id: str = ""
    auth_google_client_secret: str = ""
    auth_google_redirect_uri: str = "http://localhost:8000/api/auth/google/callback"
    gemini_api_key: str = ""
    gemini_model: str = "gemini-3-flash-preview"
    gemini_api_timeout_seconds: float = 20.0
    frontend_base_url: str = "http://localhost:3000"
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
    action_items_test_due_date: str = ""
    notion_client_id: str = ""
    notion_client_secret: str = ""
    notion_redirect_uri: str = "http://localhost:8000/api/integrations/notion/callback"
    google_calendar_client_id: str = ""
    google_calendar_client_secret: str = ""
    google_calendar_redirect_uri: str = (
        "http://localhost:8000/api/integrations/google-calendar/callback"
    )
    google_calendar_api_token: str = ""
    google_calendar_refresh_token: str = ""
    google_calendar_id: str = "primary"
    google_calendar_api_timeout_seconds: float = 10.0
    google_calendar_event_timezone: str = "UTC"
    outlook_client_id: str = ""
    outlook_client_secret: str = ""
    outlook_tenant_id: str = "common"
    outlook_redirect_uri: str = "http://localhost:8000/api/integrations/outlook-calendar/callback"
    outlook_calendar_api_token: str = ""
    outlook_calendar_event_timezone: str = "UTC"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def parse_allowed_origins(cls, value: str | list[str]) -> list[str]:
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

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

    @field_validator("auth_token_ttl_minutes", mode="before")
    @classmethod
    def normalize_auth_token_ttl(cls, value: int | str) -> int:
        parsed_value = int(value)
        if parsed_value <= 0:
            return 60 * 12
        return parsed_value


@lru_cache
def get_settings() -> Settings:
    return Settings()
