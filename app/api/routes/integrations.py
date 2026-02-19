from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import date
import secrets
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.config import Settings, get_settings
from app.schemas.auth import CurrentUserResponse
from app.schemas.integration import (
    IntegrationCredentialStatus,
    IntegrationPipelineStatus,
    IntegrationPipelinesStatus,
    IntegrationsSettingsResponse,
    IntegrationsSettingsUpdateRequest,
    IntegrationsSettingsUpdateResponse,
    IntegrationsStatusResponse,
    IntegrationSettingsField,
    IntegrationSettingsGroup,
)
from app.services.auth_service import AuthService, require_current_user
from app.services.monday_kanban_client import MondayKanbanClient
from app.services.notion_kanban_client import NotionKanbanClient
from app.services.security_utils import create_access_token, decode_access_token
from app.services.user_store import UserStore, create_user_store

router = APIRouter(prefix="/integrations", tags=["integrations"])
_HTTP_BEARER = HTTPBearer(auto_error=False)
_NOTION_OAUTH_AUTHORIZE_URL = "https://api.notion.com/v1/oauth/authorize"
_NOTION_OAUTH_TOKEN_URL = "https://api.notion.com/v1/oauth/token"
_GOOGLE_OAUTH_AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
_OUTLOOK_OAUTH_AUTHORIZE_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize"
_OUTLOOK_OAUTH_TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
_MONDAY_OAUTH_AUTHORIZE_URL = "https://auth.monday.com/oauth2/authorize"
_MONDAY_OAUTH_TOKEN_URL = "https://auth.monday.com/oauth2/token"
_MONDAY_OAUTH_SCOPES = "boards:read boards:write users:read"
_OUTLOOK_GRAPH_SCOPES = (
    "offline_access "
    "https://graph.microsoft.com/User.Read "
    "https://graph.microsoft.com/Calendars.ReadWrite"
)


@dataclass(frozen=True)
class IntegrationSettingFieldDefinition:
    env_var: str
    label: str
    description: str
    group: str
    sensitive: bool = False
    required_for: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IntegrationSettingGroupDefinition:
    id: str
    title: str
    description: str


GROUP_DEFINITIONS = (
    IntegrationSettingGroupDefinition(
        id="fireflies",
        title="Fireflies AI",
        description="Recepcion y enriquecimiento de transcripciones desde Fireflies.",
    ),
    IntegrationSettingGroupDefinition(
        id="read_ai",
        title="Read AI",
        description="Webhook alternativo para ingestion de reuniones.",
    ),
    IntegrationSettingGroupDefinition(
        id="gemini",
        title="Gemini",
        description="Extraccion de tareas accionables desde transcripciones.",
    ),
    IntegrationSettingGroupDefinition(
        id="notion_sync",
        title="Notion Sync (token)",
        description="Creacion de notas/tareas en Notion con token de integracion.",
    ),
    IntegrationSettingGroupDefinition(
        id="monday_sync",
        title="Monday Sync (token)",
        description="Creacion de notas/tareas en Monday con token OAuth por usuario.",
    ),
    IntegrationSettingGroupDefinition(
        id="google_calendar_sync",
        title="Google Calendar Sync (token)",
        description="Creacion de eventos por fechas de entrega detectadas.",
    ),
    IntegrationSettingGroupDefinition(
        id="oauth_notion",
        title="OAuth Notion",
        description="Credenciales OAuth por usuario para Notion.",
    ),
    IntegrationSettingGroupDefinition(
        id="oauth_monday",
        title="OAuth Monday",
        description="Credenciales OAuth por usuario para Monday.",
    ),
    IntegrationSettingGroupDefinition(
        id="oauth_google",
        title="OAuth Google Calendar",
        description="Credenciales OAuth por usuario para Google Calendar.",
    ),
    IntegrationSettingGroupDefinition(
        id="oauth_outlook",
        title="OAuth Outlook",
        description="Credenciales OAuth por usuario para Outlook Calendar.",
    ),
    IntegrationSettingGroupDefinition(
        id="web_app",
        title="Web App",
        description="URLs de frontend y CORS para flujos de integracion.",
    ),
)

FIELD_DEFINITIONS = (
    IntegrationSettingFieldDefinition(
        env_var="FIREFLIES_API_KEY",
        label="Fireflies API Key",
        description="Token para consultar transcripcion completa por meetingId.",
        group="fireflies",
        sensitive=True,
        required_for=("fireflies_transcript_enrichment", "notion_notes_creation"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="FIREFLIES_WEBHOOK_SECRET",
        label="Fireflies Webhook Secret",
        description="Secreto compartido para validar firma HMAC del webhook.",
        group="fireflies",
        sensitive=True,
    ),
    IntegrationSettingFieldDefinition(
        env_var="FIREFLIES_API_URL",
        label="Fireflies API URL",
        description="Endpoint GraphQL de Fireflies.",
        group="fireflies",
    ),
    IntegrationSettingFieldDefinition(
        env_var="FIREFLIES_API_TIMEOUT_SECONDS",
        label="Fireflies Timeout (s)",
        description="Timeout para llamadas de red a Fireflies.",
        group="fireflies",
    ),
    IntegrationSettingFieldDefinition(
        env_var="FIREFLIES_API_USER_AGENT",
        label="Fireflies User Agent",
        description="User agent enviado por el backend a Fireflies.",
        group="fireflies",
    ),
    IntegrationSettingFieldDefinition(
        env_var="READ_AI_API_KEY",
        label="Read AI API Key",
        description="Token para consultar detalles de reuniones en Read AI.",
        group="read_ai",
        sensitive=True,
        required_for=("read_ai_transcript_enrichment",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="READ_AI_API_URL",
        label="Read AI API URL",
        description="Endpoint base de la API de Read AI.",
        group="read_ai",
    ),
    IntegrationSettingFieldDefinition(
        env_var="TRANSCRIPTION_AUTOSYNC_ENABLED",
        label="Transcription Autosync Enabled",
        description=(
            "Habilita o deshabilita la creacion automatica de notas y eventos "
            "a partir de transcripciones."
        ),
        group="fireflies",
    ),
    IntegrationSettingFieldDefinition(
        env_var="GEMINI_API_KEY",
        label="Gemini API Key",
        description="Token para extraer action items desde transcripciones.",
        group="gemini",
        sensitive=True,
        required_for=("notion_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="GEMINI_MODEL",
        label="Gemini Model",
        description="Modelo utilizado para analizar transcripciones.",
        group="gemini",
    ),
    IntegrationSettingFieldDefinition(
        env_var="GEMINI_API_TIMEOUT_SECONDS",
        label="Gemini Timeout (s)",
        description="Timeout para llamadas a Gemini.",
        group="gemini",
    ),
    IntegrationSettingFieldDefinition(
        env_var="NOTION_API_TOKEN",
        label="Notion API Token",
        description="Token de integracion para crear tareas en base Notion.",
        group="notion_sync",
        sensitive=True,
        required_for=("notion_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="NOTION_TASKS_DATABASE_ID",
        label="Notion Tasks Database ID",
        description="Database ID del kanban de tareas.",
        group="notion_sync",
        required_for=("notion_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="NOTION_TASK_STATUS_PROPERTY",
        label="Status Property",
        description="Nombre de propiedad de estado en Notion.",
        group="notion_sync",
        required_for=("notion_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="NOTION_KANBAN_TODO_STATUS",
        label="To-do Status Option",
        description=(
            "Nombre exacto de la opcion de estado/select usada al crear tareas "
            "(por ejemplo: Por hacer, Not started)."
        ),
        group="notion_sync",
        required_for=("notion_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_API_TOKEN",
        label="Monday API Token",
        description="Access token OAuth para crear tareas en Monday.",
        group="monday_sync",
        sensitive=True,
        required_for=("monday_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_API_URL",
        label="Monday API URL",
        description="Endpoint GraphQL de Monday.",
        group="monday_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_API_TIMEOUT_SECONDS",
        label="Monday Timeout (s)",
        description="Timeout para llamadas al API de Monday.",
        group="monday_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_BOARD_ID",
        label="Monday Board ID",
        description="Board ID del kanban de tareas en Monday.",
        group="monday_sync",
        required_for=("monday_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_GROUP_ID",
        label="Monday Group ID",
        description="Group ID dentro del board donde se crean los items.",
        group="monday_sync",
        required_for=("monday_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_STATUS_COLUMN_ID",
        label="Monday Status Column ID",
        description="Column ID tipo status/dropdown para estado inicial del item.",
        group="monday_sync",
        required_for=("monday_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_KANBAN_TODO_STATUS",
        label="Monday To-do Status Label",
        description="Etiqueta de estado inicial en Monday (ejemplo: Working on it).",
        group="monday_sync",
        required_for=("monday_notes_creation", "google_calendar_due_date_events"),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_ASSIGNEE_COLUMN_ID",
        label="Monday Assignee Column ID",
        description="Column ID para asignar responsable (opcional).",
        group="monday_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_DUE_DATE_COLUMN_ID",
        label="Monday Due Date Column ID",
        description="Column ID tipo date para la fecha de entrega (opcional).",
        group="monday_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_DETAILS_COLUMN_ID",
        label="Monday Details Column ID",
        description="Column ID tipo text/long_text para detalles de la tarea (opcional).",
        group="monday_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_MEETING_ID_COLUMN_ID",
        label="Monday Meeting ID Column ID",
        description="Column ID tipo text para guardar meeting id (opcional).",
        group="monday_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="GOOGLE_CALENDAR_API_TOKEN",
        label="Google Calendar API Token",
        description="Access token para crear eventos por due_date.",
        group="google_calendar_sync",
        sensitive=True,
        required_for=("google_calendar_due_date_events",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="GOOGLE_CALENDAR_ID",
        label="Google Calendar ID",
        description="Calendario destino para eventos de tareas.",
        group="google_calendar_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="GOOGLE_CALENDAR_API_TIMEOUT_SECONDS",
        label="Google Calendar Timeout (s)",
        description="Timeout para llamadas al API de Google Calendar.",
        group="google_calendar_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="GOOGLE_CALENDAR_EVENT_TIMEZONE",
        label="Google Calendar Timezone",
        description="Zona horaria por defecto para eventos creados.",
        group="google_calendar_sync",
    ),
    IntegrationSettingFieldDefinition(
        env_var="NOTION_CLIENT_ID",
        label="Notion OAuth Client ID",
        description="Client ID OAuth para conexion por usuario.",
        group="oauth_notion",
        required_for=("notion_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="NOTION_CLIENT_SECRET",
        label="Notion OAuth Client Secret",
        description="Client secret OAuth para Notion.",
        group="oauth_notion",
        sensitive=True,
        required_for=("notion_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="NOTION_REDIRECT_URI",
        label="Notion OAuth Redirect URI",
        description="Redirect URI configurada en app OAuth de Notion.",
        group="oauth_notion",
        required_for=("notion_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_CLIENT_ID",
        label="Monday OAuth Client ID",
        description="Client ID OAuth para Monday.",
        group="oauth_monday",
        required_for=("monday_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_CLIENT_SECRET",
        label="Monday OAuth Client Secret",
        description="Client secret OAuth para Monday.",
        group="oauth_monday",
        sensitive=True,
        required_for=("monday_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="MONDAY_REDIRECT_URI",
        label="Monday OAuth Redirect URI",
        description="Redirect URI registrada en la app OAuth de Monday.",
        group="oauth_monday",
        required_for=("monday_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="GOOGLE_CALENDAR_CLIENT_ID",
        label="Google OAuth Client ID",
        description="Client ID OAuth para Google Calendar.",
        group="oauth_google",
        required_for=("google_calendar_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="GOOGLE_CALENDAR_CLIENT_SECRET",
        label="Google OAuth Client Secret",
        description="Client secret OAuth para Google Calendar.",
        group="oauth_google",
        sensitive=True,
        required_for=("google_calendar_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="GOOGLE_CALENDAR_REDIRECT_URI",
        label="Google OAuth Redirect URI",
        description="Redirect URI configurada en Google Cloud Console.",
        group="oauth_google",
        required_for=("google_calendar_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="OUTLOOK_CALENDAR_API_TOKEN",
        label="Outlook Calendar API Token",
        description="Access token OAuth para crear eventos por due_date en Outlook Calendar.",
        group="oauth_outlook",
        sensitive=True,
        required_for=("outlook_calendar_due_date_events",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="OUTLOOK_CALENDAR_EVENT_TIMEZONE",
        label="Outlook Calendar Timezone",
        description="Zona horaria por defecto para eventos creados en Outlook.",
        group="oauth_outlook",
    ),
    IntegrationSettingFieldDefinition(
        env_var="OUTLOOK_CLIENT_ID",
        label="Outlook OAuth Client ID",
        description="Client ID OAuth para Microsoft Graph.",
        group="oauth_outlook",
        required_for=("outlook_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="OUTLOOK_CLIENT_SECRET",
        label="Outlook OAuth Client Secret",
        description="Client secret OAuth para Microsoft Graph.",
        group="oauth_outlook",
        sensitive=True,
        required_for=("outlook_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="OUTLOOK_TENANT_ID",
        label="Outlook Tenant ID",
        description="Tenant de Azure AD (o common).",
        group="oauth_outlook",
        required_for=("outlook_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="OUTLOOK_REDIRECT_URI",
        label="Outlook OAuth Redirect URI",
        description="Redirect URI registrada en Azure App.",
        group="oauth_outlook",
        required_for=("outlook_oauth_connection",),
    ),
    IntegrationSettingFieldDefinition(
        env_var="FRONTEND_BASE_URL",
        label="Frontend Base URL",
        description="Base URL de la app web para redirecciones.",
        group="web_app",
    ),
    IntegrationSettingFieldDefinition(
        env_var="ALLOWED_ORIGINS",
        label="Allowed Origins",
        description="Orgenes permitidos por CORS separados por coma.",
        group="web_app",
    ),
)

GROUP_DEFINITION_BY_ID = {group.id: group for group in GROUP_DEFINITIONS}
FIELD_DEFINITION_BY_ENV_VAR = {field.env_var: field for field in FIELD_DEFINITIONS}
EDITABLE_ENV_VARS = frozenset(FIELD_DEFINITION_BY_ENV_VAR.keys())
PLATFORM_MANAGED_ENV_VARS = frozenset(
    {
        "TRANSCRIPTIONS_STORE",
        "MONGODB_URI",
        "MONGODB_DB_NAME",
        "MONGODB_TRANSCRIPTIONS_COLLECTION",
        "MONGODB_USERS_COLLECTION",
        "MONGODB_USER_SETTINGS_COLLECTION",
        "MONGODB_CONNECT_TIMEOUT_MS",
        "USER_DATA_STORE",
    },
)
PROJECT_LOCKED_ENV_VARS = frozenset(
    {
        "GEMINI_API_KEY",
        "GEMINI_MODEL",
        "GOOGLE_CALENDAR_CLIENT_ID",
        "GOOGLE_CALENDAR_CLIENT_SECRET",
        "GOOGLE_CALENDAR_REDIRECT_URI",
    },
)
POSITIVE_FLOAT_ENV_VARS = frozenset(
    {
        "FIREFLIES_API_TIMEOUT_SECONDS",
        "GEMINI_API_TIMEOUT_SECONDS",
        "NOTION_API_TIMEOUT_SECONDS",
        "MONDAY_API_TIMEOUT_SECONDS",
        "GOOGLE_CALENDAR_API_TIMEOUT_SECONDS",
    },
)
POSITIVE_INT_ENV_VARS = frozenset()
URL_ENV_VARS = frozenset(
    {
        "FIREFLIES_API_URL",
        "MONDAY_API_URL",
        "NOTION_REDIRECT_URI",
        "MONDAY_REDIRECT_URI",
        "GOOGLE_CALENDAR_REDIRECT_URI",
        "OUTLOOK_REDIRECT_URI",
        "FRONTEND_BASE_URL",
    },
)
CSV_URL_ENV_VARS = frozenset({"ALLOWED_ORIGINS"})
ENUM_ENV_VARS: dict[str, set[str]] = {}
ENUM_ENV_VARS["TRANSCRIPTION_AUTOSYNC_ENABLED"] = {"true", "false"}


@router.get("/notion/connect")
def start_notion_oauth(
    access_token: str | None = Query(default=None),
    credentials: HTTPAuthorizationCredentials | None = Depends(_HTTP_BEARER),
) -> RedirectResponse:
    current_user = _resolve_current_user_for_oauth(access_token, credentials)
    settings = get_settings()
    env_values = _read_current_user_values(current_user)
    client_id, client_secret, redirect_uri = _resolve_notion_oauth_config(env_values, settings)

    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Notion OAuth is not configured. "
                "Define NOTION_CLIENT_ID, NOTION_CLIENT_SECRET and NOTION_REDIRECT_URI."
            ),
        )

    state_token, _ = create_access_token(
        claims={
            "type": "notion_oauth_state",
            "sub": current_user.id,
            "nonce": secrets.token_urlsafe(16),
        },
        secret_key=settings.auth_secret_key,
        ttl_minutes=10,
    )
    query = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "owner": "user",
            "state": state_token,
        },
    )
    return RedirectResponse(url=f"{_NOTION_OAUTH_AUTHORIZE_URL}?{query}", status_code=302)


@router.get("/notion/callback")
def finish_notion_oauth(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
) -> RedirectResponse:
    settings = get_settings()
    if error:
        return _build_notion_oauth_redirect("error", error)
    if not code or not state:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Missing code or state.",
        )

    decoded_state = decode_access_token(state, settings.auth_secret_key)
    if not decoded_state or decoded_state.get("type") != "notion_oauth_state":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid OAuth state.",
        )
    user_id = str(decoded_state.get("sub", "")).strip()
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth state does not include a valid user.",
        )

    user_store = _get_user_store()
    user_record = user_store.get_user_by_id(user_id)
    if not user_record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found for OAuth state.",
        )

    current_user = CurrentUserResponse(
        id=str(user_record.get("_id", "")),
        email=str(user_record.get("email", "")),
        full_name=str(user_record.get("full_name", "")),
        role=str(user_record.get("role", "user")),
    )
    env_values = _read_current_user_values(current_user)
    client_id, client_secret, redirect_uri = _resolve_notion_oauth_config(env_values, settings)
    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Notion OAuth is not configured. "
                "Define NOTION_CLIENT_ID, NOTION_CLIENT_SECRET and NOTION_REDIRECT_URI."
            ),
        )

    token_payload = _exchange_notion_code_for_token(
        code=code,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )
    notion_access_token = str(token_payload.get("access_token", "")).strip()
    if not notion_access_token:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Notion token response did not include access_token.",
        )

    user_store.upsert_user_settings_values(
        user_id,
        {"NOTION_API_TOKEN": notion_access_token},
    )
    return _build_notion_oauth_redirect("success", "connected")


@router.get("/notion/databases")
def get_notion_accessible_databases(
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> list[dict]:
    env_values = _read_current_user_values(current_user)
    token = env_values.get("NOTION_API_TOKEN", "")
    if not token.strip():
        return []

    # Use existing token to list databases. database_id is optional for listing.
    client = NotionKanbanClient(api_token=token, database_id="")
    try:
        return client.list_accessible_databases()
    except Exception:
        return []


@router.get("/notion/databases/{database_id}/properties")
def get_notion_database_properties(
    database_id: str,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> dict:
    env_values = _read_current_user_values(current_user)
    token = env_values.get("NOTION_API_TOKEN", "")
    if not token.strip() or not database_id.strip():
        return {}

    client = NotionKanbanClient(api_token=token, database_id=database_id)
    try:
        return client.list_database_properties()
    except Exception:
        return {}


@router.get("/notion/databases/{database_id}/status-options")
def get_notion_database_status_options(
    database_id: str,
    status_property: str = Query(default=""),
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> dict[str, object]:
    env_values = _read_current_user_values(current_user)
    token = env_values.get("NOTION_API_TOKEN", "")
    if not token.strip() or not database_id.strip():
        return {
            "selected_property": status_property.strip() or None,
            "property_type": None,
            "options": [],
            "available_status_properties": [],
        }

    client = NotionKanbanClient(api_token=token, database_id=database_id)
    try:
        properties = client.list_database_properties()
    except Exception:
        return {
            "selected_property": status_property.strip() or None,
            "property_type": None,
            "options": [],
            "available_status_properties": [],
        }

    available_status_properties: list[str] = []
    for property_name, property_payload in properties.items():
        if not isinstance(property_name, str) or not isinstance(property_payload, dict):
            continue
        property_type = str(property_payload.get("type", "")).strip().lower()
        if property_type in {"status", "select"}:
            available_status_properties.append(property_name)

    selected_property = status_property.strip()
    if not selected_property:
        selected_property = available_status_properties[0] if available_status_properties else ""

    selected_payload = properties.get(selected_property, {}) if selected_property else {}
    selected_type = str(selected_payload.get("type", "")).strip().lower()
    selected_options: list[str] = []
    if selected_type == "status":
        selected_options = _extract_notion_option_names(
            selected_payload.get("status", {}).get("options"),
        )
    elif selected_type == "select":
        selected_options = _extract_notion_option_names(
            selected_payload.get("select", {}).get("options"),
        )

    return {
        "selected_property": selected_property or None,
        "property_type": selected_type or None,
        "options": selected_options,
        "available_status_properties": available_status_properties,
    }


@router.get("/monday/connect")
def start_monday_oauth(
    access_token: str | None = Query(default=None),
    credentials: HTTPAuthorizationCredentials | None = Depends(_HTTP_BEARER),
) -> RedirectResponse:
    current_user = _resolve_current_user_for_oauth(access_token, credentials)
    settings = get_settings()
    env_values = _read_current_user_values(current_user)
    client_id, client_secret, redirect_uri = _resolve_monday_oauth_config(env_values, settings)

    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Monday OAuth is not configured. "
                "Define MONDAY_CLIENT_ID, MONDAY_CLIENT_SECRET and MONDAY_REDIRECT_URI."
            ),
        )

    state_token, _ = create_access_token(
        claims={
            "type": "monday_oauth_state",
            "sub": current_user.id,
            "nonce": secrets.token_urlsafe(16),
        },
        secret_key=settings.auth_secret_key,
        ttl_minutes=10,
    )
    query = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": _MONDAY_OAUTH_SCOPES,
            "state": state_token,
        },
    )
    return RedirectResponse(url=f"{_MONDAY_OAUTH_AUTHORIZE_URL}?{query}", status_code=302)


@router.get("/monday/callback")
def finish_monday_oauth(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
) -> RedirectResponse:
    settings = get_settings()
    if error:
        return _build_monday_oauth_redirect("error", error)
    if not code or not state:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Missing code or state.",
        )

    decoded_state = decode_access_token(state, settings.auth_secret_key)
    if not decoded_state or decoded_state.get("type") != "monday_oauth_state":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid OAuth state.",
        )
    user_id = str(decoded_state.get("sub", "")).strip()
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth state does not include a valid user.",
        )

    user_store = _get_user_store()
    user_record = user_store.get_user_by_id(user_id)
    if not user_record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found for OAuth state.",
        )

    current_user = CurrentUserResponse(
        id=str(user_record.get("_id", "")),
        email=str(user_record.get("email", "")),
        full_name=str(user_record.get("full_name", "")),
        role=str(user_record.get("role", "user")),
    )
    env_values = _read_current_user_values(current_user)
    client_id, client_secret, redirect_uri = _resolve_monday_oauth_config(env_values, settings)
    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Monday OAuth is not configured. "
                "Define MONDAY_CLIENT_ID, MONDAY_CLIENT_SECRET and MONDAY_REDIRECT_URI."
            ),
        )

    token_payload = _exchange_monday_code_for_token(
        code=code,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )
    monday_access_token = str(token_payload.get("access_token", "")).strip()
    if not monday_access_token:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Monday token response did not include access_token.",
        )

    user_store.upsert_user_settings_values(
        user_id,
        {"MONDAY_API_TOKEN": monday_access_token},
    )
    return _build_monday_oauth_redirect("success", "connected")


@router.get("/monday/boards")
def get_monday_accessible_boards(
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> list[dict[str, str]]:
    env_values = _read_current_user_values(current_user)
    token = env_values.get("MONDAY_API_TOKEN", "")
    if not token.strip():
        return []

    client = MondayKanbanClient(api_token=token)
    try:
        boards = client.list_accessible_boards()
    except Exception:
        return []
    return [
        {
            "id": str(board.get("id", "")).strip(),
            "name": str(board.get("name", "")).strip(),
            "url": str(board.get("url", "")).strip(),
        }
        for board in boards
        if str(board.get("id", "")).strip()
    ]


@router.get("/monday/boards/{board_id}/groups")
def get_monday_board_groups(
    board_id: str,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> list[dict[str, str]]:
    env_values = _read_current_user_values(current_user)
    token = env_values.get("MONDAY_API_TOKEN", "")
    if not token.strip() or not board_id.strip():
        return []

    client = MondayKanbanClient(api_token=token, board_id=board_id)
    try:
        return client.list_board_groups(board_id=board_id)
    except Exception:
        return []


@router.get("/monday/boards/{board_id}/columns")
def get_monday_board_columns(
    board_id: str,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> list[dict[str, str]]:
    env_values = _read_current_user_values(current_user)
    token = env_values.get("MONDAY_API_TOKEN", "")
    if not token.strip() or not board_id.strip():
        return []

    client = MondayKanbanClient(api_token=token, board_id=board_id)
    try:
        return client.list_board_columns(board_id=board_id)
    except Exception:
        return []


@router.get("/monday/boards/{board_id}/status-options")
def get_monday_board_status_options(
    board_id: str,
    status_column_id: str = Query(default=""),
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> dict[str, object]:
    env_values = _read_current_user_values(current_user)
    token = env_values.get("MONDAY_API_TOKEN", "")
    if not token.strip() or not board_id.strip():
        return {
            "selected_column_id": status_column_id.strip() or None,
            "options": [],
            "available_status_columns": [],
        }

    client = MondayKanbanClient(api_token=token, board_id=board_id)
    try:
        return client.list_board_status_options(
            board_id=board_id,
            status_column_id=status_column_id,
        )
    except Exception:
        return {
            "selected_column_id": status_column_id.strip() or None,
            "options": [],
            "available_status_columns": [],
        }


@router.get("/google-calendar/connect")
def start_google_calendar_oauth(
    access_token: str | None = Query(default=None),
    credentials: HTTPAuthorizationCredentials | None = Depends(_HTTP_BEARER),
) -> RedirectResponse:
    current_user = _resolve_current_user_for_oauth(access_token, credentials)
    settings = get_settings()
    env_values = _read_current_user_values(current_user)
    client_id, client_secret, redirect_uri = _resolve_google_calendar_oauth_config(env_values, settings)

    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Google Calendar OAuth is not configured. "
                "Define GOOGLE_CALENDAR_CLIENT_ID, GOOGLE_CALENDAR_CLIENT_SECRET and "
                "GOOGLE_CALENDAR_REDIRECT_URI."
            ),
        )

    state_token, _ = create_access_token(
        claims={
            "type": "google_calendar_oauth_state",
            "sub": current_user.id,
            "nonce": secrets.token_urlsafe(16),
        },
        secret_key=settings.auth_secret_key,
        ttl_minutes=10,
    )
    query = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": "https://www.googleapis.com/auth/calendar.events",
            "state": state_token,
            "prompt": "consent",
            "access_type": "offline",
            "include_granted_scopes": "true",
        },
    )
    return RedirectResponse(url=f"{_GOOGLE_OAUTH_AUTHORIZE_URL}?{query}", status_code=302)


@router.get("/google-calendar/callback")
def finish_google_calendar_oauth(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
) -> RedirectResponse:
    settings = get_settings()
    if error:
        return _build_google_calendar_oauth_redirect("error", error)
    if not code or not state:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Missing code or state.",
        )

    decoded_state = decode_access_token(state, settings.auth_secret_key)
    if not decoded_state or decoded_state.get("type") != "google_calendar_oauth_state":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid OAuth state.",
        )
    user_id = str(decoded_state.get("sub", "")).strip()
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth state does not include a valid user.",
        )

    user_store = _get_user_store()
    user_record = user_store.get_user_by_id(user_id)
    if not user_record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found for OAuth state.",
        )
    current_user = CurrentUserResponse(
        id=str(user_record.get("_id", "")),
        email=str(user_record.get("email", "")),
        full_name=str(user_record.get("full_name", "")),
        role=str(user_record.get("role", "user")),
    )
    env_values = _read_current_user_values(current_user)
    client_id, client_secret, redirect_uri = _resolve_google_calendar_oauth_config(env_values, settings)
    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Google Calendar OAuth is not configured. "
                "Define GOOGLE_CALENDAR_CLIENT_ID, GOOGLE_CALENDAR_CLIENT_SECRET and "
                "GOOGLE_CALENDAR_REDIRECT_URI."
            ),
        )

    token_payload = _exchange_google_calendar_code_for_token(
        code=code,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )
    google_access_token = str(token_payload.get("access_token", "")).strip()
    if not google_access_token:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Google token response did not include access_token.",
        )
    google_refresh_token = str(token_payload.get("refresh_token", "")).strip()
    existing_refresh_token = env_values.get("GOOGLE_CALENDAR_REFRESH_TOKEN", "").strip()

    updates = {"GOOGLE_CALENDAR_API_TOKEN": google_access_token}
    if google_refresh_token:
        updates["GOOGLE_CALENDAR_REFRESH_TOKEN"] = google_refresh_token
    elif existing_refresh_token:
        updates["GOOGLE_CALENDAR_REFRESH_TOKEN"] = existing_refresh_token
    google_timezone = _fetch_google_calendar_timezone(google_access_token)
    if google_timezone:
        updates["GOOGLE_CALENDAR_EVENT_TIMEZONE"] = google_timezone
    user_store.upsert_user_settings_values(
        user_id,
        updates,
    )
    return _build_google_calendar_oauth_redirect("success", "connected")


@router.get("/outlook-calendar/connect")
def start_outlook_calendar_oauth(
    access_token: str | None = Query(default=None),
    credentials: HTTPAuthorizationCredentials | None = Depends(_HTTP_BEARER),
) -> RedirectResponse:
    current_user = _resolve_current_user_for_oauth(access_token, credentials)
    settings = get_settings()
    env_values = _read_current_user_values(current_user)
    client_id, client_secret, tenant_id, redirect_uri = _resolve_outlook_oauth_config(env_values, settings)

    if not client_id or not client_secret or not tenant_id or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Outlook OAuth is not configured. "
                "Define OUTLOOK_CLIENT_ID, OUTLOOK_CLIENT_SECRET, OUTLOOK_TENANT_ID and "
                "OUTLOOK_REDIRECT_URI."
            ),
        )

    state_token, _ = create_access_token(
        claims={
            "type": "outlook_calendar_oauth_state",
            "sub": current_user.id,
            "nonce": secrets.token_urlsafe(16),
        },
        secret_key=settings.auth_secret_key,
        ttl_minutes=10,
    )
    query = urlencode(
        {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "response_mode": "query",
            "scope": _OUTLOOK_GRAPH_SCOPES,
            "state": state_token,
            "prompt": "select_account",
        },
    )
    authorize_url = _OUTLOOK_OAUTH_AUTHORIZE_URL_TEMPLATE.format(tenant_id=tenant_id)
    return RedirectResponse(url=f"{authorize_url}?{query}", status_code=302)


@router.get("/outlook-calendar/callback")
def finish_outlook_calendar_oauth(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
) -> RedirectResponse:
    settings = get_settings()
    if error:
        return _build_outlook_oauth_redirect("error", error)
    if not code or not state:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Missing code or state.",
        )

    decoded_state = decode_access_token(state, settings.auth_secret_key)
    if not decoded_state or decoded_state.get("type") != "outlook_calendar_oauth_state":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid OAuth state.",
        )
    user_id = str(decoded_state.get("sub", "")).strip()
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth state does not include a valid user.",
        )

    user_store = _get_user_store()
    user_record = user_store.get_user_by_id(user_id)
    if not user_record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found for OAuth state.",
        )
    current_user = CurrentUserResponse(
        id=str(user_record.get("_id", "")),
        email=str(user_record.get("email", "")),
        full_name=str(user_record.get("full_name", "")),
        role=str(user_record.get("role", "user")),
    )
    env_values = _read_current_user_values(current_user)
    client_id, client_secret, tenant_id, redirect_uri = _resolve_outlook_oauth_config(env_values, settings)
    if not client_id or not client_secret or not tenant_id or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Outlook OAuth is not configured. "
                "Define OUTLOOK_CLIENT_ID, OUTLOOK_CLIENT_SECRET, OUTLOOK_TENANT_ID and "
                "OUTLOOK_REDIRECT_URI."
            ),
        )

    token_payload = _exchange_outlook_calendar_code_for_token(
        code=code,
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id,
        redirect_uri=redirect_uri,
    )
    outlook_access_token = str(token_payload.get("access_token", "")).strip()
    if not outlook_access_token:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Outlook token response did not include access_token.",
        )
    outlook_refresh_token = str(token_payload.get("refresh_token", "")).strip()
    existing_refresh_token = env_values.get("OUTLOOK_CALENDAR_REFRESH_TOKEN", "").strip()

    updates = {"OUTLOOK_CALENDAR_API_TOKEN": outlook_access_token}
    if outlook_refresh_token:
        updates["OUTLOOK_CALENDAR_REFRESH_TOKEN"] = outlook_refresh_token
    elif existing_refresh_token:
        updates["OUTLOOK_CALENDAR_REFRESH_TOKEN"] = existing_refresh_token
    outlook_timezone = _fetch_outlook_calendar_timezone(outlook_access_token)
    if outlook_timezone:
        updates["OUTLOOK_CALENDAR_EVENT_TIMEZONE"] = outlook_timezone
    user_store.upsert_user_settings_values(
        user_id,
        updates,
    )
    return _build_outlook_oauth_redirect("success", "connected")


@router.get("/status", response_model=IntegrationsStatusResponse)
def get_integrations_status(
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> IntegrationsStatusResponse:
    env_values = _read_current_user_values(current_user)
    return _build_status_response(env_values)


@router.get("/settings", response_model=IntegrationsSettingsResponse)
def get_integrations_settings(
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> IntegrationsSettingsResponse:
    env_values = _read_current_user_values(current_user)
    return _build_settings_response(env_values)


@router.patch("/settings", response_model=IntegrationsSettingsUpdateResponse)
def update_integrations_settings(
    payload: IntegrationsSettingsUpdateRequest,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> IntegrationsSettingsUpdateResponse:
    updates = _normalize_updates(payload.values)
    _validate_updates(updates)

    user_store = _get_user_store()
    updated_env_vars: list[str] = []
    if updates:
        updated_env_vars = user_store.upsert_user_settings_values(current_user.id, updates)

    env_values = _read_current_user_values(current_user)
    return IntegrationsSettingsUpdateResponse(
        updated_env_vars=updated_env_vars,
        status=_build_status_response(env_values),
        settings=_build_settings_response(env_values),
    )


def _build_status_response(env_values: dict[str, str]) -> IntegrationsStatusResponse:
    def is_configured(env_var: str) -> bool:
        return bool(env_values.get(env_var, "").strip())

    credentials = IntegrationCredentialStatus(
        fireflies_api_key_configured=is_configured("FIREFLIES_API_KEY"),
        fireflies_webhook_secret_configured=is_configured("FIREFLIES_WEBHOOK_SECRET"),
        read_ai_api_key_configured=is_configured("READ_AI_API_KEY"),
        gemini_api_key_configured=is_configured("GEMINI_API_KEY"),
        notion_api_token_configured=is_configured("NOTION_API_TOKEN"),
        notion_tasks_database_id_configured=is_configured("NOTION_TASKS_DATABASE_ID"),
        google_calendar_api_token_configured=is_configured("GOOGLE_CALENDAR_API_TOKEN"),
        notion_client_id_configured=is_configured("NOTION_CLIENT_ID"),
        notion_client_secret_configured=is_configured("NOTION_CLIENT_SECRET"),
        notion_redirect_uri_configured=is_configured("NOTION_REDIRECT_URI"),
        google_calendar_client_id_configured=is_configured("GOOGLE_CALENDAR_CLIENT_ID"),
        google_calendar_client_secret_configured=is_configured("GOOGLE_CALENDAR_CLIENT_SECRET"),
        google_calendar_redirect_uri_configured=is_configured("GOOGLE_CALENDAR_REDIRECT_URI"),
        outlook_calendar_api_token_configured=is_configured("OUTLOOK_CALENDAR_API_TOKEN"),
        outlook_client_id_configured=is_configured("OUTLOOK_CLIENT_ID"),
        outlook_client_secret_configured=is_configured("OUTLOOK_CLIENT_SECRET"),
        outlook_tenant_id_configured=is_configured("OUTLOOK_TENANT_ID"),
        outlook_redirect_uri_configured=is_configured("OUTLOOK_REDIRECT_URI"),
        monday_api_token_configured=is_configured("MONDAY_API_TOKEN"),
        monday_board_id_configured=is_configured("MONDAY_BOARD_ID"),
        monday_group_id_configured=is_configured("MONDAY_GROUP_ID"),
        monday_client_id_configured=is_configured("MONDAY_CLIENT_ID"),
        monday_client_secret_configured=is_configured("MONDAY_CLIENT_SECRET"),
        monday_redirect_uri_configured=is_configured("MONDAY_REDIRECT_URI"),
    )
    notion_ready = (
        credentials.notion_api_token_configured
        and credentials.notion_tasks_database_id_configured
        and is_configured("NOTION_TASK_STATUS_PROPERTY")
        and is_configured("NOTION_KANBAN_TODO_STATUS")
    )
    monday_ready = (
        credentials.monday_api_token_configured
        and credentials.monday_board_id_configured
        and credentials.monday_group_id_configured
        and is_configured("MONDAY_STATUS_COLUMN_ID")
        and is_configured("MONDAY_KANBAN_TODO_STATUS")
    )
    notes_output_ready = notion_ready or monday_ready
    pipelines = IntegrationPipelinesStatus(
        fireflies_transcript_enrichment=_build_pipeline_status(
            {
                "FIREFLIES_API_KEY": credentials.fireflies_api_key_configured,
            },
        ),
        read_ai_transcript_enrichment=_build_pipeline_status(
            {
                "READ_AI_API_KEY": credentials.read_ai_api_key_configured,
            },
        ),
        notion_notes_creation=_build_pipeline_status(
            {
                "FIREFLIES_API_KEY": credentials.fireflies_api_key_configured,
                "GEMINI_API_KEY": credentials.gemini_api_key_configured,
                "NOTION_API_TOKEN": credentials.notion_api_token_configured,
                "NOTION_TASKS_DATABASE_ID": credentials.notion_tasks_database_id_configured,
                "NOTION_TASK_STATUS_PROPERTY": is_configured("NOTION_TASK_STATUS_PROPERTY"),
                "NOTION_KANBAN_TODO_STATUS": is_configured("NOTION_KANBAN_TODO_STATUS"),
            },
        ),
        monday_notes_creation=_build_pipeline_status(
            {
                "FIREFLIES_API_KEY": credentials.fireflies_api_key_configured,
                "GEMINI_API_KEY": credentials.gemini_api_key_configured,
                "MONDAY_API_TOKEN": credentials.monday_api_token_configured,
                "MONDAY_BOARD_ID": credentials.monday_board_id_configured,
                "MONDAY_GROUP_ID": credentials.monday_group_id_configured,
                "MONDAY_STATUS_COLUMN_ID": is_configured("MONDAY_STATUS_COLUMN_ID"),
                "MONDAY_KANBAN_TODO_STATUS": is_configured("MONDAY_KANBAN_TODO_STATUS"),
            },
        ),
        google_calendar_due_date_events=_build_pipeline_status(
            {
                "FIREFLIES_API_KEY": credentials.fireflies_api_key_configured,
                "GEMINI_API_KEY": credentials.gemini_api_key_configured,
                "NOTES_OUTPUT_CONFIGURED": notes_output_ready,
                "GOOGLE_CALENDAR_API_TOKEN": credentials.google_calendar_api_token_configured,
            },
        ),
        outlook_calendar_due_date_events=_build_pipeline_status(
            {
                "FIREFLIES_API_KEY": credentials.fireflies_api_key_configured,
                "GEMINI_API_KEY": credentials.gemini_api_key_configured,
                "NOTES_OUTPUT_CONFIGURED": notes_output_ready,
                "OUTLOOK_CALENDAR_API_TOKEN": credentials.outlook_calendar_api_token_configured,
            },
        ),
        notion_oauth_connection=_build_pipeline_status(
            {
                "NOTION_CLIENT_ID": credentials.notion_client_id_configured,
                "NOTION_CLIENT_SECRET": credentials.notion_client_secret_configured,
                "NOTION_REDIRECT_URI": credentials.notion_redirect_uri_configured,
            },
        ),
        monday_oauth_connection=_build_pipeline_status(
            {
                "MONDAY_CLIENT_ID": credentials.monday_client_id_configured,
                "MONDAY_CLIENT_SECRET": credentials.monday_client_secret_configured,
                "MONDAY_REDIRECT_URI": credentials.monday_redirect_uri_configured,
            },
        ),
        google_calendar_oauth_connection=_build_pipeline_status(
            {
                "GOOGLE_CALENDAR_CLIENT_ID": credentials.google_calendar_client_id_configured,
                "GOOGLE_CALENDAR_CLIENT_SECRET": (
                    credentials.google_calendar_client_secret_configured
                ),
                "GOOGLE_CALENDAR_REDIRECT_URI": credentials.google_calendar_redirect_uri_configured,
            },
        ),
        outlook_oauth_connection=_build_pipeline_status(
            {
                "OUTLOOK_CLIENT_ID": credentials.outlook_client_id_configured,
                "OUTLOOK_CLIENT_SECRET": credentials.outlook_client_secret_configured,
                "OUTLOOK_TENANT_ID": credentials.outlook_tenant_id_configured,
                "OUTLOOK_REDIRECT_URI": credentials.outlook_redirect_uri_configured,
            },
        ),
    )
    return IntegrationsStatusResponse(credentials=credentials, pipelines=pipelines)


def _build_settings_response(env_values: dict[str, str]) -> IntegrationsSettingsResponse:
    groups: list[IntegrationSettingsGroup] = []
    for group_definition in GROUP_DEFINITIONS:
        group_fields: list[IntegrationSettingsField] = []
        for field_definition in FIELD_DEFINITIONS:
            if field_definition.group != group_definition.id:
                continue
            raw_value = env_values.get(field_definition.env_var, "")
            configured = bool(raw_value.strip())
            group_fields.append(
                IntegrationSettingsField(
                    env_var=field_definition.env_var,
                    label=field_definition.label,
                    description=field_definition.description,
                    sensitive=field_definition.sensitive,
                    configured=configured,
                    value=None if field_definition.sensitive else raw_value,
                    required_for=list(field_definition.required_for),
                ),
            )
        groups.append(
            IntegrationSettingsGroup(
                id=group_definition.id,
                title=group_definition.title,
                description=group_definition.description,
                fields=group_fields,
            ),
        )
    return IntegrationsSettingsResponse(groups=groups)


def _read_current_user_values(current_user: CurrentUserResponse) -> dict[str, str]:
    settings = get_settings()
    user_store = _get_user_store()

    if not user_store.has_user_settings(current_user.id):
        if current_user.role == "admin":
            user_store.replace_user_settings_values(
                current_user.id,
                _read_default_values_from_settings(settings),
            )
        else:
            user_store.replace_user_settings_values(current_user.id, {})

    values = user_store.get_user_settings_values(current_user.id)
    # Gemini config is project-level and should not vary by user.
    values["GEMINI_API_KEY"] = settings.gemini_api_key
    values["GEMINI_MODEL"] = settings.gemini_model
    # Google Calendar OAuth app credentials are project-level values from .env.
    values["GOOGLE_CALENDAR_CLIENT_ID"] = settings.google_calendar_client_id
    values["GOOGLE_CALENDAR_CLIENT_SECRET"] = settings.google_calendar_client_secret
    values["GOOGLE_CALENDAR_REDIRECT_URI"] = settings.google_calendar_redirect_uri
    for env_var in EDITABLE_ENV_VARS:
        if env_var not in values:
            if env_var == "TRANSCRIPTION_AUTOSYNC_ENABLED":
                values[env_var] = "true"
                continue
            values[env_var] = ""
    return values


def _read_default_values_from_settings(settings: Settings) -> dict[str, str]:
    defaults: dict[str, str] = {}
    for env_var in EDITABLE_ENV_VARS:
        if env_var == "TRANSCRIPTION_AUTOSYNC_ENABLED":
            defaults[env_var] = "true"
            continue
        attr_name = env_var.lower()
        if not hasattr(settings, attr_name):
            continue
        raw_value = getattr(settings, attr_name)
        if isinstance(raw_value, list):
            normalized_value = ",".join(
                str(value).strip()
                for value in raw_value
                if str(value).strip()
            )
        elif isinstance(raw_value, bool):
            normalized_value = "true" if raw_value else "false"
        elif raw_value is None:
            normalized_value = ""
        else:
            normalized_value = str(raw_value).strip()
        if normalized_value:
            defaults[env_var] = normalized_value
    return defaults


def _get_user_store() -> UserStore:
    return create_user_store(get_settings())


def _normalize_updates(raw_values: dict[str, str]) -> dict[str, str]:
    updates: dict[str, str] = {}
    for env_var, raw_value in raw_values.items():
        if env_var in PROJECT_LOCKED_ENV_VARS:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"{env_var} is managed at project level and cannot be edited "
                    "from this endpoint."
                ),
            )
        if env_var in PLATFORM_MANAGED_ENV_VARS:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"{env_var} is managed by the platform and cannot be edited "
                    "from this endpoint."
                ),
            )
        if env_var not in EDITABLE_ENV_VARS:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Environment variable {env_var} is not editable from this endpoint.",
            )
        normalized_value = (raw_value or "").strip()
        if env_var == "TRANSCRIPTION_AUTOSYNC_ENABLED" and normalized_value:
            normalized_value = _normalize_boolean_text(normalized_value)
        updates[env_var] = normalized_value
    return updates


def _validate_updates(updates: dict[str, str]) -> None:
    for env_var, value in updates.items():
        if env_var in POSITIVE_FLOAT_ENV_VARS and value:
            _assert_positive_float(env_var, value)
        if env_var in POSITIVE_INT_ENV_VARS and value:
            _assert_positive_int(env_var, value)
        if env_var in URL_ENV_VARS and value:
            _assert_url(env_var, value)
        if env_var in CSV_URL_ENV_VARS and value:
            for raw_url in value.split(","):
                _assert_url(env_var, raw_url.strip())
        if env_var in ENUM_ENV_VARS and value:
            if value not in ENUM_ENV_VARS[env_var]:
                allowed_values = ", ".join(sorted(ENUM_ENV_VARS[env_var]))
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"{env_var} must be one of: {allowed_values}.",
                )
        if env_var == "ACTION_ITEMS_TEST_DUE_DATE" and value:
            _assert_iso_date(env_var, value)


def _assert_positive_float(env_var: str, value: str) -> None:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{env_var} must be a positive number.",
        ) from exc
    if parsed <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{env_var} must be a positive number.",
        )


def _assert_positive_int(env_var: str, value: str) -> None:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{env_var} must be a positive integer.",
        ) from exc
    if parsed <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{env_var} must be a positive integer.",
        )


def _assert_url(env_var: str, value: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{env_var} must be a valid http/https URL.",
        )


def _assert_iso_date(env_var: str, value: str) -> None:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{env_var} must be an ISO date in YYYY-MM-DD format.",
        ) from exc


def _normalize_boolean_text(raw_value: str) -> str:
    lowered = raw_value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return "true"
    if lowered in {"0", "false", "no", "off"}:
        return "false"
    return lowered


def _build_pipeline_status(requirements: dict[str, bool]) -> IntegrationPipelineStatus:
    missing_env_vars = [env_var for env_var, configured in requirements.items() if not configured]
    return IntegrationPipelineStatus(ready=not missing_env_vars, missing_env_vars=missing_env_vars)


def _extract_notion_option_names(raw_options: object) -> list[str]:
    if not isinstance(raw_options, list):
        return []
    values: list[str] = []
    seen: set[str] = set()
    for raw_option in raw_options:
        if not isinstance(raw_option, dict):
            continue
        name = str(raw_option.get("name", "")).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        values.append(name)
    return values


def _resolve_current_user_for_oauth(
    access_token: str | None,
    credentials: HTTPAuthorizationCredentials | None,
) -> CurrentUserResponse:
    token = (access_token or "").strip()
    if not token and credentials and credentials.scheme.lower() == "bearer":
        token = credentials.credentials.strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=(
                "Authentication required. Send Authorization: Bearer <token> "
                "or access_token query parameter."
            ),
        )
    return AuthService().get_current_user_from_token(token)


def _exchange_google_calendar_code_for_token(
    *,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
) -> dict[str, object]:
    body = urlencode(
        {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
    ).encode("utf-8")
    request = Request(
        _GOOGLE_OAUTH_TOKEN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            raw_payload = response.read().decode("utf-8")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to exchange Google Calendar authorization code.",
        ) from exc

    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid Google token response.",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid Google token response payload.",
        )
    return payload


def _exchange_notion_code_for_token(
    *,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
) -> dict[str, object]:
    auth_value = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    payload = json.dumps(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        },
    ).encode("utf-8")
    request = Request(
        _NOTION_OAUTH_TOKEN_URL,
        data=payload,
        headers={
            "Authorization": f"Basic {auth_value}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            raw_payload = response.read().decode("utf-8")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to exchange Notion authorization code.",
        ) from exc

    try:
        parsed_payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid Notion token response.",
        ) from exc
    if not isinstance(parsed_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid Notion token response payload.",
        )
    return parsed_payload


def _exchange_monday_code_for_token(
    *,
    code: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
) -> dict[str, object]:
    payload = json.dumps(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        },
    ).encode("utf-8")
    request_payload = Request(
        _MONDAY_OAUTH_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request_payload, timeout=15) as response:
            raw_payload = response.read().decode("utf-8")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to exchange Monday authorization code.",
        ) from exc

    try:
        parsed_payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid Monday token response.",
        ) from exc
    if not isinstance(parsed_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid Monday token response payload.",
        )
    return parsed_payload


def _exchange_outlook_calendar_code_for_token(
    *,
    code: str,
    client_id: str,
    client_secret: str,
    tenant_id: str,
    redirect_uri: str,
) -> dict[str, object]:
    body = urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "scope": _OUTLOOK_GRAPH_SCOPES,
        },
    ).encode("utf-8")
    token_url = _OUTLOOK_OAUTH_TOKEN_URL_TEMPLATE.format(tenant_id=tenant_id)
    request = Request(
        token_url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            raw_payload = response.read().decode("utf-8")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to exchange Outlook authorization code.",
        ) from exc

    try:
        parsed_payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid Outlook token response.",
        ) from exc
    if not isinstance(parsed_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid Outlook token response payload.",
        )
    return parsed_payload


def _fetch_google_calendar_timezone(access_token: str) -> str | None:
    token = access_token.strip()
    if not token:
        return None
    request_payload = Request(
        "https://www.googleapis.com/calendar/v3/users/me/settings/timezone",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urlopen(request_payload, timeout=10) as response:
            raw_payload = response.read().decode("utf-8")
    except Exception:
        return None
    try:
        parsed_payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed_payload, dict):
        return None
    timezone_value = str(parsed_payload.get("value", "")).strip()
    return timezone_value or None


def _fetch_outlook_calendar_timezone(access_token: str) -> str | None:
    token = access_token.strip()
    if not token:
        return None
    request_payload = Request(
        "https://graph.microsoft.com/v1.0/me/mailboxSettings",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urlopen(request_payload, timeout=10) as response:
            raw_payload = response.read().decode("utf-8")
    except Exception:
        return None
    try:
        parsed_payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed_payload, dict):
        return None
    timezone_value = str(parsed_payload.get("timeZone", "")).strip()
    return timezone_value or None


def _build_notion_oauth_redirect(status_value: str, message: str) -> RedirectResponse:
    frontend_base_url = get_settings().frontend_base_url.rstrip("/")
    query = urlencode(
        {
            "notion_oauth": status_value,
            "notion_oauth_message": message,
        },
    )
    return RedirectResponse(url=f"{frontend_base_url}/configuracion?{query}", status_code=302)


def _build_monday_oauth_redirect(status_value: str, message: str) -> RedirectResponse:
    frontend_base_url = get_settings().frontend_base_url.rstrip("/")
    query = urlencode(
        {
            "monday_oauth": status_value,
            "monday_oauth_message": message,
        },
    )
    return RedirectResponse(url=f"{frontend_base_url}/configuracion?{query}", status_code=302)


def _build_google_calendar_oauth_redirect(status_value: str, message: str) -> RedirectResponse:
    frontend_base_url = get_settings().frontend_base_url.rstrip("/")
    query = urlencode(
        {
            "google_calendar_oauth": status_value,
            "google_calendar_oauth_message": message,
        },
    )
    return RedirectResponse(url=f"{frontend_base_url}/configuracion?{query}", status_code=302)


def _build_outlook_oauth_redirect(status_value: str, message: str) -> RedirectResponse:
    frontend_base_url = get_settings().frontend_base_url.rstrip("/")
    query = urlencode(
        {
            "outlook_calendar_oauth": status_value,
            "outlook_calendar_oauth_message": message,
        },
    )
    return RedirectResponse(url=f"{frontend_base_url}/configuracion?{query}", status_code=302)


def _resolve_google_calendar_oauth_config(
    _env_values: dict[str, str],
    settings: Settings,
) -> tuple[str, str, str]:
    client_id = settings.google_calendar_client_id.strip()
    client_secret = settings.google_calendar_client_secret.strip()
    redirect_uri = settings.google_calendar_redirect_uri.strip()
    return client_id, client_secret, redirect_uri


def _resolve_notion_oauth_config(
    env_values: dict[str, str],
    settings: Settings,
) -> tuple[str, str, str]:
    client_id = env_values.get("NOTION_CLIENT_ID", "").strip() or settings.notion_client_id.strip()
    client_secret = (
        env_values.get("NOTION_CLIENT_SECRET", "").strip() or settings.notion_client_secret.strip()
    )
    redirect_uri = env_values.get("NOTION_REDIRECT_URI", "").strip() or settings.notion_redirect_uri.strip()
    return client_id, client_secret, redirect_uri


def _resolve_monday_oauth_config(
    env_values: dict[str, str],
    settings: Settings,
) -> tuple[str, str, str]:
    client_id = env_values.get("MONDAY_CLIENT_ID", "").strip() or settings.monday_client_id.strip()
    client_secret = (
        env_values.get("MONDAY_CLIENT_SECRET", "").strip() or settings.monday_client_secret.strip()
    )
    redirect_uri = env_values.get("MONDAY_REDIRECT_URI", "").strip() or settings.monday_redirect_uri.strip()
    return client_id, client_secret, redirect_uri


def _resolve_outlook_oauth_config(
    env_values: dict[str, str],
    settings: Settings,
) -> tuple[str, str, str, str]:
    client_id = env_values.get("OUTLOOK_CLIENT_ID", "").strip() or settings.outlook_client_id.strip()
    client_secret = (
        env_values.get("OUTLOOK_CLIENT_SECRET", "").strip() or settings.outlook_client_secret.strip()
    )
    tenant_id = env_values.get("OUTLOOK_TENANT_ID", "").strip() or settings.outlook_tenant_id.strip()
    redirect_uri = env_values.get("OUTLOOK_REDIRECT_URI", "").strip() or settings.outlook_redirect_uri.strip()
    return client_id, client_secret, tenant_id, redirect_uri


