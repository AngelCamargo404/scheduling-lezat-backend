from urllib.parse import quote

from fastapi import APIRouter, Depends, Query
from fastapi.responses import RedirectResponse

from app.core.config import get_settings
from app.schemas.auth import (
    AuthTokenResponse,
    CurrentUserResponse,
    LoginRequest,
    RegisterRequest,
)
from app.services.auth_service import AuthService, require_current_user

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=AuthTokenResponse)
def register(payload: RegisterRequest) -> AuthTokenResponse:
    service = AuthService()
    return service.register(payload)


@router.post("/login", response_model=AuthTokenResponse)
def login(payload: LoginRequest) -> AuthTokenResponse:
    service = AuthService()
    return service.login(payload)


@router.get("/me", response_model=CurrentUserResponse)
def get_me(
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> CurrentUserResponse:
    return current_user


@router.get("/google/start")
def start_google_auth() -> RedirectResponse:
    service = AuthService()
    return RedirectResponse(url=service.build_google_authorization_url(), status_code=302)


@router.get("/google/callback")
def handle_google_callback(
    code: str = Query(..., min_length=1),
    state: str = Query(..., min_length=1),
) -> RedirectResponse:
    service = AuthService()
    auth_result = service.login_with_google_callback(code=code, state=state)
    frontend_base_url = get_settings().frontend_base_url.rstrip("/")
    fragment = (
        "access_token="
        f"{quote(auth_result.access_token)}"
        f"&expires_in_seconds={auth_result.expires_in_seconds}"
    )
    return RedirectResponse(
        url=f"{frontend_base_url}/auth/google/callback#{fragment}",
        status_code=302,
    )
