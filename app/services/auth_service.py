from __future__ import annotations

import json
import secrets
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.config import Settings, get_settings
from app.schemas.auth import AuthTokenResponse, CurrentUserResponse, LoginRequest, RegisterRequest
from app.services.security_utils import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)
from app.services.user_store import UserStore, create_user_store

_HTTP_BEARER = HTTPBearer(auto_error=False)
_GOOGLE_OAUTH_AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_OAUTH_TOKEN_INFO_URL = "https://oauth2.googleapis.com/tokeninfo"


class AuthService:
    def __init__(
        self,
        settings: Settings | None = None,
        user_store: UserStore | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.user_store = user_store or create_user_store(self.settings)
        self.ensure_default_admin_user()

    def ensure_default_admin_user(self) -> None:
        default_email = self.settings.default_admin_email.strip().lower()
        default_password = self.settings.default_admin_password.strip()
        default_full_name = self.settings.default_admin_full_name.strip() or "Administrator"
        if not default_email or not default_password:
            return

        existing_user = self.user_store.get_user_by_email(default_email)
        if existing_user:
            return

        try:
            self.user_store.create_user(
                email=default_email,
                full_name=default_full_name,
                password_hash=hash_password(default_password),
                role="admin",
            )
        except ValueError:
            return

    def register(self, payload: RegisterRequest) -> AuthTokenResponse:
        full_name = payload.full_name.strip()
        email = str(payload.email).strip().lower()
        password = payload.password

        if len(full_name) < 2:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="full_name must contain at least 2 characters.",
            )
        if len(email) < 3:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="email must contain at least 3 characters.",
            )
        if len(password) < 4:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="password must contain at least 4 characters.",
            )

        if self.user_store.get_user_by_email(email):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="An account with this email already exists.",
            )

        user_record = self.user_store.create_user(
            email=email,
            full_name=full_name,
            password_hash=hash_password(password),
            role="user",
        )
        self.user_store.replace_user_settings_values(str(user_record["_id"]), {})
        return self._build_auth_token_response(user_record)

    def login(self, payload: LoginRequest) -> AuthTokenResponse:
        email = str(payload.email).strip().lower()
        password = payload.password

        user_record = self.user_store.get_user_by_email(email)
        if not user_record:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password.",
            )

        stored_hash = str(user_record.get("password_hash", ""))
        if not verify_password(password, stored_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password.",
            )

        return self._build_auth_token_response(user_record)

    def get_current_user_from_token(self, access_token: str) -> CurrentUserResponse:
        payload = decode_access_token(access_token, self.settings.auth_secret_key)
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired access token.",
            )

        subject = payload.get("sub")
        if not isinstance(subject, str) or not subject.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid access token payload.",
            )

        user_record = self.user_store.get_user_by_id(subject)
        if not user_record:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found for this access token.",
            )
        return self._to_current_user_response(user_record)

    def build_google_authorization_url(self) -> str:
        self._assert_google_oauth_is_configured()
        state_token, _ = create_access_token(
            claims={
                "type": "google_oauth_state",
                "nonce": secrets.token_urlsafe(16),
            },
            secret_key=self.settings.auth_secret_key,
            ttl_minutes=10,
        )
        query = urlencode(
            {
                "client_id": self.settings.auth_google_client_id,
                "redirect_uri": self.settings.auth_google_redirect_uri,
                "response_type": "code",
                "scope": "openid email profile",
                "state": state_token,
                "prompt": "select_account",
                "access_type": "offline",
                "include_granted_scopes": "true",
            },
        )
        return f"{_GOOGLE_OAUTH_AUTHORIZE_URL}?{query}"

    def login_with_google_callback(self, *, code: str, state: str) -> AuthTokenResponse:
        self._assert_google_oauth_is_configured()
        decoded_state = decode_access_token(state, self.settings.auth_secret_key)
        if not decoded_state or decoded_state.get("type") != "google_oauth_state":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid OAuth state.",
            )

        token_payload = self._exchange_google_code_for_token(code)
        id_token = token_payload.get("id_token")
        if not isinstance(id_token, str) or not id_token.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Google response did not include id_token.",
            )

        token_info = self._validate_google_id_token(id_token)
        email = str(token_info.get("email", "")).strip().lower()
        if not email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Google account does not expose a valid email.",
            )

        email_verified = token_info.get("email_verified")
        if str(email_verified).lower() not in {"true", "1"}:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Google email is not verified.",
            )

        full_name = str(token_info.get("name", "")).strip() or email
        user_record = self.user_store.get_user_by_email(email)
        if not user_record:
            user_record = self.user_store.create_user(
                email=email,
                full_name=full_name,
                password_hash=hash_password(secrets.token_urlsafe(32)),
                role="user",
            )
            self.user_store.replace_user_settings_values(str(user_record["_id"]), {})

        return self._build_auth_token_response(user_record)

    def _build_auth_token_response(self, user_record: dict[str, object]) -> AuthTokenResponse:
        current_user = self._to_current_user_response(user_record)
        access_token, expires_in_seconds = create_access_token(
            claims={
                "sub": current_user.id,
                "email": current_user.email,
                "role": current_user.role,
            },
            secret_key=self.settings.auth_secret_key,
            ttl_minutes=self.settings.auth_token_ttl_minutes,
        )
        return AuthTokenResponse(
            access_token=access_token,
            expires_in_seconds=expires_in_seconds,
            user=current_user,
        )

    def _to_current_user_response(self, user_record: dict[str, object]) -> CurrentUserResponse:
        return CurrentUserResponse(
            id=str(user_record.get("_id", "")),
            email=str(user_record.get("email", "")),
            full_name=str(user_record.get("full_name", "")),
            role=str(user_record.get("role", "user")),
        )

    def _assert_google_oauth_is_configured(self) -> None:
        if (
            not self.settings.auth_google_client_id.strip()
            or not self.settings.auth_google_client_secret.strip()
            or not self.settings.auth_google_redirect_uri.strip()
        ):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Google OAuth is not configured.",
            )

    def _exchange_google_code_for_token(self, code: str) -> dict[str, object]:
        body = urlencode(
            {
                "code": code,
                "client_id": self.settings.auth_google_client_id,
                "client_secret": self.settings.auth_google_client_secret,
                "redirect_uri": self.settings.auth_google_redirect_uri,
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
                detail="Failed to exchange Google authorization code.",
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

    def _validate_google_id_token(self, id_token: str) -> dict[str, object]:
        query = urlencode({"id_token": id_token})
        request = Request(f"{_GOOGLE_OAUTH_TOKEN_INFO_URL}?{query}", method="GET")
        try:
            with urlopen(request, timeout=15) as response:
                raw_payload = response.read().decode("utf-8")
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to validate Google id_token.",
            ) from exc

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Invalid Google token validation response.",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Invalid Google token validation payload.",
            )

        audience = str(payload.get("aud", "")).strip()
        if audience != self.settings.auth_google_client_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Google token audience mismatch.",
            )
        return payload


def require_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_HTTP_BEARER),
) -> CurrentUserResponse:
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
        )
    service = AuthService()
    return service.get_current_user_from_token(credentials.credentials)
