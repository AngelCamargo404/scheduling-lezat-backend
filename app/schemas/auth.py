from pydantic import BaseModel


class CurrentUserResponse(BaseModel):
    id: str
    email: str
    full_name: str
    role: str


class RegisterRequest(BaseModel):
    full_name: str
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class AuthTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_seconds: int
    user: CurrentUserResponse
