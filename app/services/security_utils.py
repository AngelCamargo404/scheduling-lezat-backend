from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any

PBKDF2_ALGORITHM = "sha256"
PBKDF2_ITERATIONS = 390_000
PBKDF2_SALT_BYTES = 16


def hash_password(password: str) -> str:
    password_bytes = password.encode("utf-8")
    salt = os.urandom(PBKDF2_SALT_BYTES)
    digest = hashlib.pbkdf2_hmac(
        PBKDF2_ALGORITHM,
        password_bytes,
        salt,
        PBKDF2_ITERATIONS,
    )
    return (
        "pbkdf2_sha256"
        f"${PBKDF2_ITERATIONS}"
        f"${_b64url_encode(salt)}"
        f"${_b64url_encode(digest)}"
    )


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, raw_iterations, raw_salt, raw_digest = stored_hash.split("$", maxsplit=3)
    except ValueError:
        return False

    if algorithm != "pbkdf2_sha256":
        return False

    try:
        iterations = int(raw_iterations)
        salt = _b64url_decode(raw_salt)
        expected_digest = _b64url_decode(raw_digest)
    except (ValueError, TypeError):
        return False

    actual_digest = hashlib.pbkdf2_hmac(
        PBKDF2_ALGORITHM,
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual_digest, expected_digest)


def create_access_token(
    *,
    claims: dict[str, Any],
    secret_key: str,
    ttl_minutes: int,
) -> tuple[str, int]:
    issued_at = datetime.now(UTC)
    expires_at = issued_at + timedelta(minutes=ttl_minutes)

    payload = {
        **claims,
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    payload_bytes = json.dumps(
        payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    payload_segment = _b64url_encode(payload_bytes)
    signature = hmac.new(
        secret_key.encode("utf-8"),
        payload_segment.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    signature_segment = _b64url_encode(signature)
    token = f"{payload_segment}.{signature_segment}"
    expires_in_seconds = max(int((expires_at - issued_at).total_seconds()), 0)
    return token, expires_in_seconds


def decode_access_token(token: str, secret_key: str) -> dict[str, Any] | None:
    try:
        payload_segment, signature_segment = token.split(".", maxsplit=1)
    except ValueError:
        return None

    expected_signature = hmac.new(
        secret_key.encode("utf-8"),
        payload_segment.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    provided_signature = _b64url_decode(signature_segment)
    if not hmac.compare_digest(expected_signature, provided_signature):
        return None

    payload_bytes = _b64url_decode(payload_segment)
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    raw_expiration = payload.get("exp")
    if not isinstance(raw_expiration, int):
        return None
    if raw_expiration < int(datetime.now(UTC).timestamp()):
        return None

    return payload


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding_size = (-len(value)) % 4
    padded = f"{value}{'=' * padding_size}"
    return base64.urlsafe_b64decode(padded.encode("ascii"))
