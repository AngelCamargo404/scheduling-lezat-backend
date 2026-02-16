import json
import logging
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.config import Settings, get_settings
from app.schemas.auth import CurrentUserResponse
from app.schemas.transcription import (
    TranscriptionBackfillResponse,
    TranscriptionProvider,
    TranscriptionRecord,
    TranscriptionRecordsResponse,
    TranscriptionWebhookResponse,
)
from app.services.auth_service import require_current_user
from app.services.transcription_service import TranscriptionService
from app.services.user_store import create_user_store

router = APIRouter(prefix="/transcriptions", tags=["transcriptions"])
logger = logging.getLogger(__name__)


@router.post(
    "/webhooks/fireflies",
    response_model=TranscriptionWebhookResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def receive_fireflies_webhook(
    request: Request,
) -> TranscriptionWebhookResponse:
    payload, raw_body = await _load_payload_and_raw_body(request)
    logger.info(
        "Webhook received provider=fireflies path=%s has_signature=%s",
        str(request.url.path),
        bool(request.headers.get("x-hub-signature")),
    )
    return _process_webhook(
        provider=TranscriptionProvider.fireflies,
        payload=payload,
        request=request,
        raw_body=raw_body,
        signature=request.headers.get("x-hub-signature"),
    )


@router.post(
    "/webhooks/read-ai",
    response_model=TranscriptionWebhookResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def receive_read_ai_webhook(
    payload: dict[str, Any],
    request: Request,
) -> TranscriptionWebhookResponse:
    logger.info("Webhook received provider=read_ai path=%s", str(request.url.path))
    return _process_webhook(
        provider=TranscriptionProvider.read_ai,
        payload=payload,
        request=request,
    )


@router.get(
    "/received",
    response_model=TranscriptionRecordsResponse,
)
def list_received_transcriptions(limit: int = 50) -> TranscriptionRecordsResponse:
    settings = get_settings()
    service = TranscriptionService(settings)
    return service.list_received(limit=limit)


@router.get(
    "/received/by-meeting/{meeting_id}",
    response_model=TranscriptionRecord,
)
def get_received_transcription_by_meeting_id(meeting_id: str) -> TranscriptionRecord:
    settings = get_settings()
    service = TranscriptionService(settings)
    return service.get_received_by_meeting_id(meeting_id)


@router.post(
    "/backfill/{meeting_id}",
    response_model=TranscriptionBackfillResponse,
)
def backfill_transcription_by_meeting_id(
    meeting_id: str,
    current_user: CurrentUserResponse = Depends(require_current_user),
) -> TranscriptionBackfillResponse:
    settings = _build_effective_settings_for_user(current_user)
    service = TranscriptionService(settings)
    return service.backfill_by_meeting_id(meeting_id)


@router.get(
    "/received/{record_id}",
    response_model=TranscriptionRecord,
)
def get_received_transcription(record_id: str) -> TranscriptionRecord:
    settings = get_settings()
    service = TranscriptionService(settings)
    return service.get_received(record_id)


def _process_webhook(
    provider: TranscriptionProvider,
    payload: Mapping[str, Any],
    request: Request,
    raw_body: bytes | None = None,
    signature: str | None = None,
) -> TranscriptionWebhookResponse:
    settings = get_settings()
    service = TranscriptionService(settings)
    shared_secret = _extract_shared_secret(request)
    try:
        response = service.process_webhook(
            provider=provider,
            payload=payload,
            shared_secret=shared_secret,
            raw_body=raw_body,
            signature=signature,
        )
    except HTTPException as exc:
        logger.warning(
            "Webhook rejected provider=%s path=%s status_code=%s detail=%s",
            provider.value,
            str(request.url.path),
            exc.status_code,
            exc.detail,
        )
        raise
    except Exception:
        logger.exception(
            "Webhook processing failed provider=%s path=%s",
            provider.value,
            str(request.url.path),
        )
        raise

    logger.info(
        "Webhook processed provider=%s path=%s meeting_id=%s stored_record_id=%s enrichment_status=%s",
        provider.value,
        str(request.url.path),
        response.meeting_id,
        response.stored_record_id,
        response.enrichment_status,
    )
    return response


def _extract_shared_secret(request: Request) -> str | None:
    x_webhook_secret = request.headers.get("x-webhook-secret")
    if x_webhook_secret:
        return x_webhook_secret.strip()

    authorization = request.headers.get("authorization")
    if not authorization:
        return None

    auth_scheme, _, auth_token = authorization.partition(" ")
    if auth_scheme.lower() != "bearer":
        return None

    token = auth_token.strip()
    return token or None


async def _load_payload_and_raw_body(request: Request) -> tuple[dict[str, Any], bytes]:
    raw_body = await request.body()
    try:
        parsed_payload = json.loads(raw_body or b"{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Request body must be valid JSON.",
        ) from exc

    if not isinstance(parsed_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Request body must be a JSON object.",
        )

    return parsed_payload, raw_body


def _build_effective_settings_for_user(current_user: CurrentUserResponse) -> Settings:
    base_settings = get_settings()
    user_store = create_user_store(base_settings)
    user_values = user_store.get_user_settings_values(current_user.id)
    merged_payload = base_settings.model_dump()
    # By product rule, autosync starts enabled unless the user explicitly disables it.
    merged_payload["transcription_autosync_enabled"] = True
    if not user_values:
        return Settings.model_validate(merged_payload)

    overrides: dict[str, Any] = {}
    for env_var, raw_value in user_values.items():
        if env_var == "GEMINI_API_KEY":
            continue
        attr_name = env_var.lower()
        if not hasattr(base_settings, attr_name):
            continue
        base_value = getattr(base_settings, attr_name)
        if isinstance(base_value, list):
            overrides[attr_name] = [
                value.strip()
                for value in raw_value.split(",")
                if value.strip()
            ]
        elif isinstance(base_value, bool):
            lowered = raw_value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                overrides[attr_name] = True
            elif lowered in {"0", "false", "no", "off"}:
                overrides[attr_name] = False
        elif isinstance(base_value, int):
            overrides[attr_name] = int(raw_value)
        elif isinstance(base_value, float):
            overrides[attr_name] = float(raw_value)
        else:
            overrides[attr_name] = raw_value

    merged_payload.update(overrides)
    return Settings.model_validate(merged_payload)
