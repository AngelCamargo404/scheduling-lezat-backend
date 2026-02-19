import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from http.client import RemoteDisconnected
from time import sleep
from typing import Any
from urllib import error, parse, request

from app.services.action_item_models import ActionItem


class GeminiActionItemsError(Exception):
    pass


class GeminiActionItemsClient:
    def __init__(
        self,
        api_key: str,
        model: str,
        timeout_seconds: float = 20.0,
        api_base_url: str = "https://generativelanguage.googleapis.com/v1beta",
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.api_base_url = api_base_url.rstrip("/")

    def extract_action_items(
        self,
        *,
        meeting_id: str | None,
        transcript_text: str,
        transcript_sentences: list[dict[str, Any]],
        participant_emails: list[str],
    ) -> list[ActionItem]:
        reference_date = datetime.now(UTC).date()
        prompt = self._build_prompt(
            meeting_id=meeting_id,
            reference_date=reference_date,
            transcript_text=transcript_text,
            transcript_sentences=transcript_sentences,
            participant_emails=participant_emails,
        )
        response_payload = self._generate(prompt)
        output_text = self._extract_text_response(response_payload)
        parsed_output = self._parse_json_output(output_text)
        raw_items = parsed_output.get("action_items")
        if not isinstance(raw_items, list):
            return []

        action_items: list[ActionItem] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                continue
            item = ActionItem.from_payload(raw_item, reference_date=reference_date)
            if not item:
                continue
            action_items.append(item)
        return action_items

    def _generate(self, prompt: str) -> dict[str, Any]:
        query = parse.urlencode({"key": self.api_key})
        endpoint = f"{self.api_base_url}/models/{self.model}:generateContent?{query}"
        payload = {
            "system_instruction": {
                "parts": [
                    {
                        "text": (
                            "Eres un analista de reuniones. Extrae unicamente tareas accionables "
                            "asignadas a personas. Responde JSON valido."
                        ),
                    },
                ],
            },
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
        }
        req = request.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        max_attempts = 3
        response_body: bytes | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    response_body = response.read()
                break
            except TimeoutError as exc:
                if attempt >= max_attempts:
                    raise GeminiActionItemsError("Gemini API request timed out.") from exc
            except RemoteDisconnected as exc:
                if attempt >= max_attempts:
                    raise GeminiActionItemsError(
                        "Gemini API connection was closed before sending a response.",
                    ) from exc
            except error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="ignore")
                is_retryable_status = exc.code in {429, 500, 502, 503, 504}
                if not is_retryable_status or attempt >= max_attempts:
                    raise GeminiActionItemsError(
                        f"Gemini API HTTP {exc.code}: {body or 'empty response body'}",
                    ) from exc
            except error.URLError as exc:
                if attempt >= max_attempts:
                    raise GeminiActionItemsError(
                        f"Gemini API connection error: {exc.reason}",
                    ) from exc

            sleep(0.5 * attempt)

        if response_body is None:
            raise GeminiActionItemsError("Gemini API request failed after multiple attempts.")

        try:
            parsed_body = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise GeminiActionItemsError("Gemini API returned invalid JSON.") from exc

        if not isinstance(parsed_body, dict):
            raise GeminiActionItemsError("Gemini API response is not a JSON object.")
        return parsed_body

    def _extract_text_response(self, payload: Mapping[str, Any]) -> str:
        candidates = payload.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            raise GeminiActionItemsError("Gemini API response missing candidates.")

        first_candidate = candidates[0]
        if not isinstance(first_candidate, Mapping):
            raise GeminiActionItemsError("Gemini API response candidate is invalid.")

        content = first_candidate.get("content")
        if not isinstance(content, Mapping):
            raise GeminiActionItemsError("Gemini API response missing content.")

        parts = content.get("parts")
        if not isinstance(parts, list):
            raise GeminiActionItemsError("Gemini API response missing content parts.")

        chunks: list[str] = []
        for part in parts:
            if not isinstance(part, Mapping):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                chunks.append(text.strip())

        if not chunks:
            raise GeminiActionItemsError("Gemini API response did not include text output.")
        return "\n".join(chunks)

    def _parse_json_output(self, raw_text: str) -> dict[str, Any]:
        direct = self._loads_json_if_possible(raw_text)
        if direct is not None:
            return direct

        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise GeminiActionItemsError("Gemini output is not valid JSON.")

        candidate = raw_text[start : end + 1]
        parsed_candidate = self._loads_json_if_possible(candidate)
        if parsed_candidate is None:
            raise GeminiActionItemsError("Gemini output could not be parsed as JSON.")
        return parsed_candidate

    def _loads_json_if_possible(self, value: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None

        if not isinstance(parsed, dict):
            return None
        return parsed

    def _build_prompt(
        self,
        *,
        meeting_id: str | None,
        reference_date: date,
        transcript_text: str,
        transcript_sentences: list[dict[str, Any]],
        participant_emails: list[str],
    ) -> str:
        compact_sentences = transcript_sentences[:200]
        serialized_sentences = json.dumps(compact_sentences, ensure_ascii=False)
        serialized_emails = json.dumps(participant_emails, ensure_ascii=False)
        serialized_text = transcript_text.strip()

        return (
            "Analiza esta reunion y extrae SOLO tareas accionables reales.\n"
            "No incluyas resumenes, opiniones, contexto, notas generales o texto que no sea tarea.\n"
            "Una tarea valida debe implicar una accion concreta o compromiso verificable.\n"
            "Si una tarea no tiene responsable claro, deja assignee_email y assignee_name en null.\n"
            "Reglas de fecha:\n"
            "- fecha_actual: "
            f"{reference_date.isoformat()}\n"
            f"- anio_actual: {reference_date.year}\n"
            f"- mes_actual: {reference_date.month:02d}\n"
            "- Si hay fecha relativa (manana, ayer, en 1 semana, dentro de 2 semanas, 1 mes), "
            "convierte a YYYY-MM-DD tomando como base fecha_actual.\n"
            "- Si aparece fecha tipo '23 de febrero' sin anio, usa anio_actual.\n"
            "- Si aparece '25 de este mes', usa mes_actual y anio_actual.\n"
            "Si no existe una fecha inferible, deja due_date en null.\n"
            "Reglas de agenda de reunion:\n"
            "- Si la tarea implica agendar reunion y hay hora explicita, llena scheduled_start en ISO "
            "local YYYY-MM-DDTHH:MM:SS.\n"
            "- Si hay fin/duracion explicita, llena scheduled_end. Si no, deja scheduled_end en null.\n"
            "- Si se menciona zona horaria explicita (EST, PST, hora de Mexico, etc.), llena "
            "event_timezone con formato IANA (ej. America/New_York, America/Mexico_City).\n"
            "- Si hay frecuencia (todos los jueves, cada semana, cada mes, al inicio de cada mes), "
            "llena recurrence_rule usando RRULE sin prefijo RRULE: (ej. "
            "FREQ=WEEKLY;INTERVAL=1;BYDAY=TH).\n"
            "- Si la transcripcion menciona explicitamente Google Meet o Microsoft Teams, llena "
            "online_meeting_platform con: google_meet o microsoft_teams.\n"
            "- Si se solicita una reunion pero sin proveedor explicito, usa online_meeting_platform=auto.\n"
            "- Si no aplica agenda/recurrencia/videollamada, deja esos campos en null.\n"
            "Incluye en source_sentence la frase exacta donde aparezca la tarea y/o la pista temporal.\n"
            "Devuelve un JSON con este formato exacto:\n"
            "{\n"
            '  "action_items": [\n'
            "    {\n"
            '      "title": "string",\n'
            '      "assignee_email": "string|null",\n'
            '      "assignee_name": "string|null",\n'
            '      "due_date": "YYYY-MM-DD|null",\n'
            '      "scheduled_start": "YYYY-MM-DDTHH:MM:SS|null",\n'
            '      "scheduled_end": "YYYY-MM-DDTHH:MM:SS|null",\n'
            '      "event_timezone": "IANA_TIMEZONE|null",\n'
            '      "recurrence_rule": "RRULE_WITHOUT_PREFIX|null",\n'
            '      "online_meeting_platform": "google_meet|microsoft_teams|auto|null",\n'
            '      "details": "string|null",\n'
            '      "source_sentence": "string|null"\n'
            "    }\n"
            "  ]\n"
            "}\n\n"
            f"meeting_id: {meeting_id or 'null'}\n"
            f"participant_emails: {serialized_emails}\n"
            f"sentences: {serialized_sentences}\n"
            f"transcript:\n{serialized_text}"
        )
