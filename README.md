# Scheduling Lezat Backend

API del sistema de agendamiento de Lezat construida con FastAPI.
Este servicio expone endpoints para salud del sistema, autenticacion, disponibilidad de horarios e ingesta de transcripciones.

## Tecnologias
- FastAPI
- Uvicorn
- Pydantic Settings
- Pytest + HTTPX
- Ruff

## Requisitos
- Python 3.11+
- pip

## Traer el proyecto a local
1. Clona el repositorio:
```bash
git clone <URL_DEL_REPOSITORIO>
```
2. Entra al backend:
```bash
cd scheduling-lezat-backend
```

Si ya lo tienes clonado y solo quieres actualizarlo:
```bash
git pull origin <tu-rama>
```

## Configuracion local
1. Crea el archivo de entorno:
```bash
cp .env.example .env
```
En Windows PowerShell tambien puedes usar:
```powershell
Copy-Item .env.example .env
```
2. Crea y activa entorno virtual:
```bash
python -m venv .venv
```
PowerShell:
```powershell
.\.venv\Scripts\Activate.ps1
```
3. Instala dependencias:
```bash
pip install -e ".[dev]"
```
4. (Opcional) configura secretos para webhooks:
```env
FIREFLIES_WEBHOOK_SECRET=<secreto-compartido>
FIREFLIES_API_URL=https://api.fireflies.ai/graphql
FIREFLIES_API_KEY=<api-key-fireflies>
FIREFLIES_API_TIMEOUT_SECONDS=10
FIREFLIES_API_USER_AGENT=LezatSchedulingBackend/1.0
READ_AI_WEBHOOK_SECRET=<secreto-compartido>
TRANSCRIPTION_AUTOSYNC_ENABLED=true
TRANSCRIPTIONS_STORE=mongodb
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB_NAME=lezat_scheduling
MONGODB_TRANSCRIPTIONS_COLLECTION=transcriptions
MONGODB_CONNECT_TIMEOUT_MS=2000
GEMINI_API_KEY=<api-key-gemini>
GEMINI_MODEL=gemini-3-flash-preview
GEMINI_API_TIMEOUT_SECONDS=45
NOTION_API_TOKEN=<token-de-integracion-notion>
NOTION_TASKS_DATABASE_ID=<database-id-kanban>
NOTION_TASK_STATUS_PROPERTY=Status
ACTION_ITEMS_TEST_DUE_DATE=
GOOGLE_CALENDAR_API_TOKEN=<access-token-google-calendar>
GOOGLE_CALENDAR_REFRESH_TOKEN=<refresh-token-google-calendar>
GOOGLE_CALENDAR_ID=primary
GOOGLE_CALENDAR_API_TIMEOUT_SECONDS=10
GOOGLE_CALENDAR_EVENT_TIMEZONE=UTC
```

## Correr en local
1. Inicia la API:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```
2. Verifica que este arriba:
```txt
http://localhost:8000/api/health
```

## Documentacion de API
- Swagger UI: `http://localhost:8000/api/docs`
- ReDoc: `http://localhost:8000/api/redoc`

## Endpoints principales
- `GET /api/health`
- `POST /api/auth/register`
- `POST /api/auth/login`
- `GET /api/auth/google/start`
- `GET /api/auth/google/callback`
- `GET /api/auth/me` (requiere `Authorization: Bearer <token>`)
- `GET /api/integrations/status`
- `GET /api/integrations/settings`
- `PATCH /api/integrations/settings`
- `POST /api/v1/auth/register`
- `POST /api/v1/auth/login`
- `GET /api/v1/auth/google/start`
- `GET /api/v1/auth/google/callback`
- `GET /api/v1/auth/me` (requiere `Authorization: Bearer <token>`)
- `GET /api/v1/integrations/status`
- `GET /api/v1/integrations/settings`
- `PATCH /api/v1/integrations/settings`
- `GET /api/scheduling/slots`
- `GET /api/v1/scheduling/slots`
- `POST /api/transcriptions/webhooks/fireflies`
- `POST /api/transcriptions/webhooks/read-ai`
- `GET /api/transcriptions/received`
- `GET /api/transcriptions/received/by-meeting/{meeting_id}`
- `POST /api/transcriptions/backfill/{meeting_id}`
- `GET /api/transcriptions/received/{record_id}`
- `POST /api/v1/transcriptions/webhooks/fireflies`
- `POST /api/v1/transcriptions/webhooks/read-ai`
- `GET /api/v1/transcriptions/received`
- `GET /api/v1/transcriptions/received/by-meeting/{meeting_id}`
- `POST /api/v1/transcriptions/backfill/{meeting_id}`
- `GET /api/v1/transcriptions/received/{record_id}`

## Autenticacion y configuracion por usuario
- El backend crea automaticamente un usuario admin inicial con `DEFAULT_ADMIN_EMAIL` y `DEFAULT_ADMIN_PASSWORD` (por defecto: `admin` / `admin`).
- Las contraseñas se almacenan con hash `PBKDF2-HMAC-SHA256`.
- Tambien puedes autenticar con Google OAuth (openid email profile) configurando:
  - `AUTH_GOOGLE_CLIENT_ID`
  - `AUTH_GOOGLE_CLIENT_SECRET`
  - `AUTH_GOOGLE_REDIRECT_URI`
- `GET/PATCH /api/integrations/settings` y `GET /api/integrations/status` requieren token Bearer y guardan configuracion por usuario en MongoDB (`MONGODB_USER_SETTINGS_COLLECTION`).
- El almacenamiento de usuarios, configuraciones e historico de transcripciones se gestiona en MongoDB y no se expone para edicion desde la interfaz de integraciones.
- Los valores sensibles no se devuelven en texto plano desde el endpoint de settings (`value=null` para campos sensibles).

## Webhooks de transcripciones
- Los endpoints reciben JSON crudo y normalizan campos clave (meeting id, provider, plataforma y disponibilidad de transcript).
- Fireflies: si configuras `FIREFLIES_WEBHOOK_SECRET`, el backend valida `x-hub-signature` (HMAC SHA-256) del payload. Para pruebas manuales tambien acepta `X-Webhook-Secret` o `Authorization: Bearer <token>`.
- Fireflies: cuando llega `eventType=Transcription completed`, el backend usa `meetingId` para consultar la API GraphQL de Fireflies y traer la transcripcion final.
- READ AI: No requiere validacion de secreto (`READ_AI_WEBHOOK_SECRET` no se usa). El webhook se asocia por API Key.
- El backend identifica si el meeting corresponde a Google Meet usando `meeting.platform` o `meeting.url`.
- Cada webhook aceptado se guarda en MongoDB (coleccion `transcriptions`) con payload crudo, `client_reference_id`, estado de enriquecimiento (`enrichment_status`) y transcripcion de Fireflies cuando esta disponible.
- Cada nota/tarea creada en Notion desde una transcripcion se registra ademas en MongoDB (coleccion `action_item_creations`) con meeting id, pagina de Notion y estado de sincronizacion con calendarios.
- En respuestas de consulta (`/received`, `/received/{record_id}`, `/received/by-meeting/{meeting_id}`), los registros de Fireflies exponen `transcript_sentences` (oraciones con speaker y tiempos) y `participant_emails` (emails unificados de participantes).
- Si configuras Gemini + Notion, cada webhook intenta extraer tareas de la reunion y crear tarjetas en un Kanban de Notion usando la propiedad de estado configurada.
- Si `TRANSCRIPTION_AUTOSYNC_ENABLED=false`, el backend sigue recibiendo y guardando la transcripcion, pero omite la creacion automatica de notas y eventos.
- Si una tarea extraida incluye fecha de entrega (`due_date`) y configuras `GOOGLE_CALENDAR_API_TOKEN`, el backend tambien crea un evento de dia completo en Google Calendar con el contexto de la reunion en la descripcion.
- Para pruebas controladas, puedes forzar fecha en tareas sin `due_date` usando `ACTION_ITEMS_TEST_DUE_DATE=YYYY-MM-DD`.
- Si tienes registros antiguos sin texto, usa `POST /api/transcriptions/backfill/{meeting_id}` para reconsultar Fireflies y actualizar los documentos existentes por ese `meeting_id`.

## Calidad y pruebas
- Ejecutar pruebas:
```bash
pytest
```
- Lint:
```bash
ruff check .
```
