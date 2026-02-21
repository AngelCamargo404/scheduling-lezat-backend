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
4. Configura solo las variables de entorno permitidas:
```env
APP_NAME=Lezat Scheduling API
APP_ENV=development
APP_VERSION=0.1.0
API_PREFIX=/api
ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
TRANSCRIPTIONS_STORE=mongodb
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB_NAME=lezat_scheduling
MONGODB_TRANSCRIPTIONS_COLLECTION=transcriptions
MONGODB_CONNECT_TIMEOUT_MS=2000
MONGODB_USER_SETTINGS_COLLECTION=user_integration_settings
MONGODB_USERS_COLLECTION=users
USER_DATA_STORE=mongodb
FIREFLIES_API_URL=https://api.fireflies.ai/graphql
FIREFLIES_API_TIMEOUT_SECONDS=10
FIREFLIES_API_USER_AGENT=LezatSchedulingBackend/1.0
GEMINI_API_KEY=<api-key-gemini>
GEMINI_MODEL=gemini-3-flash-preview
GEMINI_API_TIMEOUT_SECONDS=45
FRONTEND_BASE_URL=http://localhost:3000
NOTION_API_TIMEOUT_SECONDS=30
NOTION_API_VERSION=2022-06-28
NOTION_CLIENT_ID=<notion-client-id>
NOTION_CLIENT_SECRET=<notion-client-secret>
NOTION_REDIRECT_URI=http://localhost:8000/api/integrations/notion/callback
GOOGLE_CALENDAR_CLIENT_ID=<google-client-id>
GOOGLE_CALENDAR_CLIENT_SECRET=<google-client-secret>
GOOGLE_CALENDAR_REDIRECT_URI=http://localhost:8000/api/integrations/google-calendar/callback
GOOGLE_CALENDAR_ID=primary
GOOGLE_CALENDAR_API_TIMEOUT_SECONDS=10
GOOGLE_CALENDAR_EVENT_TIMEZONE=UTC
OUTLOOK_CLIENT_ID=<outlook-client-id>
OUTLOOK_CLIENT_SECRET=<outlook-client-secret>
OUTLOOK_TENANT_ID=common
OUTLOOK_REDIRECT_URI=http://localhost:8000/api/integrations/outlook-calendar/callback
AUTH_GOOGLE_CLIENT_ID=<auth-google-client-id>
AUTH_GOOGLE_CLIENT_SECRET=<auth-google-client-secret>
AUTH_GOOGLE_REDIRECT_URI=http://localhost:8000/api/auth/google/callback
```

### Dominios por entorno
- `APP_ENV=development` (o vacio):
  - Defaults de frontend/CORS en `localhost` (`3000`) y redirect URIs del backend en `http://localhost:8000`.
- `APP_ENV=production`:
  - Defaults a dominios de produccion actuales:
    - Frontend: `https://abundant-balance-production-9587.up.railway.app`
    - Backend/OAuth callbacks: `https://scheduling-lezat-backend-production.up.railway.app`

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
- `POST /api/transcriptions/webhooks/fireflies/{user_id}`
- `POST /api/transcriptions/webhooks/read-ai/{user_id}`
- `GET /api/transcriptions/received`
- `GET /api/transcriptions/received/by-meeting/{meeting_id}`
- `POST /api/transcriptions/backfill/{meeting_id}`
- `GET /api/transcriptions/received/{record_id}`
- `POST /api/v1/transcriptions/webhooks/fireflies/{user_id}`
- `POST /api/v1/transcriptions/webhooks/read-ai/{user_id}`
- `GET /api/v1/transcriptions/received`
- `GET /api/v1/transcriptions/received/by-meeting/{meeting_id}`
- `POST /api/v1/transcriptions/backfill/{meeting_id}`
- `GET /api/v1/transcriptions/received/{record_id}`

## Autenticacion y configuracion por usuario
- El backend crea automaticamente un usuario admin inicial (`admin` / `admin`).
- Las contraseñas se almacenan con hash `PBKDF2-HMAC-SHA256`.
- Tambien puedes autenticar con Google OAuth (openid email profile) configurando:
  - `AUTH_GOOGLE_CLIENT_ID`
  - `AUTH_GOOGLE_CLIENT_SECRET`
  - `AUTH_GOOGLE_REDIRECT_URI`
- `GET/PATCH /api/integrations/settings` y `GET /api/integrations/status` requieren token Bearer y guardan configuracion por usuario en MongoDB (`MONGODB_USER_SETTINGS_COLLECTION` / `user_integration_settings`).
- El almacenamiento de usuarios, configuraciones e historico de transcripciones se gestiona en MongoDB y no se expone para edicion desde la interfaz de integraciones.
- Los valores sensibles no se devuelven en texto plano desde el endpoint de settings (`value=null` para campos sensibles).
- Fireflies es obligatorio para operar los flujos de transcripcion.
- Debes definir `TEAM_LEADER_TIMEZONE` para interpretar horarios de reuniones programadas desde transcripciones (por defecto `America/Bogota`).
- Ademas de Fireflies, debes conectar al menos una salida: Google Calendar, Notion Kanban o Monday Kanban.
- Si usas Notion, debes completar Database ID y los datos clave de integracion (token, permisos y mapeos/campos requeridos por el flujo).
- Si usas Monday, debes completar Board ID, Group ID y los datos clave de integracion (token OAuth, columna de estado y mapeos/campos requeridos por el flujo).
- Para cuentas Monday externas al workspace donde se creo la app, un admin debe instalar primero la app publica (link `response_type=install`) y luego autorizar OAuth desde `/api/integrations/monday/connect`.

## Webhooks de transcripciones
- Los endpoints reciben JSON crudo y normalizan campos clave (meeting id, provider, plataforma y disponibilidad de transcript).
- Para separar configuracion por usuario, usa `BACKEND_BASE_URL` segun entorno:
  - Desarrollo: `http://localhost:8000`
  - Produccion: `https://scheduling-lezat-backend-production.up.railway.app`
  - Fireflies: `{BACKEND_BASE_URL}/api/transcriptions/webhooks/fireflies/{user_id}`
  - Read AI: `{BACKEND_BASE_URL}/api/transcriptions/webhooks/read-ai/{user_id}`
- Los endpoints sin `user_id` (`/api/transcriptions/webhooks/fireflies` y `/api/transcriptions/webhooks/read-ai`) se rechazan con `422` y no procesan el payload.
- Si el `user_id` no existe, el webhook responde `404`.
- Fireflies: cuando llega `eventType=Transcription completed`, el backend usa `meetingId` para consultar la API GraphQL de Fireflies y traer la transcripcion final.
- El backend identifica si el meeting corresponde a Google Meet usando `meeting.platform` o `meeting.url`.
- Cada webhook aceptado se guarda en MongoDB (coleccion `transcriptions`) con payload crudo, `client_reference_id`, estado de enriquecimiento (`enrichment_status`) y transcripcion de Fireflies cuando esta disponible.
- Cada nota/tarea creada desde una transcripcion (Notion/Monday) se registra ademas en MongoDB (coleccion `action_item_creations`) con meeting id, identificadores de salida y estado de sincronizacion con calendarios.
- En respuestas de consulta (`/received`, `/received/{record_id}`, `/received/by-meeting/{meeting_id}`), los registros de Fireflies exponen `transcript_sentences` (oraciones con speaker y tiempos) y `participant_emails` (emails unificados de participantes).
- Si configuras Gemini + Notion/Monday, cada webhook intenta extraer tareas de la reunion y crear tarjetas en el Kanban configurado.
- Si una tarea extraida incluye fecha de entrega (`due_date`) y configuras Google Calendar/Outlook por usuario, el backend tambien crea eventos en calendario.
- `POST /api/transcriptions/backfill/{meeting_id}` sirve para completar registros antiguos, pero no forma parte del onboarding base.

## Calidad y pruebas
- Ejecutar pruebas:
```bash
pytest
```
- Lint:
```bash
ruff check .
```
