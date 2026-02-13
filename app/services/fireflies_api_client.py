import json
from collections.abc import Mapping
from typing import Any
from urllib import error, request


class FirefliesApiError(Exception):
    pass


class FirefliesApiClient:
    def __init__(
        self,
        api_url: str,
        api_key: str,
        timeout_seconds: float = 10.0,
        user_agent: str = "LezatSchedulingBackend/1.0",
    ) -> None:
        self.api_url = api_url
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

    def fetch_transcript_by_meeting_id(self, meeting_id: str) -> dict[str, Any]:
        queries = (
            """
            query TranscriptById($id: String!) {
              transcript(id: $id) {
                id
                title
                date
                meeting_link
                transcript_url
                organizer_email
                host_email
                participants
                fireflies_users
                user {
                  email
                }
                meeting_attendees {
                  email
                  name
                  displayName
                }
                sentences {
                  index
                  speaker_name
                  speaker_id
                  text
                  start_time
                  end_time
                }
              }
            }
            """,
            """
            query TranscriptById($id: String!) {
              transcript(id: $id) {
                id
                title
                date
                meeting_link
                transcript_url
                organizer_email
                participants
                meeting_attendees {
                  email
                }
                sentences {
                  index
                  speaker_name
                  speaker_id
                  text
                  start_time
                  end_time
                }
              }
            }
            """,
            """
            query TranscriptById($id: String!) {
              transcript(id: $id) {
                id
                title
                date
                meeting_link
                transcript_url
                sentences {
                  text
                }
              }
            }
            """,
        )

        graphql_error: FirefliesApiError | None = None
        for graphql_query in queries:
            try:
                return self._fetch_transcript_with_query(
                    meeting_id=meeting_id,
                    graphql_query=graphql_query,
                )
            except FirefliesApiError as exc:
                if "GraphQL error" not in str(exc):
                    raise
                graphql_error = exc

        if graphql_error:
            raise graphql_error
        raise FirefliesApiError("Fireflies transcript query failed.")

    def _fetch_transcript_with_query(
        self,
        meeting_id: str,
        graphql_query: str,
    ) -> dict[str, Any]:
        payload = {"query": graphql_query, "variables": {"id": meeting_id}}
        raw_payload = json.dumps(payload).encode("utf-8")
        req = request.Request(
            self.api_url,
            data=raw_payload,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                response_body = response.read()
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise FirefliesApiError(
                f"Fireflies API HTTP {exc.code}: {body or 'empty response body'}"
            ) from exc
        except error.URLError as exc:
            raise FirefliesApiError(f"Fireflies API connection error: {exc.reason}") from exc

        try:
            parsed_body = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise FirefliesApiError("Fireflies API returned invalid JSON.") from exc

        errors_payload = parsed_body.get("errors")
        if errors_payload:
            raise FirefliesApiError(f"Fireflies API GraphQL error: {errors_payload}")

        data = parsed_body.get("data")
        if not isinstance(data, Mapping):
            raise FirefliesApiError("Fireflies API response missing data.")

        transcript = data.get("transcript")
        if not isinstance(transcript, Mapping):
            raise FirefliesApiError("Fireflies transcript not found for provided meeting_id.")

        return dict(transcript)
