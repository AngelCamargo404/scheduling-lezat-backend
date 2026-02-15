import json
from collections.abc import Mapping
from typing import Any
from urllib import error, request


class ReadAiApiError(Exception):
    pass


class ReadAiApiClient:
    def __init__(
        self,
        api_url: str,
        api_key: str,
        timeout_seconds: float = 10.0,
        user_agent: str = "LezatSchedulingBackend/1.0",
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

    def fetch_meeting_details(self, meeting_id: str) -> dict[str, Any]:
        """
        Fetches meeting details from Read AI API.
        """
        if not self.api_key:
             return {}

        # Assuming standard REST endpoint structure: /meetings/{id}
        # Adjust endpoint path if documentation specifies otherwise.
        url = f"{self.api_url}/meetings/{meeting_id}" 
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": self.user_agent,
            "Content-Type": "application/json",
        }

        req = request.Request(url, headers=headers, method="GET")

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                if response.status != 200:
                    raise ReadAiApiError(f"API returned status {response.status}")
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as e:
            if e.code == 404:
                 return {}
            try:
                error_body = e.read().decode("utf-8")
            except Exception:
                error_body = "Unknown error"
            raise ReadAiApiError(f"HTTP Error {e.code}: {error_body}") from e
        except error.URLError as e:
            raise ReadAiApiError(f"Network Error: {e.reason}") from e
