"""
Eduqat API client for Python.

Provides methods to interact with the Eduqat public API.
"""

import os
import json
import urllib.request
import urllib.error
from typing import Any, Optional
from dotenv import load_dotenv


class EduqatApiError(Exception):
    """Exception raised for Eduqat API errors."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class EduqatClient:
    """Client for interacting with the Eduqat public API."""

    def __init__(self, api_key: Optional[str] = None, env_file: str = '.env.local'):
        """
        Initialize the Eduqat client.

        Args:
            api_key: Optional API key. If not provided, reads from EDUQAT_API_KEY env var.
            env_file: Path to env file to load (default: .env.local)
        """
        # Load environment variables
        load_dotenv(env_file)

        self.api_key = api_key or os.getenv('EDUQAT_API_KEY')
        if not self.api_key:
            raise EduqatApiError('EDUQAT_API_KEY environment variable is required')

        self.base_url = 'https://public-api.eduqat.com'

    def _make_request(
        self,
        endpoint: str,
        method: str = 'GET',
        body: Optional[dict] = None
    ) -> dict:
        """
        Make an HTTP request to the Eduqat API.

        Args:
            endpoint: API endpoint (e.g., '/manage/admin/enrollments')
            method: HTTP method (GET, POST, etc.)
            body: Optional request body for POST requests

        Returns:
            Parsed JSON response

        Raises:
            EduqatApiError: If the request fails
        """
        url = f'{self.base_url}{endpoint}'

        headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.api_key,
        }

        data = None
        if body:
            data = json.dumps(body).encode('utf-8')

        req = urllib.request.Request(url, data=data, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            try:
                error_data = json.loads(error_body)
                message = error_data.get('message', f'HTTP {e.code}: {e.reason}')
            except json.JSONDecodeError:
                message = f'HTTP {e.code}: {e.reason}'
            raise EduqatApiError(message, status_code=e.code)
        except urllib.error.URLError as e:
            raise EduqatApiError(f'URL Error: {e.reason}')
        except Exception as e:
            raise EduqatApiError(f'Unexpected error: {str(e)}')

    def get_enrollments(self) -> dict:
        """
        Get all enrollments from /manage/admin/enrollments.

        Returns:
            Dict with 'count' and 'items' keys containing enrollment data.

        Example response structure:
            {
                "count": 71,
                "items": [
                    {
                        "id": "0V39brms4QzUz92mRC9a",
                        "user_id": "7dee0c2e-fe07-436f-8a85-3a8cc8574bc9",
                        "course_id": 2,
                        "learning_progress": 40,
                        "metadata": {
                            "started_at": "2025-11-21T04:02:08Z",
                            ...
                        },
                        "completions": {...},
                        "user_data": {...},
                        "certificates": [...],
                        ...
                    },
                    ...
                ]
            }
        """
        return self._make_request('/manage/admin/enrollments')

    def get(self, endpoint: str) -> dict:
        """
        Make a GET request to any endpoint.

        Args:
            endpoint: API endpoint path

        Returns:
            Parsed JSON response
        """
        return self._make_request(endpoint, method='GET')

    def post(self, endpoint: str, body: dict) -> dict:
        """
        Make a POST request to any endpoint.

        Args:
            endpoint: API endpoint path
            body: Request body

        Returns:
            Parsed JSON response
        """
        return self._make_request(endpoint, method='POST', body=body)