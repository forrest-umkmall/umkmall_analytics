"""
Eduqat API client for Python.

Provides methods to interact with the Eduqat public API.
"""

import os
import json
import urllib.request
import urllib.parse
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
        body: Optional[dict] = None,
        params: Optional[dict] = None
    ) -> dict:
        """
        Make an HTTP request to the Eduqat API.

        Args:
            endpoint: API endpoint (e.g., '/manage/admin/enrollments')
            method: HTTP method (GET, POST, etc.)
            body: Optional request body for POST requests
            params: Optional query parameters for GET requests

        Returns:
            Parsed JSON response

        Raises:
            EduqatApiError: If the request fails
        """
        url = f'{self.base_url}{endpoint}'

        # Add query parameters if provided
        if params:
            query_string = urllib.parse.urlencode(params)
            url = f'{url}?{query_string}'

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

    def get_enrollments(self, page: Optional[int] = None, limit: Optional[int] = None) -> dict:
        """
        Get all enrollments from /manage/admin/enrollments.

        Automatically fetches all pages if page/limit are not provided.

        Args:
            page: Optional specific page to fetch (1-indexed)
            limit: Optional number of items per page

        Returns:
            Dict with 'count' and 'items' keys containing enrollment data.

        Note:
            As of 2025-11-26, the Eduqat API appears to have a limitation where
            pagination parameters (page, limit) are not respected. The pagination
            logic below is implemented for when/if the API is fixed.

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
        # If specific page/limit requested, return single page
        if page is not None or limit is not None:
            params = {}
            if page is not None:
                params['page'] = page
            if limit is not None:
                params['limit'] = limit
            response = self._make_request('/manage/admin/enrollments', params=params)
            # Return actual count based on items fetched, not API's count field
            return {
                'count': len(response.get('items', [])),
                'items': response.get('items', [])
            }

        # Otherwise, fetch all pages automatically
        all_items = []
        page = 1
        page_limit = 100  # Use a larger page size for efficiency

        while True:
            params = {'page': page, 'limit': page_limit}
            response = self._make_request('/manage/admin/enrollments', params=params)

            items = response.get('items', [])
            all_items.extend(items)

            # If we got fewer items than the limit, we've reached the last page
            if len(items) < page_limit:
                break

            page += 1

        return {
            'count': len(all_items),
            'items': all_items
        }

    def get_users(self, page: Optional[int] = None, limit: Optional[int] = None) -> dict:
        """
        Get all users from /manage/admin/users.

        Automatically fetches all pages if page/limit are not provided.

        Args:
            page: Optional specific page to fetch (1-indexed)
            limit: Optional number of items per page

        Returns:
            Dict with 'count' and 'items' keys containing user data.

        Note:
            As of 2025-11-26, the Eduqat API appears to have a limitation where
            pagination parameters (page, limit) are not respected. The API returns
            a maximum of 25 items regardless of parameters. The pagination logic
            below is implemented for when/if the API is fixed.

        Example response structure:
            {
                "count": 51,
                "items": [
                    {
                        "id": 78,
                        "user_id": 78,
                        "subid": "7dee0c2e-fe07-436f-8a85-3a8cc8574bc9",
                        "name": "Novita Fadhilah",
                        "email": "novitafadhilah.nf@gmail.com",
                        "phone_number": null,
                        "role": "learner",
                        "status": "ACTIVE",
                        "total_course": 0,
                        "total_enrollment": 1,
                        "created_at": "2025-11-21T04:01:39.406Z",
                        ...
                    },
                    ...
                ]
            }
        """
        # If specific page/limit requested, return single page
        if page is not None or limit is not None:
            params = {}
            if page is not None:
                params['page'] = page
            if limit is not None:
                params['limit'] = limit
            response = self._make_request('/manage/admin/users', params=params)
            # Return actual count based on items fetched, not API's count field
            return {
                'count': len(response.get('items', [])),
                'items': response.get('items', [])
            }

        # Otherwise, fetch all pages automatically
        all_items = []
        page = 1
        page_limit = 100  # Use a larger page size for efficiency

        while True:
            params = {'page': page, 'limit': page_limit}
            response = self._make_request('/manage/admin/users', params=params)

            items = response.get('items', [])
            all_items.extend(items)

            # If we got fewer items than the limit, we've reached the last page
            if len(items) < page_limit:
                break

            page += 1

        return {
            'count': len(all_items),
            'items': all_items
        }

    def get_courses(self, page: Optional[int] = None, limit: Optional[int] = None) -> dict:
        """
        Get all courses from /manage/admin/courses.

        Automatically fetches all pages if page/limit are not provided.

        Args:
            page: Optional specific page to fetch (1-indexed)
            limit: Optional number of items per page

        Returns:
            Dict with 'count' and 'items' keys containing course data.

        Note:
            Pagination logic is implemented to fetch all pages automatically.

        Example response structure:
            {
                "count": 10,
                "items": [
                    {
                        "id": 1,
                        "name": "Course Name",
                        "description": "Course description",
                        ...
                    },
                    ...
                ]
            }
        """
        # If specific page/limit requested, return single page
        if page is not None or limit is not None:
            params = {}
            if page is not None:
                params['page'] = page
            if limit is not None:
                params['limit'] = limit
            response = self._make_request('/manage/admin/courses', params=params)
            # Return actual count based on items fetched, not API's count field
            return {
                'count': len(response.get('items', [])),
                'items': response.get('items', [])
            }

        # Otherwise, fetch all pages automatically
        all_items = []
        page = 1
        page_limit = 100  # Use a larger page size for efficiency

        while True:
            params = {'page': page, 'limit': page_limit}
            response = self._make_request('/manage/admin/courses', params=params)

            items = response.get('items', [])
            all_items.extend(items)

            # If we got fewer items than the limit, we've reached the last page
            if len(items) < page_limit:
                break

            page += 1

        return {
            'count': len(all_items),
            'items': all_items
        }

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

    def get_ai_conversations(self, page: Optional[int] = None, limit: Optional[int] = None) -> dict:
        """
        Get all AI submission conversations from /ai/api/ext/submission-conversations.

        Automatically fetches all pages if page/limit are not provided.

        Args:
            page: Optional specific page to fetch (1-indexed)
            limit: Optional number of items per page

        Returns:
            Dict with 'count' and 'items' keys containing conversation data.

        Note:
            This endpoint uses 'data' array and 'meta' for pagination info,
            unlike other endpoints that use 'items' and 'count'.

        Example response structure:
            {
                "code": 200,
                "message": "Success",
                "data": [
                    {
                        "id": 770,
                        "conversation_id": "LURYy5hU-xi1WAFbvsCjf",
                        "user_id": "258f4916-cde6-4739-84c1-9dc86ca975ad",
                        "enrollment_id": "0UyFNKQK1L17ka37bEZi",
                        "course_id": 10,
                        "material_id": 190,
                        "status": "WAITING",
                        "score": 0,
                        "user": {...},
                        "educator": {...},
                        ...
                    }
                ],
                "meta": {
                    "total_pages": 1,
                    "total_count": 10,
                    "page": 1,
                    "limit": 10
                }
            }
        """
        endpoint = '/ai/api/ext/submission-conversations'

        # If specific page/limit requested, return single page
        if page is not None or limit is not None:
            params = {}
            if page is not None:
                params['page'] = page
            if limit is not None:
                params['limit'] = limit
            response = self._make_request(endpoint, params=params)
            data = response.get('data', [])
            return {
                'count': len(data),
                'items': data,
                'meta': response.get('meta', {})
            }

        # Otherwise, fetch all pages automatically
        all_items = []
        page = 1
        page_limit = 100  # Use a larger page size for efficiency

        while True:
            params = {'page': page, 'limit': page_limit}
            response = self._make_request(endpoint, params=params)

            data = response.get('data', [])
            all_items.extend(data)

            # Check meta for total pages
            meta = response.get('meta', {})
            total_pages = meta.get('total_pages', 1)

            # If we've fetched all pages, stop
            if page >= total_pages:
                break

            page += 1

        return {
            'count': len(all_items),
            'items': all_items
        }

    def get_ai_conversation_messages(self, conversation_id: str) -> dict:
        """
        Get messages for a specific AI conversation.

        Args:
            conversation_id: The conversation_id (session_id) to fetch messages for

        Returns:
            Dict with 'messages' key containing list of message objects.

        Example response structure:
            {
                "code": 200,
                "message": "Success",
                "data": [
                    {
                        "session_id": "LURYy5hU-xi1WAFbvsCjf",
                        "sender": "ai",
                        "value": "Hello...",
                        "timestamp": "2025-09-30T03:29:12Z"
                    },
                    ...
                ]
            }
        """
        endpoint = f'/ai/api/ext/submission-conversations/messages/{conversation_id}'
        response = self._make_request(endpoint)
        return {
            'messages': response.get('data', [])
        }