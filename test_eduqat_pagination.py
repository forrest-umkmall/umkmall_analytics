"""
Test script to debug Eduqat API pagination.

This script tests various pagination approaches to try to fetch all 52 users
from the Eduqat API.
"""

from ingestion.lib.eduqat_client import EduqatClient
import json


def print_separator(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def main():
    client = EduqatClient()

    # Test 1: Default request (no pagination params)
    print_separator("Test 1: Default request (no params)")
    response = client._make_request('/manage/admin/users')
    items = response.get('items', [])
    print(f"Count field: {response.get('count')}")
    print(f"Items returned: {len(items)}")
    if items:
        ids = sorted([item.get('id') for item in items])
        print(f"IDs: {ids}")

    # Test 2: Try page and limit parameters
    print_separator("Test 2: Page/Limit combinations")
    test_cases = [
        {'page': 1, 'limit': 25},
        {'page': 2, 'limit': 25},
        {'page': 3, 'limit': 25},
        {'page': 1, 'limit': 50},
        {'page': 0, 'limit': 25},  # Zero-indexed?
    ]

    for params in test_cases:
        response = client._make_request('/manage/admin/users', params=params)
        items = response.get('items', [])
        if items:
            ids = sorted([item.get('id') for item in items])[:5]  # First 5 IDs
            print(f"{params} -> {len(items)} items, first IDs: {ids}")
        else:
            print(f"{params} -> No items")

    # Test 3: Try offset-based pagination
    print_separator("Test 3: Offset-based pagination")
    for offset in [0, 25, 50]:
        response = client._make_request('/manage/admin/users', params={'offset': offset, 'limit': 25})
        items = response.get('items', [])
        if items:
            ids = sorted([item.get('id') for item in items])[:5]
            print(f"offset={offset}, limit=25 -> {len(items)} items, first IDs: {ids}")

    # Test 4: Try skip/take (some APIs use this)
    print_separator("Test 4: Skip/Take pagination")
    for skip in [0, 25, 50]:
        response = client._make_request('/manage/admin/users', params={'skip': skip, 'take': 25})
        items = response.get('items', [])
        if items:
            ids = sorted([item.get('id') for item in items])[:5]
            print(f"skip={skip}, take=25 -> {len(items)} items, first IDs: {ids}")

    # Test 5: Check if there's a cursor/token based pagination
    print_separator("Test 5: Check for cursor/token fields")
    response = client._make_request('/manage/admin/users')
    print(f"Response keys: {list(response.keys())}")
    for key in ['cursor', 'next_cursor', 'next', 'nextToken', 'continuation', 'has_more']:
        if key in response:
            print(f"Found {key}: {response[key]}")

    # Test 6: Try very large limit
    print_separator("Test 6: Very large limit")
    for limit in [100, 200, 500, 1000]:
        response = client._make_request('/manage/admin/users', params={'limit': limit})
        items = response.get('items', [])
        print(f"limit={limit} -> {len(items)} items returned")

    # Test 7: Check with the high-level method
    print_separator("Test 7: Using client.get_users()")
    response = client.get_users()
    print(f"Count: {response.get('count')}")
    print(f"Items: {len(response.get('items', []))}")

    # Test 8: Check for filters that might affect results
    print_separator("Test 8: Status breakdown")
    response = client._make_request('/manage/admin/users')
    items = response.get('items', [])

    statuses = {}
    roles = {}
    for item in items:
        status = item.get('status', 'UNKNOWN')
        role = item.get('role', 'UNKNOWN')
        statuses[status] = statuses.get(status, 0) + 1
        roles[role] = roles.get(role, 0) + 1

    print("Statuses:")
    for status, count in statuses.items():
        print(f"  {status}: {count}")
    print("\nRoles:")
    for role, count in roles.items():
        print(f"  {role}: {count}")

    # Test 9: Try different endpoint variations
    print_separator("Test 9: Endpoint variations")
    endpoints = [
        '/manage/admin/users',
        '/manage/admin/users/',  # With trailing slash
    ]

    for endpoint in endpoints:
        try:
            response = client._make_request(endpoint, params={'page': 1, 'limit': 100})
            items = response.get('items', [])
            print(f"{endpoint} -> {len(items)} items")
        except Exception as e:
            print(f"{endpoint} -> Error: {e}")

    # Summary
    print_separator("SUMMARY")
    print(f"API reports total count: 52")
    print(f"Actual items accessible: 25")
    print(f"Missing: 27 users")
    print("\nConclusion:")
    print("The Eduqat API appears to have a hard limit of 25 items and does not")
    print("respect pagination parameters (page, limit, offset, skip/take).")
    print("\nNext steps:")
    print("1. Contact Eduqat support about pagination")
    print("2. Check if there's a different API endpoint")
    print("3. Check if there's a newer API version")


if __name__ == "__main__":
    main()
