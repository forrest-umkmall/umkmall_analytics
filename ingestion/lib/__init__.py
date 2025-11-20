"""
Client libraries and utilities for data ingestion.
"""

from .postgres_client import PostgresClient
from .gsheets_client import GSheetsClient
from .data_cleaning import (
    normalize_phone_number,
    normalize_email,
    create_composite_key,
    deduplicate_by_composite_key,
)

__all__ = [
    "PostgresClient",
    "GSheetsClient",
    "normalize_phone_number",
    "normalize_email",
    "create_composite_key",
    "deduplicate_by_composite_key",
]
