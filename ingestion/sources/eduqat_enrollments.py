"""
Ingest enrollment data from Eduqat API.

This script pulls enrollment data from the Eduqat public API,
storing nested JSON structures (completions, metadata, user_data, certificates)
as JSONB columns for later extraction.
"""

import os
import sys
import json
from pathlib import Path
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.lib.eduqat_client import EduqatClient, EduqatApiError
from ingestion.utils.db import get_db_connection

logger = logging.getLogger(__name__)

# Source configuration
SOURCE_NAME = 'eduqat_enrollments'


def ingest_eduqat_enrollments():
    """
    Ingest enrollment data from Eduqat API.

    Fetches all enrollments and stores them in raw.eduqat_enrollments table
    with nested JSON preserved as JSONB columns.
    """
    logger.info(f"Starting ingestion for {SOURCE_NAME}")

    # Check for API key
    api_key = os.getenv('EDUQAT_API_KEY')
    if not api_key:
        logger.warning(f"EDUQAT_API_KEY not set, skipping {SOURCE_NAME}")
        return

    try:
        # Fetch data from Eduqat API
        client = EduqatClient()
        logger.info("Connected to Eduqat API")

        response = client.get_enrollments()
        enrollments = response.get('items', [])
        count = response.get('count', len(enrollments))

        logger.info(f"Extracted {count} enrollments from Eduqat API")

        if not enrollments:
            logger.warning("No enrollments extracted, skipping")
            return

        # Load to analytics database (raw schema)
        logger.info("Loading data to analytics database")
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Create raw schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

            # Create table with JSONB columns for nested data
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{SOURCE_NAME} (
                    id VARCHAR(255) PRIMARY KEY,
                    uid VARCHAR(255),
                    user_id VARCHAR(255),
                    course_id INTEGER,
                    price_id INTEGER,
                    schedule_id INTEGER,
                    order_uid VARCHAR(255),
                    order_data BOOLEAN,
                    timezone VARCHAR(100),
                    learning_progress FLOAT,
                    learning_time INTEGER,
                    completed_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP,
                    -- Nested JSON stored as JSONB
                    user_data JSONB,
                    metadata JSONB,
                    completions JSONB,
                    certificates JSONB,
                    user_groups JSONB,
                    user_group_admins JSONB,
                    -- Metadata
                    data_source VARCHAR(100),
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Truncate and reload (full refresh)
            cursor.execute(f"TRUNCATE TABLE raw.{SOURCE_NAME};")

            # Insert data
            for enrollment in enrollments:
                cursor.execute(f"""
                    INSERT INTO raw.{SOURCE_NAME} (
                        id,
                        uid,
                        user_id,
                        course_id,
                        price_id,
                        schedule_id,
                        order_uid,
                        order_data,
                        timezone,
                        learning_progress,
                        learning_time,
                        completed_at,
                        expires_at,
                        created_at,
                        user_data,
                        metadata,
                        completions,
                        certificates,
                        user_groups,
                        user_group_admins,
                        data_source,
                        extracted_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s
                    );
                """, (
                    enrollment.get('id'),
                    enrollment.get('uid'),
                    enrollment.get('user_id'),
                    enrollment.get('course_id'),
                    enrollment.get('price_id'),
                    enrollment.get('schedule_id'),
                    enrollment.get('order_uid'),
                    enrollment.get('order_data'),
                    enrollment.get('timezone'),
                    enrollment.get('learning_progress'),
                    enrollment.get('learning_time'),
                    enrollment.get('completed_at'),
                    enrollment.get('expires_at'),
                    enrollment.get('created_at'),
                    json.dumps(enrollment.get('user_data')) if enrollment.get('user_data') else None,
                    json.dumps(enrollment.get('metadata')) if enrollment.get('metadata') else None,
                    json.dumps(enrollment.get('completions')) if enrollment.get('completions') else None,
                    json.dumps(enrollment.get('certificates')) if enrollment.get('certificates') else None,
                    json.dumps(enrollment.get('user_groups')) if enrollment.get('user_groups') else None,
                    json.dumps(enrollment.get('user_group_admins')) if enrollment.get('user_group_admins') else None,
                    SOURCE_NAME,
                    datetime.utcnow()
                ))

            conn.commit()
            logger.info(f"✓ Successfully loaded {len(enrollments)} rows to raw.{SOURCE_NAME}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading data: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    except EduqatApiError as e:
        logger.error(f"Eduqat API error: {e.message} (status: {e.status_code})")
        raise
    except Exception as e:
        logger.error(f"Error ingesting {SOURCE_NAME}: {str(e)}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_eduqat_enrollments()