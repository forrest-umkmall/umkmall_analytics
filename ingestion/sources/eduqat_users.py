"""
Ingest user data from Eduqat API.

This script pulls user data from the Eduqat public API.
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
SOURCE_NAME = 'eduqat_users'


def ingest_eduqat_users():
    """
    Ingest user data from Eduqat API.

    Fetches all users and stores them in raw.eduqat_users table.
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

        response = client.get_users()
        users = response.get('items', [])
        count = response.get('count', len(users))

        logger.info(f"Extracted {count} users from Eduqat API")

        if not users:
            logger.warning("No users extracted, skipping")
            return

        # Load to analytics database (raw schema)
        logger.info("Loading data to analytics database")
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Create raw schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

            # Create table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{SOURCE_NAME} (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    subid VARCHAR(255),
                    user_name VARCHAR(255),
                    name VARCHAR(255),
                    email VARCHAR(255),
                    phone_number VARCHAR(50),
                    phone_country VARCHAR(50),
                    phone_country_calling_code VARCHAR(10),
                    description TEXT,
                    avatar_url TEXT,
                    role VARCHAR(50),
                    status VARCHAR(50),
                    total_course INTEGER,
                    total_enrollment INTEGER,
                    stripe_customer_ids JSONB,
                    metadata JSONB,
                    pre_signup_at TIMESTAMP,
                    confirmed_at TIMESTAMP,
                    last_loggin_at TIMESTAMP,
                    created_at TIMESTAMP,
                    -- Ingestion metadata
                    data_source VARCHAR(100),
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Truncate and reload (full refresh)
            cursor.execute(f"TRUNCATE TABLE raw.{SOURCE_NAME};")

            # Insert data
            for user in users:
                cursor.execute(f"""
                    INSERT INTO raw.{SOURCE_NAME} (
                        id,
                        user_id,
                        subid,
                        user_name,
                        name,
                        email,
                        phone_number,
                        phone_country,
                        phone_country_calling_code,
                        description,
                        avatar_url,
                        role,
                        status,
                        total_course,
                        total_enrollment,
                        stripe_customer_ids,
                        metadata,
                        pre_signup_at,
                        confirmed_at,
                        last_loggin_at,
                        created_at,
                        data_source,
                        extracted_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    );
                """, (
                    user.get('id'),
                    user.get('user_id'),
                    user.get('subid'),
                    user.get('user_name'),
                    user.get('name'),
                    user.get('email'),
                    user.get('phone_number'),
                    user.get('phone_country'),
                    user.get('phone_country_calling_code'),
                    user.get('description'),
                    user.get('avatar_url'),
                    user.get('role'),
                    user.get('status'),
                    user.get('total_course'),
                    user.get('total_enrollment'),
                    json.dumps(user.get('stripe_customer_ids')) if user.get('stripe_customer_ids') else None,
                    json.dumps(user.get('metadata')) if user.get('metadata') else None,
                    user.get('pre_signup_at'),
                    user.get('confirmed_at'),
                    user.get('last_loggin_at'),
                    user.get('created_at'),
                    SOURCE_NAME,
                    datetime.utcnow()
                ))

            conn.commit()
            logger.info(f"✓ Successfully loaded {len(users)} rows to raw.{SOURCE_NAME}")

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
    ingest_eduqat_users()