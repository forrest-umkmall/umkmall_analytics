"""
Ingest users data from Supabase (Postgres).

This script pulls user account data from the product database.
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.lib.postgres_client import PostgresClient
from ingestion.lib.column_mappings import apply_column_mapping
from ingestion.utils.db import get_db_connection

logger = logging.getLogger(__name__)

# Source configuration
SOURCE_NAME = 'users'
SOURCE_DB_URL = os.getenv('PRODUCT_DB_URL')


def ingest_users():
    """
    Ingest users data from the product database.
    """
    logger.info(f"Starting ingestion for {SOURCE_NAME}")

    if not SOURCE_DB_URL:
        logger.warning(f"PRODUCT_DB_URL not set, skipping {SOURCE_NAME}")
        return

    try:
        # Connect to source database (product DB)
        with PostgresClient(SOURCE_DB_URL) as source_client:
            logger.info("Connected to source database")

            # Execute the query from the source config
            query = """
                SELECT
                    id,
                    email,
                    created_at,
                    updated_at,
                    role,
                    eduqat_user_id,
                    name,
                    mobile,
                    mobile_verified,
                    mobile_verified_at,
                    avatar_url,
                    birth_date,
                    address,
                    city,
                    province,
                    postal_code,
                    language_preference,
                    ai_tone_preference,
                    interests,
                    level,
                    voice_preference,
                    latitude,
                    longitude,
                    segment,
                    business_name
                FROM users
            """

            df = source_client.execute_query(query)
            logger.info(f"Extracted {len(df)} rows from source")

            if df.empty:
                logger.warning("No data extracted, skipping")
                return

            # Apply column mapping
            df = apply_column_mapping(df)

            # Add metadata
            df['data_source'] = SOURCE_NAME
            df['extracted_at'] = datetime.utcnow()

            # Convert DataFrame to list of dicts with Python native types
            import pandas as pd
            import numpy as np

            def convert_value(val):
                """Convert pandas/numpy types to Python native types."""
                if isinstance(val, (list, dict)):
                    return val
                if pd.isna(val):
                    return None
                if isinstance(val, (pd.Timestamp, np.datetime64)):
                    return pd.Timestamp(val).to_pydatetime()
                if isinstance(val, np.integer):
                    return int(val)
                if isinstance(val, np.floating):
                    return float(val)
                if isinstance(val, np.bool_):
                    return bool(val)
                return val

            records = df.to_dict('records')
            for record in records:
                for key, val in record.items():
                    record[key] = convert_value(val)

            # Serialize interests (list/dict) to JSON string for JSONB column
            import json
            for record in records:
                if record.get('interests') is not None:
                    record['interests'] = json.dumps(record['interests'])

            # Load to analytics database (raw schema)
            logger.info("Loading data to analytics database")
            conn = get_db_connection()
            cursor = conn.cursor()

            try:
                # Create raw schema if it doesn't exist
                cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

                # Create table if it doesn't exist
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS raw.{SOURCE_NAME} (
                        id TEXT,
                        email VARCHAR(255),
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        role VARCHAR(50),
                        eduqat_user_id VARCHAR(255),
                        name VARCHAR(255),
                        mobile VARCHAR(50),
                        mobile_verified BOOLEAN,
                        mobile_verified_at TIMESTAMP,
                        avatar_url TEXT,
                        birth_date DATE,
                        address TEXT,
                        city VARCHAR(255),
                        province VARCHAR(255),
                        postal_code VARCHAR(20),
                        language_preference VARCHAR(10),
                        ai_tone_preference VARCHAR(50),
                        interests JSONB,
                        level VARCHAR(50),
                        voice_preference VARCHAR(50),
                        latitude DOUBLE PRECISION,
                        longitude DOUBLE PRECISION,
                        segment VARCHAR(100),
                        business_name VARCHAR(255),
                        data_source VARCHAR(100),
                        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                # Truncate and reload (full refresh)
                cursor.execute(f"TRUNCATE TABLE raw.{SOURCE_NAME};")

                # Insert data
                for row in records:
                    cursor.execute(f"""
                        INSERT INTO raw.{SOURCE_NAME} (
                            id,
                            email,
                            created_at,
                            updated_at,
                            role,
                            eduqat_user_id,
                            name,
                            mobile,
                            mobile_verified,
                            mobile_verified_at,
                            avatar_url,
                            birth_date,
                            address,
                            city,
                            province,
                            postal_code,
                            language_preference,
                            ai_tone_preference,
                            interests,
                            level,
                            voice_preference,
                            latitude,
                            longitude,
                            segment,
                            business_name,
                            data_source,
                            extracted_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        row.get('id'),
                        row.get('email'),
                        row.get('created_at'),
                        row.get('updated_at'),
                        row.get('role'),
                        row.get('eduqat_user_id'),
                        row.get('name'),
                        row.get('mobile'),
                        row.get('mobile_verified'),
                        row.get('mobile_verified_at'),
                        row.get('avatar_url'),
                        row.get('birth_date'),
                        row.get('address'),
                        row.get('city'),
                        row.get('province'),
                        row.get('postal_code'),
                        row.get('language_preference'),
                        row.get('ai_tone_preference'),
                        row.get('interests'),
                        row.get('level'),
                        row.get('voice_preference'),
                        row.get('latitude'),
                        row.get('longitude'),
                        row.get('segment'),
                        row.get('business_name'),
                        row.get('data_source'),
                        row.get('extracted_at')
                    ))

                conn.commit()
                logger.info(f"✓ Successfully loaded {len(df)} rows to raw.{SOURCE_NAME}")

            except Exception as e:
                conn.rollback()
                logger.error(f"Error loading data: {str(e)}")
                raise
            finally:
                cursor.close()
                conn.close()

    except Exception as e:
        logger.error(f"Error ingesting {SOURCE_NAME}: {str(e)}")
        raise


if __name__ == "__main__":
    ingest_users()
