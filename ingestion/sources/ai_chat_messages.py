"""
Ingest AI chat messages data from Supabase (Postgres).

This script pulls AI chat message data from the product database.
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
SOURCE_NAME = 'ai_chat_messages'
SOURCE_DB_URL = os.getenv('PRODUCT_DB_URL')


def ingest_ai_chat_messages():
    """
    Ingest AI chat messages data from the product database.
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
                    session_id,
                    role,
                    content,
                    message_order,
                    created_at
                FROM ai_chat_messages
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
                if pd.isna(val):
                    return None
                if isinstance(val, (pd.Timestamp, np.datetime64)):
                    return pd.Timestamp(val).to_pydatetime()
                if isinstance(val, np.integer):
                    return int(val)
                if isinstance(val, np.floating):
                    return float(val)
                return val

            records = df.to_dict('records')
            for record in records:
                for key, val in record.items():
                    record[key] = convert_value(val)

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
                        session_id TEXT,
                        role TEXT,
                        content TEXT,
                        message_order INTEGER,
                        created_at TIMESTAMP,
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
                            session_id,
                            role,
                            content,
                            message_order,
                            created_at,
                            data_source,
                            extracted_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        row.get('id'),
                        row.get('session_id'),
                        row.get('role'),
                        row.get('content'),
                        row.get('message_order'),
                        row.get('created_at'),
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
    ingest_ai_chat_messages()
