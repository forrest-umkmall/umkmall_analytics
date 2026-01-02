"""
Ingest purchase form data from Supabase (Postgres).

This script pulls customer purchase data from the product database.
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
SOURCE_NAME = 'purchase_form_data'
SOURCE_DB_URL = os.getenv('PRODUCT_DB_URL')


def ingest_purchase_data():
    """
    Ingest purchase form data from the product database.
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
                    customer_first_name,
                    customer_last_name,
                    customer_email,
                    customer_phone,
                    created_at,
                    paid_at,
                    product_type,
                    course_id,
                    amount,
                    payment_method,
                    payment_channel
                FROM payments
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
            # This handles NaT -> None conversion properly
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

                # Create table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS raw.{SOURCE_NAME} (
                        customer_first_name VARCHAR(255),
                        customer_last_name VARCHAR(255),
                        email VARCHAR(255),
                        phone_number VARCHAR(50),
                        created_at TIMESTAMP,
                        paid_at TIMESTAMP,
                        product_type VARCHAR(100),
                        course_id VARCHAR(255),
                        amount NUMERIC,
                        payment_method VARCHAR(100),
                        payment_channel VARCHAR(100),
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
                            customer_first_name,
                            customer_last_name,
                            email,
                            phone_number,
                            created_at,
                            paid_at,
                            product_type,
                            course_id,
                            amount,
                            payment_method,
                            payment_channel,
                            data_source,
                            extracted_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        row.get('customer_first_name'),
                        row.get('customer_last_name'),
                        row.get('email'),
                        row.get('phone_number'),
                        row.get('created_at'),
                        row.get('paid_at'),
                        row.get('product_type'),
                        row.get('course_id'),
                        row.get('amount'),
                        row.get('payment_method'),
                        row.get('payment_channel'),
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
    ingest_purchase_data()
