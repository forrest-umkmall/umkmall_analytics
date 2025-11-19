"""
Example data ingestion script.
Replace this with your actual data sources.
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db import get_db_connection

logger = logging.getLogger(__name__)


def ingest_example_data():
    """
    Example function to ingest data from a source.
    Replace this with your actual ingestion logic.
    """
    logger.info("Starting example data ingestion")

    # Get database connection
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create raw schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        # Example: Create a table and insert some data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.example_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                value NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Example: Insert sample data
        # In a real scenario, you would fetch data from an API or other source
        sample_data = [
            ("Example 1", 100),
            ("Example 2", 200),
            ("Example 3", 300),
        ]

        for name, value in sample_data:
            cursor.execute("""
                INSERT INTO raw.example_table (name, value)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (name, value))

        conn.commit()
        logger.info(f"Successfully ingested {len(sample_data)} records")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error ingesting data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    ingest_example_data()
