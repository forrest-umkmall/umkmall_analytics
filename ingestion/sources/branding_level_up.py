"""
Ingest leads data from Branding Level Up Google Sheets.

Note: Only reads the 'All Data' sheet to avoid API rate limits.
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.lib.gsheets_client import GSheetsClient
from ingestion.lib.column_mappings import apply_column_mapping
from ingestion.utils.db import get_db_connection

logger = logging.getLogger(__name__)

SOURCE_NAME = 'branding_level_up'
SPREADSHEET_ID = '16p-addrDK67qIacUIEeSVQ06ZU1IuGa1phKtz19M5l4'
SHEET_NAME = 'All Data'  # Only load this specific sheet


def ingest_branding_level_up():
    """Ingest leads from Branding Level Up Google Sheets."""
    logger.info(f"Starting ingestion for {SOURCE_NAME}")

    try:
        gsheets = GSheetsClient()

        # Read only the specific sheet
        df = gsheets.read_sheet_to_dataframe(SPREADSHEET_ID, f"'{SHEET_NAME}'")

        if df.empty:
            logger.warning(f"No data found in sheet '{SHEET_NAME}'")
            return

        logger.info(f"Extracted {len(df)} rows from sheet '{SHEET_NAME}'")

        # Add sheet name as metadata
        df['sheet_name'] = SHEET_NAME

        df = apply_column_mapping(df)
        df['data_source'] = SOURCE_NAME
        df['extracted_at'] = datetime.utcnow()

        # Rename 'id' column if it exists to avoid conflict with SERIAL PRIMARY KEY
        if 'id' in df.columns:
            df = df.rename(columns={'id': 'source_id'})

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

            columns = list(df.columns)
            column_defs = [f'"{col}" TEXT' for col in columns]

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{SOURCE_NAME} (
                    id SERIAL PRIMARY KEY,
                    {', '.join(column_defs)},
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cursor.execute(f"TRUNCATE TABLE raw.{SOURCE_NAME} CASCADE;")

            for _, row in df.iterrows():
                col_names = ', '.join([f'"{col}"' for col in columns])
                placeholders = ', '.join(['%s'] * len(columns))
                values = [row.get(col) for col in columns]
                cursor.execute(f"INSERT INTO raw.{SOURCE_NAME} ({col_names}) VALUES ({placeholders});", values)

            conn.commit()
            logger.info(f"✓ Successfully loaded {len(df)} rows to raw.{SOURCE_NAME}")

        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Error ingesting {SOURCE_NAME}: {str(e)}")
        raise


if __name__ == "__main__":
    ingest_branding_level_up()
