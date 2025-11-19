"""
Ingest leads data from Ads Community Google Sheets.

This script pulls lead registration data from the Ads Community spreadsheet.
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.gsheets_client import GSheetsClient
from lib.column_mappings import apply_column_mapping, normalize_dataframe
from utils.db import get_db_connection

logger = logging.getLogger(__name__)

# Source configuration
SOURCE_NAME = 'leads_ads_community'
SPREADSHEET_ID = '1pRUUvUEUZkUJijw_f1EJTc9j5szUSv8NsVUsWQAcm6Y'


def ingest_leads_ads_community():
    """
    Ingest leads from Ads Community Google Sheets.
    """
    logger.info(f"Starting ingestion for {SOURCE_NAME}")

    try:
        # Connect to Google Sheets
        gsheets = GSheetsClient()
        logger.info("Connected to Google Sheets")

        # Read all sheets from the spreadsheet
        sheets_dict = gsheets.read_all_sheets_to_dict(SPREADSHEET_ID)

        if not sheets_dict:
            logger.warning("No sheets found in spreadsheet")
            return

        # Combine all sheets into one DataFrame
        all_data = []
        for sheet_name, df in sheets_dict.items():
            if not df.empty:
                # Add sheet name as metadata
                df['sheet_name'] = sheet_name
                all_data.append(df)

        if not all_data:
            logger.warning("No data found in any sheet")
            return

        df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Extracted {len(df)} rows from {len(sheets_dict)} sheets")

        # Apply column mapping
        df = apply_column_mapping(df)

        # Normalize fields
        df = normalize_dataframe(df)

        # Add metadata
        df['data_source'] = SOURCE_NAME
        df['extracted_at'] = datetime.utcnow()

        # Load to analytics database (raw schema)
        logger.info("Loading data to analytics database")
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Create raw schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

            # Get all columns from the DataFrame
            columns = list(df.columns)

            # Create table with dynamic columns
            column_defs = []
            for col in columns:
                # Use TEXT for all columns to handle various data types
                column_defs.append(f'"{col}" TEXT')

            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS raw.{SOURCE_NAME} (
                    id SERIAL PRIMARY KEY,
                    {', '.join(column_defs)},
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(create_table_sql)

            # Truncate and reload (full refresh)
            cursor.execute(f"TRUNCATE TABLE raw.{SOURCE_NAME} CASCADE;")

            # Insert data
            for _, row in df.iterrows():
                # Build INSERT statement dynamically
                col_names = ', '.join([f'"{col}"' for col in columns])
                placeholders = ', '.join(['%s'] * len(columns))
                values = [row.get(col) for col in columns]

                insert_sql = f"""
                    INSERT INTO raw.{SOURCE_NAME} ({col_names})
                    VALUES ({placeholders});
                """
                cursor.execute(insert_sql, values)

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
    ingest_leads_ads_community()
