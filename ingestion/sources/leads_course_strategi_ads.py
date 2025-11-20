"""
Ingest leads data from Course Strategy Ads Google Sheets.
"""

import os
import sys
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.lib.gsheets_client import GSheetsClient
from ingestion.lib.column_mappings import apply_column_mapping, normalize_dataframe
from ingestion.utils.db import get_db_connection

logger = logging.getLogger(__name__)

SOURCE_NAME = 'leads_course_strategi_ads'
SPREADSHEET_ID = '1RGEkNKiFA3k_CpVvu311_0V2C9TwRY35svEFG8jqllE'


def ingest_leads_course_strategi_ads():
    """Ingest leads from Course Strategy Ads Google Sheets."""
    logger.info(f"Starting ingestion for {SOURCE_NAME}")

    try:
        gsheets = GSheetsClient()
        sheets_dict = gsheets.read_all_sheets_to_dict(SPREADSHEET_ID)

        if not sheets_dict:
            logger.warning("No sheets found")
            return

        all_data = []
        for sheet_name, df in sheets_dict.items():
            if not df.empty:
                df['sheet_name'] = sheet_name
                all_data.append(df)

        if not all_data:
            logger.warning("No data found")
            return

        df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Extracted {len(df)} rows")

        df = apply_column_mapping(df)
        df = normalize_dataframe(df)
        df['data_source'] = SOURCE_NAME
        df['extracted_at'] = datetime.utcnow()

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
    ingest_leads_course_strategi_ads()
