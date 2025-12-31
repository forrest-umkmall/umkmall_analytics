"""
Ingest survey results from Eduqat API.

This script queries the already-ingested eduqat_enrollments table for completed surveys,
then fetches detailed survey results from the Eduqat API for each one.

Strategy:
1. Query raw.eduqat_enrollments for completions where type='survey' AND survey_type='survey'
2. For each (enrollment_id, material_id) pair, fetch survey results from API
3. Store both the survey metadata and the full elements array (questions + responses)

Endpoint: /manage/v2/admin/enrollments/{enrollment_id}/survey/{material_id}
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
SOURCE_NAME = 'eduqat_survey_results'


def get_survey_completions_from_db():
    """
    Query raw.eduqat_enrollments for completed surveys.

    Returns list of tuples: (enrollment_id, material_id, survey_id, completed_at, course_id, user_id)
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Extract enrollment_id + material_id pairs where type='survey' and survey_type='survey'
        # Note: completions has a nested 'materials' object keyed by material_id
        cursor.execute("""
            SELECT
                e.id as enrollment_id,
                kv.key as material_id,
                kv.value->>'survey_id' as survey_id,
                kv.value->>'completed_at' as completed_at,
                e.course_id,
                e.user_id
            FROM raw.eduqat_enrollments e,
                 jsonb_each(e.completions->'materials') as kv
            WHERE kv.value->>'type' = 'survey'
              AND kv.value->>'survey_type' = 'survey'
              AND kv.value->>'completed_at' IS NOT NULL
        """)
        results = cursor.fetchall()
        logger.info(f"Found {len(results)} completed surveys in enrollments")
        return results
    finally:
        cursor.close()
        conn.close()


def ingest_eduqat_survey_results():
    """
    Ingest survey results from Eduqat API.

    Fetches survey results for all completed surveys found in enrollments,
    storing them in raw.eduqat_survey_results table.
    """
    logger.info(f"Starting ingestion for {SOURCE_NAME}")

    # Check for API key
    api_key = os.getenv('EDUQAT_API_KEY')
    if not api_key:
        logger.warning(f"EDUQAT_API_KEY not set, skipping {SOURCE_NAME}")
        return

    try:
        # Get completed surveys from enrollments table
        survey_completions = get_survey_completions_from_db()

        if not survey_completions:
            logger.warning("No completed surveys found, skipping")
            return

        # Fetch survey results from API
        client = EduqatClient()
        logger.info("Connected to Eduqat API")

        survey_results = []
        failed_count = 0

        for i, (enrollment_id, material_id, survey_id, completed_at, course_id, user_id) in enumerate(survey_completions):
            try:
                # Fetch survey results from API
                endpoint = f'/manage/v2/admin/enrollments/{enrollment_id}/survey/{material_id}'
                result = client.get(endpoint)

                survey_results.append({
                    'enrollment_id': enrollment_id,
                    'material_id': material_id,
                    'survey_id': survey_id,
                    'completed_at': completed_at,
                    'course_id': course_id,
                    'user_id': user_id,
                    'survey_data': result
                })

                logger.debug(f"Fetched survey results for enrollment {enrollment_id}, material {material_id}")

            except EduqatApiError as e:
                failed_count += 1
                logger.warning(f"Failed to fetch survey {material_id} for enrollment {enrollment_id}: {e.message}")
            except Exception as e:
                failed_count += 1
                logger.warning(f"Unexpected error fetching survey {material_id} for enrollment {enrollment_id}: {str(e)}")

            # Log progress every 25 surveys
            if (i + 1) % 25 == 0:
                logger.info(f"Fetched {i + 1}/{len(survey_completions)} survey results")

        logger.info(f"Extracted {len(survey_results)} survey results ({failed_count} failed)")

        if not survey_results:
            logger.warning("No survey results fetched successfully, skipping")
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
                    -- Composite primary key
                    enrollment_id VARCHAR(255),
                    material_id VARCHAR(255),

                    -- Foreign keys / context
                    survey_id VARCHAR(255),
                    course_id INTEGER,
                    user_id VARCHAR(255),

                    -- Survey metadata
                    survey_type VARCHAR(50),
                    survey_title TEXT,

                    -- Full survey response data as JSONB
                    elements JSONB,

                    -- Completion info
                    completed_at TIMESTAMP,

                    -- Metadata
                    data_source VARCHAR(100),
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    PRIMARY KEY (enrollment_id, material_id)
                );
            """)

            # Truncate and reload (full refresh)
            cursor.execute(f"TRUNCATE TABLE raw.{SOURCE_NAME};")

            # Insert data
            for result in survey_results:
                survey_data = result['survey_data']

                cursor.execute(f"""
                    INSERT INTO raw.{SOURCE_NAME} (
                        enrollment_id,
                        material_id,
                        survey_id,
                        course_id,
                        user_id,
                        survey_type,
                        survey_title,
                        elements,
                        completed_at,
                        data_source,
                        extracted_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    );
                """, (
                    result['enrollment_id'],
                    result['material_id'],
                    result.get('survey_id') or survey_data.get('id'),
                    result['course_id'],
                    result['user_id'],
                    survey_data.get('type'),
                    survey_data.get('title'),
                    json.dumps(survey_data.get('elements')) if survey_data.get('elements') else None,
                    result['completed_at'],
                    SOURCE_NAME,
                    datetime.utcnow()
                ))

            conn.commit()
            logger.info(f"Successfully loaded {len(survey_results)} rows to raw.{SOURCE_NAME}")

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
    ingest_eduqat_survey_results()
