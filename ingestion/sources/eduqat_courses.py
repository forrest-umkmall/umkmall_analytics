"""
Ingest course data from Eduqat API.

This script pulls course data from the Eduqat public API.
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
SOURCE_NAME = 'eduqat_courses'


def ingest_eduqat_courses():
    """
    Ingest course data from Eduqat API.

    Fetches all courses and stores them in raw.eduqat_courses table.
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

        response = client.get_courses()
        courses = response.get('items', [])
        count = response.get('count', len(courses))

        logger.info(f"Extracted {count} courses from Eduqat API")

        if not courses:
            logger.warning("No courses extracted, skipping")
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
                    name VARCHAR(500),
                    slug VARCHAR(500),
                    description TEXT,
                    type VARCHAR(50),
                    status VARCHAR(50),
                    author VARCHAR(255),
                    duration INTEGER,
                    language_codes JSONB,
                    categories JSONB,
                    educators JSONB,
                    images JSONB,
                    prices JSONB,
                    tags JSONB,
                    metadata JSONB,
                    parent INTEGER,
                    timezone VARCHAR(100),
                    total_student INTEGER,
                    rating NUMERIC,
                    progress_status VARCHAR(50),
                    start_date TIMESTAMP,
                    end_date TIMESTAMP,
                    published_at TIMESTAMP,
                    -- Ingestion metadata
                    data_source VARCHAR(100),
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Truncate and reload (full refresh)
            cursor.execute(f"TRUNCATE TABLE raw.{SOURCE_NAME};")

            # Insert data
            for course in courses:
                cursor.execute(f"""
                    INSERT INTO raw.{SOURCE_NAME} (
                        id,
                        name,
                        slug,
                        description,
                        type,
                        status,
                        author,
                        duration,
                        language_codes,
                        categories,
                        educators,
                        images,
                        prices,
                        tags,
                        metadata,
                        parent,
                        timezone,
                        total_student,
                        rating,
                        progress_status,
                        start_date,
                        end_date,
                        published_at,
                        data_source,
                        extracted_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    );
                """, (
                    course.get('id'),
                    course.get('name'),
                    course.get('slug'),
                    course.get('description'),
                    course.get('type'),
                    course.get('status'),
                    course.get('author'),
                    course.get('duration'),
                    json.dumps(course.get('language_codes')) if course.get('language_codes') else None,
                    json.dumps(course.get('categories')) if course.get('categories') else None,
                    json.dumps(course.get('educators')) if course.get('educators') else None,
                    json.dumps(course.get('images')) if course.get('images') else None,
                    json.dumps(course.get('prices')) if course.get('prices') else None,
                    json.dumps(course.get('tags')) if course.get('tags') else None,
                    json.dumps(course.get('metadata')) if course.get('metadata') else None,
                    course.get('parent'),
                    course.get('timezone'),
                    course.get('total_student'),
                    course.get('rating'),
                    course.get('progress_status'),
                    course.get('start_date'),
                    course.get('end_date'),
                    course.get('published_at'),
                    SOURCE_NAME,
                    datetime.utcnow()
                ))

            conn.commit()
            logger.info(f"✓ Successfully loaded {len(courses)} rows to raw.{SOURCE_NAME}")

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
    ingest_eduqat_courses()
