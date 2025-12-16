"""
Ingest AI conversation data from Eduqat API.

This script pulls AI tutor conversation data from the Eduqat API,
fetching both conversation metadata and individual messages,
storing messages as a JSONB array for later extraction in dbt.

Endpoints used:
- /ai/api/ext/submission-conversations (list all conversations, paginated)
- /ai/api/ext/submission-conversations/messages/{conversation_id} (get messages)
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
SOURCE_NAME = 'eduqat_ai_conversations'


def ingest_eduqat_ai_conversations():
    """
    Ingest AI conversation data from Eduqat API.

    Fetches all AI conversations and their messages, storing them in
    raw.eduqat_ai_conversations table with messages as JSONB array.
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

        # Get all conversations (paginated)
        response = client.get_ai_conversations()
        conversations = response.get('items', [])
        count = response.get('count', len(conversations))

        logger.info(f"Extracted {count} AI conversations from Eduqat API")

        if not conversations:
            logger.warning("No AI conversations extracted, skipping")
            return

        # Fetch messages for each conversation
        logger.info("Fetching messages for each conversation...")
        for i, conv in enumerate(conversations):
            conversation_id = conv.get('conversation_id')
            if conversation_id:
                try:
                    messages_response = client.get_ai_conversation_messages(conversation_id)
                    conv['messages'] = messages_response.get('messages', [])
                    logger.debug(f"Fetched {len(conv['messages'])} messages for conversation {conversation_id}")
                except EduqatApiError as e:
                    logger.warning(f"Failed to fetch messages for {conversation_id}: {e.message}")
                    conv['messages'] = []
            else:
                conv['messages'] = []

            # Log progress every 50 conversations
            if (i + 1) % 50 == 0:
                logger.info(f"Fetched messages for {i + 1}/{count} conversations")

        logger.info(f"Completed fetching messages for all {count} conversations")

        # Load to analytics database (raw schema)
        logger.info("Loading data to analytics database")
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Create raw schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

            # Create table with JSONB columns for nested data
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{SOURCE_NAME} (
                    id INTEGER PRIMARY KEY,
                    conversation_id VARCHAR(255) UNIQUE,
                    user_id VARCHAR(255),
                    enrollment_id VARCHAR(255),
                    course_id INTEGER,
                    material_id INTEGER,
                    status VARCHAR(50),
                    score INTEGER,
                    content TEXT,
                    audio_url TEXT,
                    x_site_id VARCHAR(100),
                    educator_id VARCHAR(255),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    -- Nested JSON stored as JSONB
                    user_data JSONB,
                    educator_data JSONB,
                    messages JSONB,
                    -- Metadata
                    data_source VARCHAR(100),
                    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Truncate and reload (full refresh)
            cursor.execute(f"TRUNCATE TABLE raw.{SOURCE_NAME};")

            # Insert data
            for conv in conversations:
                cursor.execute(f"""
                    INSERT INTO raw.{SOURCE_NAME} (
                        id,
                        conversation_id,
                        user_id,
                        enrollment_id,
                        course_id,
                        material_id,
                        status,
                        score,
                        content,
                        audio_url,
                        x_site_id,
                        educator_id,
                        created_at,
                        updated_at,
                        user_data,
                        educator_data,
                        messages,
                        data_source,
                        extracted_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    );
                """, (
                    conv.get('id'),
                    conv.get('conversation_id'),
                    conv.get('user_id'),
                    conv.get('enrollment_id'),
                    conv.get('course_id'),
                    conv.get('material_id'),
                    conv.get('status'),
                    conv.get('score'),
                    conv.get('content'),
                    conv.get('audio_url'),
                    conv.get('x_site_id'),
                    conv.get('educator_id'),
                    conv.get('created_at'),
                    conv.get('updated_at'),
                    json.dumps(conv.get('user')) if conv.get('user') else None,
                    json.dumps(conv.get('educator')) if conv.get('educator') else None,
                    json.dumps(conv.get('messages')) if conv.get('messages') else None,
                    SOURCE_NAME,
                    datetime.utcnow()
                ))

            conn.commit()
            logger.info(f"Successfully loaded {len(conversations)} rows to raw.{SOURCE_NAME}")

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
    ingest_eduqat_ai_conversations()
