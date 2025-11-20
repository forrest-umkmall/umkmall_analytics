"""
Database connection utilities.
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_db_connection():
    """
    Create and return a PostgreSQL database connection.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB", "analytics")
        )
        return conn
    except Exception as e:
        raise Exception(f"Failed to connect to database: {str(e)}")


def execute_query(query, params=None, fetch=False):
    """
    Execute a SQL query and optionally fetch results.

    Args:
        query (str): SQL query to execute
        params (tuple): Query parameters
        fetch (bool): Whether to fetch and return results

    Returns:
        list: Query results if fetch=True, otherwise None
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query, params)
        conn.commit()

        if fetch:
            results = cursor.fetchall()
            return results

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
