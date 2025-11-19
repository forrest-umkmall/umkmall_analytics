"""
PostgreSQL client for data extraction with connection pooling.

This module provides a client for reading data from PostgreSQL databases,
with support for connection pooling and both table-based and query-based extraction.
"""

import os
import pandas as pd
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
import warnings


class PostgresClient:
    """Client for interacting with PostgreSQL databases."""

    def __init__(self, connection_string: str, min_connections: int = 1, max_connections: int = 5):
        """
        Initialize the PostgreSQL client with connection pooling.

        Args:
            connection_string: PostgreSQL connection string (e.g., postgresql://user:pass@host:5432/dbname)
            min_connections: Minimum number of connections in the pool
            max_connections: Maximum number of connections in the pool
        """
        self.connection_string = self._resolve_env_var(connection_string)

        try:
            # Create psycopg2 connection pool for general use
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                min_connections,
                max_connections,
                self.connection_string
            )

            # Create SQLAlchemy engine for pandas compatibility
            self.engine = create_engine(self.connection_string, pool_pre_ping=True)

            print(f"✓ PostgreSQL connection pool initialized ({min_connections}-{max_connections} connections)")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize PostgreSQL connection pool: {e}")

    @staticmethod
    def _resolve_env_var(value: str) -> str:
        """
        Resolve environment variable references in a string.

        Supports formats:
        - ${VAR_NAME}
        - $VAR_NAME
        - VAR_NAME (if it matches an existing env var)

        Args:
            value: String that may contain environment variable references

        Returns:
            Resolved string with environment variables expanded
        """
        if not value:
            return value

        # Check if the entire value is an env var name (no $ prefix)
        if value in os.environ:
            return os.environ[value]

        # Handle ${VAR_NAME} format
        if value.startswith('${') and value.endswith('}'):
            var_name = value[2:-1]
            if var_name in os.environ:
                return os.environ[var_name]
            raise ValueError(f"Environment variable '{var_name}' not found")

        # Handle $VAR_NAME format
        if value.startswith('$'):
            var_name = value[1:]
            if var_name in os.environ:
                return os.environ[var_name]
            raise ValueError(f"Environment variable '{var_name}' not found")

        # Return as-is if no env var pattern detected
        return value

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool.

        Yields:
            psycopg2 connection object
        """
        connection = self.connection_pool.getconn()
        try:
            yield connection
        finally:
            self.connection_pool.putconn(connection)

    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Args:
            query: SQL query to execute
            params: Optional tuple of parameters for parameterized queries

        Returns:
            DataFrame with query results
        """
        try:
            # Use SQLAlchemy engine to avoid pandas warning
            df = pd.read_sql_query(query, self.engine, params=params)
            return df
        except Exception as e:
            raise Exception(f"Error executing query: {e}\nQuery: {query}")

    def read_table(self, table_name: str, schema: Optional[str] = None,
                   columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Read an entire table into a DataFrame.

        Args:
            table_name: Name of the table to read
            schema: Optional schema name (defaults to 'public')
            columns: Optional list of specific columns to read (reads all if None)

        Returns:
            DataFrame with table data
        """
        # Build the qualified table name
        if schema:
            full_table_name = f'"{schema}"."{table_name}"'
        else:
            full_table_name = f'"{table_name}"'

        # Build column selection
        column_selection = ', '.join(f'"{col}"' for col in columns) if columns else '*'

        query = f"SELECT {column_selection} FROM {full_table_name}"

        print(f"Reading table: {full_table_name}")
        df = self.execute_query(query)
        print(f"✓ Loaded {len(df)} rows, {len(df.columns)} columns")

        return df

    def read_tables(self, table_names: List[str], schema: Optional[str] = None,
                   add_table_name_column: bool = True) -> pd.DataFrame:
        """
        Read multiple tables and concatenate them into a single DataFrame.

        Args:
            table_names: List of table names to read
            schema: Optional schema name (defaults to 'public')
            add_table_name_column: Whether to add a 'table_name' column

        Returns:
            Concatenated DataFrame with data from all tables
        """
        dataframes = []

        for table_name in table_names:
            df = self.read_table(table_name, schema=schema)
            if add_table_name_column:
                df['table_name'] = table_name
            dataframes.append(df)

        if not dataframes:
            return pd.DataFrame()

        # Concatenate with all columns (union of columns across all tables)
        merged_df = pd.concat(dataframes, ignore_index=True)
        print(f"✓ Merged {len(table_names)} tables: {len(merged_df)} total rows")

        return merged_df

    def list_tables(self, schema: str = 'public') -> List[str]:
        """
        List all tables in a schema.

        Args:
            schema: Schema name (defaults to 'public')

        Returns:
            List of table names
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        df = self.execute_query(query, params=(schema,))
        return df['table_name'].tolist()

    def get_table_info(self, table_name: str, schema: str = 'public') -> pd.DataFrame:
        """
        Get column information for a table.

        Args:
            table_name: Name of the table
            schema: Schema name (defaults to 'public')

        Returns:
            DataFrame with column information (name, type, nullable, etc.)
        """
        query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            ORDER BY ordinal_position
        """
        return self.execute_query(query, params=(schema, table_name))

    def close(self):
        """Close all connections in the pool."""
        if hasattr(self, 'connection_pool') and self.connection_pool:
            if not self.connection_pool.closed:
                self.connection_pool.closeall()

        if hasattr(self, 'engine') and self.engine:
            self.engine.dispose()

        print("✓ PostgreSQL connection pool closed")

    def __del__(self):
        """Cleanup on deletion."""
        self.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
