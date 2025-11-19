"""
Staging layer transformations.

This module handles:
- Loading data from configured sources
- Column mapping and renaming
- Field-level filtering (include/exclude)
- Field normalization
- Adding metadata columns
"""

import pandas as pd
from typing import Dict
from src.config.sources import DataSourceConfig
from src.GSheets import GSheetsClient
from src.postgres_client import PostgresClient


def stage_source(config: DataSourceConfig) -> pd.DataFrame:
    """
    Load and stage a single data source with all configured transformations.

    This is the entry point for each data source. It:
    1. Loads raw data from the source
    2. Normalizes column names
    3. Applies column mapping
    4. Filters fields (include/exclude)
    5. Applies field normalizers
    6. Adds metadata columns

    Args:
        config: DataSourceConfig with all source settings

    Returns:
        Staged DataFrame ready for intermediate layer processing
    """
    print(f"\n{'='*60}")
    print(f"STAGING: {config.name}")
    print(f"Type: {config.source_type}")
    print(f"{'='*60}")

    # Step 1: Load raw data
    df = _load_raw_data(config)

    if df.empty:
        print(f"⚠️  No data loaded from {config.name}")
        return df

    print(f"✓ Loaded {len(df)} rows, {len(df.columns)} columns")

    # Step 2: Normalize column names (lowercase, replace spaces, remove special chars)
    df.columns = (df.columns
                  .str.lower()
                  .str.replace(' ', '_', regex=False)
                  .str.replace(r'[^a-z0-9_]', '_', regex=True)  # Replace special chars with underscore
                  .str.replace(r'_+', '_', regex=True)  # Replace multiple underscores with single
                  .str.strip('_'))  # Remove leading/trailing underscores

    # Step 3: Apply column mapping
    if config.column_mapping:
        df = df.rename(columns=config.column_mapping)
        print(f"✓ Applied column mapping")

    # Step 4: Filter fields
    if config.include_fields:
        # Only keep specified fields (that exist in df)
        fields_to_keep = [f for f in config.include_fields if f in df.columns]
        df = df[fields_to_keep]
        print(f"✓ Filtered to {len(fields_to_keep)} included fields")

    if config.exclude_fields:
        # Remove excluded fields
        fields_to_drop = [f for f in config.exclude_fields if f in df.columns]
        df = df.drop(columns=fields_to_drop)
        print(f"✓ Excluded {len(fields_to_drop)} fields")

    # Step 5: Apply field normalizers
    # Get standard normalizers if not set in config
    field_normalizers = config.field_normalizers
    if field_normalizers is None:
        from src.config.sources import get_standard_normalizers
        field_normalizers = get_standard_normalizers()

    if field_normalizers:
        for field_name, normalizer_fn in field_normalizers.items():
            if field_name in df.columns:
                df[field_name] = df[field_name].apply(normalizer_fn)
        print(f"✓ Applied {len(field_normalizers)} field normalizers")

    # Step 6: Add metadata columns
    if config.add_source_metadata:
        df['_source'] = config.name

    print(f"✓ Staged {len(df)} rows from {config.name}\n")

    return df


def _load_raw_data(config: DataSourceConfig) -> pd.DataFrame:
    """
    Load raw data from the configured source type.

    Args:
        config: DataSourceConfig

    Returns:
        Raw DataFrame (before any transformations)
    """
    if config.source_type == 'gsheet':
        return _load_gsheet(config)
    elif config.source_type == 'postgres':
        return _load_postgres(config)
    elif config.source_type == 'excel':
        return _load_excel(config)
    elif config.source_type == 'csv':
        return _load_csv(config)
    else:
        raise ValueError(f"Unsupported source type: {config.source_type}")


def _load_gsheet(config: DataSourceConfig) -> pd.DataFrame:
    """Load data from Google Sheets."""
    client = GSheetsClient()

    # GSheetsClient only supports exclude_sheets, not include sheets
    # So we need to load all and filter if sheet_names is specified
    sheets_dict = client.read_all_sheets_to_dict(
        config.source_id,
        exclude_sheets=config.exclude_sheets
    )

    if not sheets_dict:
        return pd.DataFrame()

    # Filter to specific sheets if requested
    if config.sheet_names:
        sheets_dict = {k: v for k, v in sheets_dict.items() if k in config.sheet_names}

    if not sheets_dict:
        return pd.DataFrame()

    # Stack all sheets
    all_sheets = []
    for sheet_name, sheet_df in sheets_dict.items():
        if config.add_sheet_metadata:
            sheet_df['_sheet_name'] = sheet_name
        all_sheets.append(sheet_df)

    df = pd.concat(all_sheets, ignore_index=True, sort=False)
    return df


def _load_postgres(config: DataSourceConfig) -> pd.DataFrame:
    """Load data from Postgres."""
    with PostgresClient(config.source_id) as pg_client:
        # Custom query takes precedence
        if config.query:
            df = pg_client.execute_query(config.query)

        # Specific tables
        elif config.sheet_names:  # Reusing sheet_names field for table names
            df = pg_client.read_tables(
                config.sheet_names,
                schema=config.schema or 'public',
                add_table_name_column=config.add_sheet_metadata
            )

        # All tables from schema
        else:
            schema = config.schema or 'public'
            tables = pg_client.list_tables(schema=schema)
            df = pg_client.read_tables(
                tables,
                schema=schema,
                add_table_name_column=config.add_sheet_metadata
            )

    return df


def _load_excel(config: DataSourceConfig) -> pd.DataFrame:
    """Load data from Excel file."""
    excel_file = pd.ExcelFile(config.source_id)

    # Determine which sheets to load
    if config.sheet_names:
        sheet_names = config.sheet_names
    else:
        sheet_names = excel_file.sheet_names

    if config.exclude_sheets:
        sheet_names = [s for s in sheet_names if s not in config.exclude_sheets]

    # Load and stack sheets
    all_sheets = []
    for sheet_name in sheet_names:
        sheet_df = pd.read_excel(config.source_id, sheet_name=sheet_name)
        if config.add_sheet_metadata:
            sheet_df['_sheet_name'] = sheet_name
        all_sheets.append(sheet_df)

    df = pd.concat(all_sheets, ignore_index=True, sort=False)
    return df


def _load_csv(config: DataSourceConfig) -> pd.DataFrame:
    """Load data from CSV file."""
    df = pd.read_csv(config.source_id)
    return df


def stage_all_sources(source_configs: list) -> Dict[str, pd.DataFrame]:
    """
    Stage multiple sources and return as a dictionary.

    Args:
        source_configs: List of DataSourceConfig objects

    Returns:
        Dictionary mapping source name to staged DataFrame
    """
    staged_data = {}

    for config in source_configs:
        try:
            staged_data[config.name] = stage_source(config)
        except Exception as e:
            print(f"✗ Error staging {config.name}: {e}")
            # Store empty DataFrame so pipeline can continue
            staged_data[config.name] = pd.DataFrame()

    return staged_data
