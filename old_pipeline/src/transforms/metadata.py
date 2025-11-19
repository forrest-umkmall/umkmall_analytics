"""
Metadata extraction for data sources.

This module provides utilities to extract field-level metadata
from staged data sources.
"""

import pandas as pd
from typing import Dict


def extract_field_metadata(staged_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Extract field metadata from all staged sources.

    Creates a DataFrame with columns:
    - field_source: Name of the data source
    - field_name: Name of the field/column
    - field_type: Data type of the field

    Args:
        staged_data: Dictionary mapping source name to staged DataFrame

    Returns:
        DataFrame with field metadata from all sources
    """
    print(f"\n{'='*60}")
    print("EXTRACTING FIELD METADATA")
    print(f"{'='*60}")

    metadata_rows = []

    for source_name, df in staged_data.items():
        if df.empty:
            print(f"  Skipping {source_name} (empty)")
            continue

        # Extract field info for each column
        for col_name in df.columns:
            # Skip internal metadata columns
            if col_name.startswith('_'):
                continue

            # Determine pandas dtype
            dtype = df[col_name].dtype

            # Map pandas dtype to more readable type
            if pd.api.types.is_integer_dtype(dtype):
                field_type = 'integer'
            elif pd.api.types.is_float_dtype(dtype):
                field_type = 'float'
            elif pd.api.types.is_bool_dtype(dtype):
                field_type = 'boolean'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                field_type = 'datetime'
            elif pd.api.types.is_string_dtype(dtype) or dtype == 'object':
                field_type = 'text'
            else:
                field_type = str(dtype)

            metadata_rows.append({
                'field_source': source_name,
                'field_name': col_name,
                'field_type': field_type,
            })

        print(f"  ✓ {source_name}: {len([c for c in df.columns if not c.startswith('_')])} fields")

    # Create metadata DataFrame
    metadata_df = pd.DataFrame(metadata_rows)

    # Sort by source then field name for better readability
    if not metadata_df.empty:
        metadata_df = metadata_df.sort_values(['field_source', 'field_name']).reset_index(drop=True)

    print(f"\n✓ Extracted metadata for {len(metadata_df)} fields across {len(staged_data)} sources")

    return metadata_df
