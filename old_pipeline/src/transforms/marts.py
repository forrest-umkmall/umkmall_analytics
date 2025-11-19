"""
Output/marts layer transformations.

This module handles final outputs to Google Sheets or other destinations.
"""

import os
import pandas as pd
from typing import Dict
from src.config.layers import OutputLayer
from src.GSheets import GSheetsClient


def write_output(
    layer: OutputLayer,
    data: Dict[str, pd.DataFrame]
) -> None:
    """
    Write an output layer to Google Sheets.

    Args:
        layer: OutputLayer configuration
        data: Dictionary of available DataFrames (intermediate + staged)
    """
    print(f"\n{'='*60}")
    print(f"OUTPUT: {layer.name}")
    print(f"Source layer: {layer.source_layer}")
    print(f"Destination: {layer.sheet_name}")
    print(f"{'='*60}")

    # Get the source data
    if layer.source_layer not in data:
        print(f"✗ Source layer '{layer.source_layer}' not found")
        return

    df = data[layer.source_layer].copy()

    print(f"  Rows: {len(df)}")
    print(f"  Columns: {len(df.columns)}")

    # Apply transformations
    if layer.transformations:
        for transform_fn in layer.transformations:
            print(f"  Applying transformation: {transform_fn.__name__}")
            df = transform_fn(df)
            print(f"    → {len(df)} rows after transformation")

    # Remove internal metadata columns before writing
    metadata_cols = [col for col in df.columns if col.startswith('_')]
    if metadata_cols:
        df = df.drop(columns=metadata_cols)
        print(f"  Removed {len(metadata_cols)} metadata columns")

    # Apply column subsetting if specified
    if layer.include_columns:
        available_cols = [col for col in layer.include_columns if col in df.columns]
        missing_cols = [col for col in layer.include_columns if col not in df.columns]

        if missing_cols:
            print(f"  ⚠️  Warning: {len(missing_cols)} columns not found: {missing_cols[:5]}")

        df = df[available_cols]
        print(f"  Subset to {len(available_cols)} columns")

    # Apply column reordering if specified
    if layer.column_order:
        # Separate columns into ordered and unordered
        ordered_cols = [col for col in layer.column_order if col in df.columns]
        remaining_cols = [col for col in df.columns if col not in layer.column_order]

        # Combine ordered columns first, then remaining
        final_column_order = ordered_cols + remaining_cols
        df = df[final_column_order]

        missing_ordered = [col for col in layer.column_order if col not in df.columns]
        if missing_ordered:
            print(f"  ⚠️  Warning: {len(missing_ordered)} ordered columns not found: {missing_ordered[:5]}")

        print(f"  Reordered columns: {len(ordered_cols)} specified, {len(remaining_cols)} remaining")

    # Resolve environment variables in sheet_id and sheet_name
    sheet_id = _resolve_env_vars(layer.sheet_id)
    sheet_name = _resolve_env_vars(layer.sheet_name)

    # Write to Google Sheets
    _write_to_gsheet(df, sheet_id, sheet_name)

    print(f"✓ Written {len(df)} rows to {layer.sheet_name}\n")


def _resolve_env_vars(value: str) -> str:
    """
    Resolve environment variables in a string.

    Supports ${VAR_NAME} syntax.

    Args:
        value: String that may contain ${VAR_NAME} patterns

    Returns:
        String with environment variables resolved
    """
    import re

    def replace_env_var(match):
        var_name = match.group(1)
        return os.getenv(var_name, '')

    return re.sub(r'\$\{([^}]+)\}', replace_env_var, value)


def _write_to_gsheet(
    df: pd.DataFrame,
    spreadsheet_id: str,
    sheet_name: str
) -> None:
    """
    Write DataFrame to Google Sheets.

    Args:
        df: DataFrame to write
        spreadsheet_id: Google Sheets ID
        sheet_name: Sheet name
    """
    if not spreadsheet_id:
        print(f"⚠️  No spreadsheet ID provided, skipping write")
        return

    client = GSheetsClient()

    client.write_dataframe(
        spreadsheet_id=spreadsheet_id,
        range_name=sheet_name,
        df=df,
        include_index=False,
        clear_before_write=True
    )

    print(f"  📊 View at: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
