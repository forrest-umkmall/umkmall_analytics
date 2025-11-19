"""
Intermediate layer transformations.

This module handles:
- UnionLayer: Stacking sources vertically (all columns, more rows)
- MergeLayer: Joining sources horizontally by keys (deduplication, enrichment)
"""

import pandas as pd
from typing import Dict, Union
from src.config.layers import UnionLayer, MergeLayer, ConflictResolution


def process_union_layer(
    layer: UnionLayer,
    staged_data: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Process a UnionLayer: stack sources vertically.

    Args:
        layer: UnionLayer configuration
        staged_data: Dictionary of staged DataFrames

    Returns:
        Unioned DataFrame with all rows from all sources
    """
    print(f"\n{'='*60}")
    print(f"UNION LAYER: {layer.name}")
    print(f"Sources: {layer.sources}")
    print(f"{'='*60}")

    # Collect dataframes to union
    dfs_to_union = []
    for source_name in layer.sources:
        if source_name not in staged_data:
            print(f"⚠️  Source '{source_name}' not found in staged data, skipping")
            continue

        df = staged_data[source_name].copy()

        # Add/update source tracking if needed
        if layer.add_source_column and '_source' not in df.columns:
            df['_source'] = source_name

        dfs_to_union.append(df)
        print(f"  - {source_name}: {len(df)} rows, {len(df.columns)} columns")

    if not dfs_to_union:
        print(f"⚠️  No data to union")
        return pd.DataFrame()

    # Union all dataframes (keeps all columns, fills NaN for missing)
    result = pd.concat(dfs_to_union, ignore_index=True, sort=False)

    print(f"\n✓ Unioned {len(result)} rows, {len(result.columns)} columns")

    # Apply transformations
    if layer.transformations:
        for transform_fn in layer.transformations:
            print(f"  Applying transformation: {transform_fn.__name__}")
            result = transform_fn(result)

    print(f"✓ Final: {len(result)} rows\n")

    return result


def process_merge_layer(
    layer: MergeLayer,
    staged_and_intermediate_data: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Process a MergeLayer: join sources horizontally by keys.

    Args:
        layer: MergeLayer configuration
        staged_and_intermediate_data: Dictionary of available DataFrames

    Returns:
        Merged DataFrame with enriched data
    """
    print(f"\n{'='*60}")
    print(f"MERGE LAYER: {layer.name}")
    print(f"Sources: {layer.sources}")
    print(f"Merge keys: {layer.merge_keys}")
    print(f"Merge type: {layer.merge_type}")
    print(f"{'='*60}")

    if len(layer.sources) < 2:
        raise ValueError(f"MergeLayer requires at least 2 sources, got {len(layer.sources)}")

    # Prepare sources for merging
    prepared_sources = []
    for source_name in layer.sources:
        if source_name not in staged_and_intermediate_data:
            print(f"⚠️  Source '{source_name}' not found, skipping")
            continue

        df = staged_and_intermediate_data[source_name].copy()

        # Prepare this source for merging
        df_prepared = _prepare_source_for_merge(df, source_name, layer)
        prepared_sources.append((source_name, df_prepared))

        print(f"  - {source_name}: {len(df_prepared)} rows")

    if len(prepared_sources) < 2:
        print(f"⚠️  Not enough sources to merge")
        return pd.DataFrame()

    # Check if all sources are empty
    if all(len(df) == 0 for _, df in prepared_sources):
        print(f"⚠️  All sources are empty, skipping merge")
        return pd.DataFrame()

    # Merge all sources progressively
    result = _merge_sources(prepared_sources, layer)

    print(f"\n✓ Merged {len(result)} rows, {len(result.columns)} columns")

    # Apply transformations
    if layer.transformations:
        for transform_fn in layer.transformations:
            print(f"  Applying transformation: {transform_fn.__name__}")
            result = transform_fn(result)

    print(f"✓ Final: {len(result)} rows\n")

    return result


def _prepare_source_for_merge(
    df: pd.DataFrame,
    source_name: str,
    layer: MergeLayer
) -> pd.DataFrame:
    """
    Prepare a source DataFrame for merging.

    This handles column renaming based on merge strategy:
    - columns_to_merge: Keep original name
    - columns_to_keep_separate: Add source suffix
    """
    df = df.copy()

    # Identify which columns need source suffix
    columns_to_suffix = []
    for col in df.columns:
        # Don't suffix merge keys
        if col in layer.merge_keys:
            continue

        # Don't suffix metadata columns
        if col.startswith('_'):
            continue

        # Don't suffix columns marked to merge
        if col in layer.columns_to_merge:
            continue

        # Suffix columns marked to keep separate
        if col in layer.columns_to_keep_separate:
            columns_to_suffix.append(col)

    # Rename columns with source suffix
    rename_map = {col: f"{col}_{source_name}" for col in columns_to_suffix}
    df = df.rename(columns=rename_map)

    return df


def _merge_sources(
    prepared_sources: list,
    layer: MergeLayer
) -> pd.DataFrame:
    """
    Merge multiple sources using pandas merge.

    Progressively merges sources left-to-right, handling conflicts.
    """
    # Start with first source
    source_name, result = prepared_sources[0]

    # Merge each subsequent source
    for source_name, df_to_merge in prepared_sources[1:]:
        result = _merge_two_dataframes(
            result,
            df_to_merge,
            layer.merge_keys,
            layer.merge_type,
            layer.conflict_resolution
        )

    return result


def _merge_two_dataframes(
    left: pd.DataFrame,
    right: pd.DataFrame,
    merge_keys: list,
    merge_type: str,
    conflict_resolution: Dict[str, ConflictResolution]
) -> pd.DataFrame:
    """
    Merge two dataframes with conflict resolution.

    Args:
        left: Left DataFrame
        right: Right DataFrame
        merge_keys: Columns to merge on
        merge_type: 'inner', 'outer', 'left', 'right'
        conflict_resolution: How to handle conflicting columns

    Returns:
        Merged DataFrame
    """
    # Handle empty DataFrames
    if len(left) == 0:
        return right
    if len(right) == 0:
        return left

    # Check if merge keys exist in both DataFrames
    missing_left = [k for k in merge_keys if k not in left.columns]
    missing_right = [k for k in merge_keys if k not in right.columns]

    if missing_left or missing_right:
        print(f"  ⚠️  Warning: Missing merge keys, cannot merge")
        if missing_left:
            print(f"     Left DF missing: {missing_left}")
        if missing_right:
            print(f"     Right DF missing: {missing_right}")
        # Return left or union them?
        return left if merge_type == 'left' else pd.concat([left, right], ignore_index=True)

    # Find overlapping columns (excluding merge keys)
    left_cols = set(left.columns) - set(merge_keys)
    right_cols = set(right.columns) - set(merge_keys)
    overlapping = left_cols & right_cols

    # Perform merge with suffixes for overlapping columns
    merged = pd.merge(
        left,
        right,
        on=merge_keys,
        how=merge_type,
        suffixes=('_left', '_right')
    )

    # Resolve conflicts for overlapping columns
    for col in overlapping:
        left_col = f"{col}_left"
        right_col = f"{col}_right"

        # Get conflict resolution strategy for this column
        resolution = conflict_resolution.get(col, ConflictResolution(strategy='first'))

        # Resolve the conflict
        merged[col] = _resolve_conflict(
            merged[left_col],
            merged[right_col],
            resolution
        )

        # Drop the temporary suffixed columns
        merged = merged.drop(columns=[left_col, right_col])

    return merged


def _resolve_conflict(
    left_series: pd.Series,
    right_series: pd.Series,
    resolution: ConflictResolution
) -> pd.Series:
    """
    Resolve conflicts between two series based on strategy.

    Args:
        left_series: Values from left DataFrame
        right_series: Values from right DataFrame
        resolution: ConflictResolution strategy

    Returns:
        Resolved series
    """
    if resolution.strategy == 'first':
        # Take left value, fill with right if left is NaN
        return left_series.fillna(right_series)

    elif resolution.strategy == 'last':
        # Take right value, fill with left if right is NaN
        return right_series.fillna(left_series)

    elif resolution.strategy == 'prefer_source':
        # This is handled at a higher level by ordering sources
        # For now, default to 'first'
        return left_series.fillna(right_series)

    elif resolution.strategy == 'concat':
        # Concatenate non-null values
        def concat_values(left, right):
            if pd.isna(left) and pd.isna(right):
                return None
            elif pd.isna(left):
                return str(right)
            elif pd.isna(right):
                return str(left)
            else:
                left_str = str(left)
                right_str = str(right)
                if left_str == right_str:
                    return left_str
                return f"{left_str}{resolution.separator}{right_str}"

        return pd.Series([
            concat_values(l, r)
            for l, r in zip(left_series, right_series)
        ])

    elif resolution.strategy == 'max':
        return pd.Series([
            max(l, r) if pd.notna(l) and pd.notna(r)
            else l if pd.notna(l)
            else r
            for l, r in zip(left_series, right_series)
        ])

    elif resolution.strategy == 'min':
        return pd.Series([
            min(l, r) if pd.notna(l) and pd.notna(r)
            else l if pd.notna(l)
            else r
            for l, r in zip(left_series, right_series)
        ])

    elif resolution.strategy == 'custom':
        if resolution.custom_fn is None:
            raise ValueError("Custom conflict resolution requires custom_fn")
        return resolution.custom_fn(left_series, right_series)

    else:
        raise ValueError(f"Unknown conflict resolution strategy: {resolution.strategy}")


def process_intermediate_layer(
    layer: Union[UnionLayer, MergeLayer],
    data: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Process an intermediate layer (union or merge).

    Args:
        layer: UnionLayer or MergeLayer configuration
        data: Dictionary of available DataFrames

    Returns:
        Processed DataFrame
    """
    if isinstance(layer, UnionLayer):
        return process_union_layer(layer, data)
    elif isinstance(layer, MergeLayer):
        return process_merge_layer(layer, data)
    else:
        raise ValueError(f"Unknown layer type: {type(layer)}")
