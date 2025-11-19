"""
Data cleaning and transformation utilities.

This module provides functions for normalizing, cleaning, and deduplicating data.
"""

import pandas as pd
import re
from typing import Optional, List, Dict, Any


def normalize_phone_number(phone: Any) -> Optional[str]:
    """
    Normalize Indonesian phone numbers to a standard format.

    Converts various formats to: +62XXXXXXXXXX

    Examples:
        0812-3456-7890 -> +628123456790
        +62 812 3456 7890 -> +628123456790
        62812-3456-7890 -> +628123456790
        812-3456-7890 -> +628123456790

    Args:
        phone: Phone number in any format (str, int, or None)

    Returns:
        Normalized phone number string or None if invalid
    """
    if pd.isna(phone) or phone == '':
        return None

    # Convert to string and remove all spaces, dashes, parentheses, dots
    phone_str = str(phone).strip()
    phone_str = re.sub(r'[\s\-\(\)\.]', '', phone_str)

    # Remove any non-digit characters except leading +
    phone_str = re.sub(r'[^\d+]', '', phone_str)

    # Remove leading zeros or +62 or 62
    if phone_str.startswith('+62'):
        phone_str = phone_str[3:]
    elif phone_str.startswith('62'):
        phone_str = phone_str[2:]
    elif phone_str.startswith('0'):
        phone_str = phone_str[1:]

    # Check if we have a valid number (should be 9-12 digits after removing prefix)
    if not phone_str.isdigit() or len(phone_str) < 9 or len(phone_str) > 12:
        return None

    # Add +62 prefix
    return f'+62{phone_str}'


def normalize_email(email: Any) -> Optional[str]:
    """
    Normalize email addresses to lowercase and trim whitespace.

    Args:
        email: Email address in any format

    Returns:
        Normalized email string or None if invalid/empty
    """
    if pd.isna(email) or email == '':
        return None

    email_str = str(email).strip().lower()

    # Basic email validation (has @ and . after @)
    if '@' not in email_str or '.' not in email_str.split('@')[1]:
        return None

    return email_str


def create_composite_key(row: pd.Series, email_col: str = 'email',
                        phone_col: str = 'phone_number') -> Optional[str]:
    """
    Create a composite key from email and phone number.

    Args:
        row: DataFrame row
        email_col: Name of email column
        phone_col: Name of phone number column

    Returns:
        Composite key string or None if both are missing
    """
    email = row.get(email_col)
    phone = row.get(phone_col)

    # Create key from available fields
    if pd.notna(email) and pd.notna(phone):
        return f'{email}|{phone}'
    elif pd.notna(email):
        return f'{email}|'
    elif pd.notna(phone):
        return f'|{phone}'
    else:
        return None


def deduplicate_by_composite_key(
    df: pd.DataFrame,
    email_col: str = 'email',
    phone_col: str = 'phone_number',
    keep: str = 'first',
    add_duplicate_info: bool = True
) -> pd.DataFrame:
    """
    Deduplicate DataFrame based on email + phone number composite key.

    Records are considered duplicates if they share:
    - Both email AND phone number (exact match)
    - Same email (even if phone differs)
    - Same phone (even if email differs)

    Args:
        df: Input DataFrame
        email_col: Name of email column (default: 'email')
        phone_col: Name of phone number column (default: 'phone_number')
        keep: Which duplicate to keep ('first', 'last', or False to remove all duplicates)
        add_duplicate_info: If True, adds columns with duplicate statistics

    Returns:
        Deduplicated DataFrame
    """
    if df.empty:
        return df

    df = df.copy()

    # Normalize email and phone before deduplication
    if email_col in df.columns:
        df[f'{email_col}_normalized'] = df[email_col].apply(normalize_email)
    else:
        df[f'{email_col}_normalized'] = None

    if phone_col in df.columns:
        df[f'{phone_col}_normalized'] = df[phone_col].apply(normalize_phone_number)
    else:
        df[f'{phone_col}_normalized'] = None

    # Create composite key
    df['_composite_key'] = df.apply(
        lambda row: create_composite_key(
            row,
            f'{email_col}_normalized',
            f'{phone_col}_normalized'
        ),
        axis=1
    )

    # Count duplicates before removing
    initial_count = len(df)

    if add_duplicate_info:
        # Add duplicate count for each key
        df['duplicate_count'] = df.groupby('_composite_key')['_composite_key'].transform('count')
        df['is_duplicate'] = df['duplicate_count'] > 1

    # Remove duplicates
    df_deduped = df.drop_duplicates(subset=['_composite_key'], keep=keep)

    # Clean up temporary columns (but keep normalized versions)
    df_deduped = df_deduped.drop(columns=['_composite_key'])

    final_count = len(df_deduped)
    duplicates_removed = initial_count - final_count

    print(f"\n{'='*60}")
    print(f"Deduplication Summary")
    print(f"{'='*60}")
    print(f"Initial rows: {initial_count:,}")
    print(f"Final rows: {final_count:,}")
    print(f"Duplicates removed: {duplicates_removed:,}")
    print(f"Duplicate rate: {(duplicates_removed/initial_count*100):.1f}%")
    print(f"{'='*60}\n")

    return df_deduped


def deduplicate_with_merge_strategy(
    df: pd.DataFrame,
    email_col: str = 'email',
    phone_col: str = 'phone_number',
    merge_cols: Optional[List[str]] = None,
    keep: str = 'first'
) -> pd.DataFrame:
    """
    Advanced deduplication that merges data from duplicate records.

    When duplicates are found, this function keeps the first record but fills
    in any missing values with data from duplicate records.

    Args:
        df: Input DataFrame
        email_col: Name of email column
        phone_col: Name of phone number column
        merge_cols: List of columns to merge data from (if None, merges all columns)
        keep: Which duplicate to use as base ('first' or 'last')

    Returns:
        Deduplicated DataFrame with merged data
    """
    if df.empty:
        return df

    df = df.copy()

    # Normalize keys
    if email_col in df.columns:
        df[f'{email_col}_normalized'] = df[email_col].apply(normalize_email)
    else:
        df[f'{email_col}_normalized'] = None

    if phone_col in df.columns:
        df[f'{phone_col}_normalized'] = df[phone_col].apply(normalize_phone_number)
    else:
        df[f'{phone_col}_normalized'] = None

    # Create composite key
    df['_composite_key'] = df.apply(
        lambda row: create_composite_key(
            row,
            f'{email_col}_normalized',
            f'{phone_col}_normalized'
        ),
        axis=1
    )

    # Get columns to merge
    if merge_cols is None:
        merge_cols = [col for col in df.columns if col not in
                     ['_composite_key', f'{email_col}_normalized', f'{phone_col}_normalized']]

    # Group by composite key and merge
    def merge_group(group):
        """Merge duplicate records, filling NaN values from other records."""
        if len(group) == 1:
            return group.iloc[0] if keep == 'first' else group.iloc[-1]

        # Start with the first/last record
        base = group.iloc[0].copy() if keep == 'first' else group.iloc[-1].copy()

        # Fill missing values from other records
        for col in merge_cols:
            if pd.isna(base[col]):
                # Find first non-null value in the group
                non_null_values = group[col].dropna()
                if len(non_null_values) > 0:
                    base[col] = non_null_values.iloc[0]

        return base

    initial_count = len(df)

    # Apply merging
    df_merged = df.groupby('_composite_key', as_index=False).apply(merge_group)

    # Reset index
    if isinstance(df_merged.index, pd.MultiIndex):
        df_merged = df_merged.reset_index(drop=True)

    # Clean up temporary column
    df_merged = df_merged.drop(columns=['_composite_key'])

    final_count = len(df_merged)
    duplicates_merged = initial_count - final_count

    print(f"\n{'='*60}")
    print(f"Smart Deduplication with Merging Summary")
    print(f"{'='*60}")
    print(f"Initial rows: {initial_count:,}")
    print(f"Final rows: {final_count:,}")
    print(f"Duplicates merged: {duplicates_merged:,}")
    print(f"Duplicate rate: {(duplicates_merged/initial_count*100):.1f}%")
    print(f"{'='*60}\n")

    return df_merged


def analyze_duplicates(
    df: pd.DataFrame,
    email_col: str = 'email',
    phone_col: str = 'phone_number'
) -> Dict[str, Any]:
    """
    Analyze duplicate patterns in the dataset.

    Args:
        df: Input DataFrame
        email_col: Name of email column
        phone_col: Name of phone number column

    Returns:
        Dictionary with duplicate statistics
    """
    if df.empty:
        return {'total_rows': 0}

    df_temp = df.copy()

    # Normalize
    df_temp['email_norm'] = df_temp[email_col].apply(normalize_email) if email_col in df_temp.columns else None
    df_temp['phone_norm'] = df_temp[phone_col].apply(normalize_phone_number) if phone_col in df_temp.columns else None

    stats = {
        'total_rows': len(df_temp),
        'rows_with_email': df_temp['email_norm'].notna().sum() if 'email_norm' in df_temp else 0,
        'rows_with_phone': df_temp['phone_norm'].notna().sum() if 'phone_norm' in df_temp else 0,
        'rows_with_both': ((df_temp['email_norm'].notna()) & (df_temp['phone_norm'].notna())).sum() if 'email_norm' in df_temp and 'phone_norm' in df_temp else 0,
        'rows_with_neither': ((df_temp['email_norm'].isna()) & (df_temp['phone_norm'].isna())).sum() if 'email_norm' in df_temp and 'phone_norm' in df_temp else len(df_temp),
    }

    # Count duplicates
    if 'email_norm' in df_temp:
        stats['duplicate_emails'] = df_temp['email_norm'].notna().sum() - df_temp['email_norm'].nunique()
        stats['unique_emails'] = df_temp['email_norm'].nunique()

    if 'phone_norm' in df_temp:
        stats['duplicate_phones'] = df_temp['phone_norm'].notna().sum() - df_temp['phone_norm'].nunique()
        stats['unique_phones'] = df_temp['phone_norm'].nunique()

    # Composite key duplicates
    df_temp['composite_key'] = df_temp.apply(
        lambda row: create_composite_key(row, 'email_norm', 'phone_norm'),
        axis=1
    )
    stats['duplicate_composite'] = df_temp['composite_key'].notna().sum() - df_temp['composite_key'].nunique()
    stats['unique_composite'] = df_temp['composite_key'].nunique()

    return stats


def print_duplicate_analysis(stats: Dict[str, Any]):
    """Pretty print duplicate analysis statistics."""
    print(f"\n{'='*60}")
    print(f"Duplicate Analysis")
    print(f"{'='*60}")
    print(f"Total rows: {stats['total_rows']:,}")
    print(f"")
    print(f"Data completeness:")
    print(f"  - Rows with email: {stats['rows_with_email']:,} ({stats['rows_with_email']/stats['total_rows']*100:.1f}%)")
    print(f"  - Rows with phone: {stats['rows_with_phone']:,} ({stats['rows_with_phone']/stats['total_rows']*100:.1f}%)")
    print(f"  - Rows with both: {stats['rows_with_both']:,} ({stats['rows_with_both']/stats['total_rows']*100:.1f}%)")
    print(f"  - Rows with neither: {stats['rows_with_neither']:,} ({stats['rows_with_neither']/stats['total_rows']*100:.1f}%)")
    print(f"")

    if 'unique_emails' in stats:
        print(f"Email duplicates:")
        print(f"  - Unique emails: {stats['unique_emails']:,}")
        print(f"  - Duplicate emails: {stats['duplicate_emails']:,}")

    if 'unique_phones' in stats:
        print(f"Phone duplicates:")
        print(f"  - Unique phones: {stats['unique_phones']:,}")
        print(f"  - Duplicate phones: {stats['duplicate_phones']:,}")

    print(f"")
    print(f"Composite key (email + phone) duplicates:")
    print(f"  - Unique keys: {stats['unique_composite']:,}")
    print(f"  - Duplicate keys: {stats['duplicate_composite']:,}")
    print(f"{'='*60}\n")
