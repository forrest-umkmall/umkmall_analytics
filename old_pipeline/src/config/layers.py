"""
Layer configurations for progressive data transformation.

This module defines how staged data sources are combined:
- UnionLayer: Stack sources vertically (more rows, all columns kept)
- MergeLayer: Join sources horizontally by keys (deduplication, conflict resolution)
- OutputLayer: Write final data to Google Sheets
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable, Literal


@dataclass
class UnionLayer:
    """
    Stack multiple sources vertically (UNION operation).

    Use this when:
    - Combining similar data sources (e.g., all your Google Sheet leads)
    - You want to keep all rows from all sources
    - Columns may differ between sources (NaN filled automatically)
    - No deduplication needed at this stage

    Example:
        UnionLayer(
            name='all_marketing_leads',
            sources=['leads_ads_community', 'website_form_responses', 'leads_course_strategi_ads'],
            add_source_column=True,  # Tracks which source each row came from
        )
    """
    name: str
    sources: List[str]                              # Source names to stack
    add_source_column: bool = True                  # Add 'data_source' tracking column
    transformations: Optional[List[Callable]] = None  # Applied after stacking


@dataclass
class ConflictResolution:
    """
    Strategy for resolving conflicts when merging sources.

    When two sources have the same column but different values for the same key:
    - 'first': Take value from first source in list
    - 'last': Take value from last source in list
    - 'prefer_source': Prefer values from specified source
    - 'concat': Concatenate values (for text fields)
    - 'max'/'min': Take max/min value (for numeric fields)
    - 'custom': Use custom function
    """
    strategy: Literal['first', 'last', 'prefer_source', 'concat', 'max', 'min', 'custom']
    prefer_source: Optional[str] = None             # Required if strategy='prefer_source'
    separator: str = ' | '                          # Used if strategy='concat'
    custom_fn: Optional[Callable] = None            # Required if strategy='custom'


@dataclass
class MergeLayer:
    """
    Merge multiple sources horizontally based on key columns (JOIN operation).

    Use this when:
    - Enriching data from one source with data from another
    - You want to deduplicate based on email/phone
    - Need to combine columns from different sources for the same entity

    Column handling:
    - columns_to_merge: Single column in output (need conflict resolution)
    - columns_to_keep_separate: Prefixed by source name (e.g., 'revenue_postgres', 'revenue_gsheet')

    Example:
        MergeLayer(
            name='enriched_contacts',
            sources=['all_marketing_leads', 'purchase_form_data'],
            merge_keys=['email', 'phone_number'],
            merge_type='left',  # Keep all marketing leads, add purchase data where available
            columns_to_merge=['nama_usaha', 'provinsi_usaha'],
            columns_to_keep_separate=['revenue', 'registration_date'],
            conflict_resolution={
                'nama_usaha': ConflictResolution(strategy='prefer_source', prefer_source='purchase_form_data'),
                'provinsi_usaha': ConflictResolution(strategy='first'),
            }
        )
    """
    name: str
    sources: List[str]                              # Source names to merge
    merge_keys: List[str]                           # Keys to merge on (e.g., ['email', 'phone_number'])
    merge_type: Literal['inner', 'outer', 'left', 'right'] = 'outer'

    # Column handling strategy
    columns_to_merge: List[str] = field(default_factory=list)        # Single column, needs conflict resolution
    columns_to_keep_separate: List[str] = field(default_factory=list)  # Prefix with source name

    # Conflict resolution for merged columns
    conflict_resolution: Dict[str, ConflictResolution] = field(default_factory=dict)

    transformations: Optional[List[Callable]] = None  # Applied after merging


@dataclass
class OutputLayer:
    """
    Write a layer to Google Sheets.

    Example:
        OutputLayer(
            name='master_contacts_output',
            source_layer='enriched_contacts',
            sheet_id=os.getenv('TARGET_SHEET_ID'),
            sheet_name='MasterContacts',
            column_order=['email', 'phone_number', 'nama_usaha'],  # Optional: specify column order
            include_columns=['email', 'phone_number', 'nama_usaha'],  # Optional: only include these columns
        )
    """
    name: str
    source_layer: str                               # Single layer to output
    sheet_id: str                                   # Google Sheets ID
    sheet_name: str                                 # Sheet name
    transformations: Optional[List[Callable]] = None  # Final transformations before write
    column_order: Optional[List[str]] = None        # Optional: reorder columns by name
    include_columns: Optional[List[str]] = None     # Optional: subset of columns to include


# ==============================================================================
# EXAMPLE PIPELINE CONFIGURATION
# ==============================================================================

# Import transformation functions
from src.data_cleaning import deduplicate_with_merge_strategy


# Layer 1: Union all marketing lead sources
UNION_MARKETING_LEADS = UnionLayer(
    name='all_marketing_leads',
    sources=[
        'leads_ads_community',
        'website_form_responses',
        'leads_course_strategi_ads',
        'branding_level_up',  # Commented out - rate limit issue
    ],
    add_source_column=True,  # Tracks origin
)


# Layer 2: Deduplicate marketing leads
# Merge marketing leads with purchase data
MERGE_WITH_PURCHASES = MergeLayer(
    name='enriched_contacts',
    sources=['all_marketing_leads', 'purchase_form_data'],
    merge_keys=['email', 'phone_number'],  # Merge on BOTH email and phone_number
    merge_type='outer',
    columns_to_merge=['nama_usaha', 'nama_pemilik_usaha', 'provinsi_usaha', 'bidang_usaha'],
    columns_to_keep_separate=['timestamp', 'paid_at'],
    conflict_resolution={
        # For overlapping columns, prefer values from marketing leads (first source)
        'nama_usaha': ConflictResolution(strategy='first'),
        'nama_pemilik_usaha': ConflictResolution(strategy='first'),
        'provinsi_usaha': ConflictResolution(strategy='first'),
        'bidang_usaha': ConflictResolution(strategy='first'),
    },
    transformations=[deduplicate_with_merge_strategy],
)


# Output 1: All data by contact (enriched with purchase data)
OUTPUT_ALL_DATA_BY_CONTACT = OutputLayer(
    name='all_data_by_contact',
    source_layer='enriched_contacts',  # Use merged data instead
    sheet_id='${TARGET_SHEET_ID}',  # Will be resolved at runtime
    sheet_name='All Data By Contact',
    transformations=[],  # Deduplication already applied in merge layer
)


# Output 2: Marketing data (raw union, with deduplication)
OUTPUT_MARKETING_DATA = OutputLayer(
    name='marketing_data',
    source_layer='all_marketing_leads',
    sheet_id='${TARGET_SHEET_ID}',  # Will be resolved at runtime
    sheet_name='Marketing Data',
    transformations=[deduplicate_with_merge_strategy],
)


# Output 3: Product data (purchase form data only)
OUTPUT_PRODUCT_DATA = OutputLayer(
    name='product_data',
    source_layer='purchase_form_data',
    sheet_id='${TARGET_SHEET_ID}',  # Will be resolved at runtime
    sheet_name='Product Data',
    transformations=[],
)


# Output 4: Field metadata (all fields from all sources)
OUTPUT_FIELD_METADATA = OutputLayer(
    name='field_metadata',
    source_layer='_field_metadata',  # Special metadata layer
    sheet_id='${TARGET_SHEET_ID}',  # Will be resolved at runtime
    sheet_name='All Fields in Sources',
    transformations=[],
)


# ==============================================================================
# PIPELINE DEFINITION
# ==============================================================================

# Define the complete pipeline
INTERMEDIATE_LAYERS = [
    UNION_MARKETING_LEADS,
    MERGE_WITH_PURCHASES,  # Commented out - Postgres not running
]

OUTPUT_LAYERS = [
    OUTPUT_ALL_DATA_BY_CONTACT,
    OUTPUT_MARKETING_DATA,
    OUTPUT_PRODUCT_DATA,
    OUTPUT_FIELD_METADATA,
]
