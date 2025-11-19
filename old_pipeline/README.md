# Data Pipeline

A scalable, layered data processing pipeline for reading data from multiple sources (Google Sheets, Postgres, Excel, CSV) with progressive transformations and automatic column normalization.

## Architecture Overview

This pipeline uses a **layered transformation** approach similar to dbt (staging → intermediate → marts), implemented in Python for flexibility with APIs, custom normalizations, and AI workflows.

```
┌─────────────────────────────────────────────────────────────┐
│                    STAGING LAYER                            │
│  Load & normalize each source individually                 │
│  - Column mapping                                           │
│  - Field filtering                                          │
│  - Normalization (email, phone)                            │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                 INTERMEDIATE LAYERS                         │
│  Progressive combining of sources                           │
│                                                             │
│  UnionLayer: Stack sources vertically (more rows)          │
│  - All columns kept                                         │
│  - No deduplication                                         │
│  - Use for: combining similar sources                       │
│                                                             │
│  MergeLayer: Join sources horizontally (enrichment)        │
│  - Merge on keys (email, phone)                            │
│  - Conflict resolution                                      │
│  - Use for: enriching data, deduplication                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    OUTPUT LAYER                             │
│  Write to Google Sheets (can have multiple outputs)        │
└─────────────────────────────────────────────────────────────┘
```

### File Structure

```
data_pipeline/
├── main_new.py                    # Pipeline orchestration
│
├── src/
│   ├── config/
│   │   ├── sources.py            # DataSourceConfig definitions
│   │   └── layers.py             # Layer configurations (Union/Merge/Output)
│   │
│   ├── transforms/
│   │   ├── staging.py            # Load & normalize sources
│   │   ├── intermediate.py       # Union & Merge operations
│   │   └── marts.py              # Output to Google Sheets
│   │
│   ├── GSheets.py                # Google Sheets client
│   ├── postgres_client.py        # Postgres client
│   └── data_cleaning.py          # Normalization utilities
│
└── ARCHITECTURE.md                # Detailed architecture docs
```

## Key Concepts

### 1. UnionLayer vs MergeLayer

**UnionLayer** (Vertical stacking):
```python
UnionLayer(
    name='all_marketing_leads',
    sources=['gsheet1', 'gsheet2', 'gsheet3'],
)
# Result: All rows from all sources, all columns kept (NaN for missing)
```

**MergeLayer** (Horizontal joining):
```python
MergeLayer(
    name='enriched_contacts',
    sources=['marketing_leads', 'purchase_data'],
    merge_keys=['email', 'phone_number'],
    columns_to_merge=['nama_usaha'],          # Single column
    columns_to_keep_separate=['revenue'],     # Prefixed: revenue_marketing_leads
    conflict_resolution={
        'nama_usaha': ConflictResolution(strategy='prefer_source', prefer_source='purchase_data'),
    }
)
# Result: One row per unique email/phone, enriched with purchase data
```

### 2. Progressive Layering

The pipeline processes data in stages:

1. **Staging**: Each source loaded and normalized independently
2. **Intermediate**: Sources combined progressively (union/merge)
3. **Output**: Write to one or more destinations

This allows you to:
- Keep raw data separate from enriched data
- Create multiple output views from the same data
- Add transformations at any layer

## Setup

### Google Sheets Authentication

The client supports three authentication methods:

**Option 1: Using environment variables (recommended for production)**
```bash
export GOOGLE_CLIENT_EMAIL="your-service-account@project.iam.gserviceaccount.com"
export GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nYour key here\n-----END PRIVATE KEY-----"
```

**Option 2: Using credentials JSON file**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"
```

**Option 3: Pass path directly in code**
```python
client = GSheetsClient(credentials_path="/path/to/credentials.json")
```

### Install Dependencies

```bash
uv add pandas google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client python-dotenv psycopg2-binary
```

## Usage

### Run the Pipeline

```bash
python main_new.py
```

### Example Output

```
============================================================
DATA PIPELINE - LAYERED ARCHITECTURE
============================================================

### STAGE 1: STAGING ###

============================================================
STAGING: leads_ads_community
Type: gsheet
============================================================
✓ Loaded 6103 rows, 30 columns
✓ Applied column mapping
✓ Applied 2 field normalizers
✓ Staged 6103 rows from leads_ads_community

### STAGE 2: INTERMEDIATE LAYERS ###

============================================================
UNION LAYER: all_marketing_leads
Sources: ['leads_ads_community', 'website_form_responses', ...]
============================================================
  - leads_ads_community: 6103 rows, 31 columns
  - website_form_responses: 3957 rows, 20 columns
✓ Unioned 10458 rows, 47 columns

### STAGE 3: OUTPUTS ###

============================================================
OUTPUT: master_contacts
Source layer: all_marketing_leads
Destination: Master Contacts
============================================================
  Rows: 10458
  Columns: 47
  Applying transformation: deduplicate_with_merge_strategy
    → 7194 rows after transformation
✓ Written 7194 rows to Master Contacts
```

## Configuration

### Adding a New Data Source

Edit `src/config/sources.py`:

```python
DataSourceConfig(
    name='new_gsheet_source',
    source_type='gsheet',
    source_id='1ABC123xyz...',  # Spreadsheet ID from URL
    column_mapping=GLOBAL_COLUMN_MAPPING,
    description='Description of this source'
)
```

**Supported source types:**
- `gsheet` - Google Sheets
- `postgres` - PostgreSQL database
- `excel` - Excel files
- `csv` - CSV files

### Adding to a Layer

Edit `src/config/layers.py`:

```python
# Add to existing union layer
UNION_MARKETING_LEADS = UnionLayer(
    name='all_marketing_leads',
    sources=['existing_source', 'new_gsheet_source'],  # Add here
)

# Or create a new layer
NEW_LAYER = MergeLayer(
    name='enriched_data',
    sources=['all_marketing_leads', 'external_api'],
    merge_keys=['email', 'phone_number'],
    columns_to_merge=['nama_usaha'],
    columns_to_keep_separate=['timestamp'],
)

# Add to pipeline
INTERMEDIATE_LAYERS = [
    UNION_MARKETING_LEADS,
    NEW_LAYER,  # Include your new layer
]
```

### Creating a New Output

```python
OUTPUT_SPECIAL = OutputLayer(
    name='special_report',
    source_layer='enriched_data',  # Layer to output
    sheet_id='${TARGET_SHEET_ID}',
    sheet_name='Special Report',
    transformations=[custom_transformation],  # Optional
)

OUTPUT_LAYERS = [
    OUTPUT_MASTER_CONTACTS,
    OUTPUT_SPECIAL,  # Add your output
]
```

## Column Mapping

The pipeline automatically normalizes column names using `GLOBAL_COLUMN_MAPPING` in `src/config/sources.py`.

### How It Works

1. All column names converted to lowercase with underscores
2. Mapping dictionary translates source-specific names to standard names
3. Applied at **staging level** (per source)

### Example Mapping

```python
GLOBAL_COLUMN_MAPPING = {
    # Phone number variations
    'no.hp/telp': 'phone_number',
    'no.telepon': 'phone_number',
    'phone_number': 'phone_number',

    # Email
    'email': 'email',

    # Business name variations
    'nama_bisnis/usaha_yang_dimiliki': 'nama_usaha',
    'nama_usaha': 'nama_usaha',
}
```

## Data Cleaning & Normalization

Built-in utilities in `src/data_cleaning.py`:

### Phone Number Normalization

```python
normalize_phone_number('0812-3456-7890')  # → '+628123456790'
```

Handles various Indonesian formats and converts to standard `+62XXXXXXXXXX`.

### Email Normalization

```python
normalize_email('User@Example.COM ')  # → 'user@example.com'
```

Lowercase and trim whitespace.

### Deduplication

```python
deduplicate_with_merge_strategy(df)
```

Smart deduplication that:
- Merges on email + phone composite key
- Fills missing values from duplicate records
- Reports duplicate statistics

## Advanced Features

### Field-Level Controls

```python
DataSourceConfig(
    name='selective_source',
    source_type='gsheet',
    source_id='1ABC...',

    # Only load specific fields
    include_fields=['email', 'phone_number', 'nama_usaha'],

    # Or exclude specific fields
    exclude_fields=['internal_notes', 'spam_score'],

    # Apply custom normalizers
    field_normalizers={
        'email': normalize_email,
        'phone_number': normalize_phone_number,
    }
)
```

### Conflict Resolution

When merging sources with overlapping columns:

```python
conflict_resolution={
    'nama_usaha': ConflictResolution(
        strategy='prefer_source',
        prefer_source='purchase_data'  # Prefer this source
    ),
    'notes': ConflictResolution(
        strategy='concat',
        separator=' | '  # Combine values
    ),
    'created_at': ConflictResolution(
        strategy='min'  # Take earliest
    ),
}
```

**Available strategies:**
- `first` - Take first non-null value
- `last` - Take last non-null value
- `prefer_source` - Prefer specific source
- `concat` - Concatenate values
- `max` / `min` - Take maximum/minimum
- `custom` - Custom function

### Future Extensions

The architecture is designed for:

**API Enrichment:**
```python
def enrich_with_vendor_api(df):
    # Call external API for each contact
    return df

layer = MergeLayer(..., transformations=[enrich_with_vendor_api])
```

**AI Classification:**
```python
def classify_industry(df):
    # Use LLM to classify business types
    return df

output = OutputLayer(..., transformations=[classify_industry])
```

## Benefits Over Traditional Approaches

| Traditional | Layered Architecture |
|-------------|---------------------|
| All merging in one step | Progressive layering |
| Hard to track column origins | Clear union vs merge distinction |
| Single output only | Multiple output views |
| Column conflicts unclear | Explicit conflict resolution |
| Monolithic code | Separated concerns |

## Troubleshooting

### Google Sheets Rate Limits

If you hit rate limits (60 reads/minute):
- Use `exclude_sheets` to skip unnecessary tabs
- Or use `sheet_names` to specify only needed sheets
- Consider caching frequently accessed sheets

### Memory Issues

For large datasets:
- Use `include_fields` to load only needed columns
- Process sources in batches
- Consider using Postgres instead of Google Sheets for large data

### Debugging

Run with verbose output to see each stage:
```bash
python main_new.py 2>&1 | tee pipeline.log
```

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture documentation
- [DEDUPLICATION_GUIDE.md](DEDUPLICATION_GUIDE.md) - Deduplication strategies

## Environment Variables

Required in `.env`:
```bash
# Google Sheets
GOOGLE_CLIENT_EMAIL=your-service-account@project.iam.gserviceaccount.com
GOOGLE_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----

# Output destination
TARGET_SHEET_ID=1eSq1GBQ...  # Your Google Sheet ID
TARGET_SHEET_NAME=Master Contacts

# Optional: Postgres
PRODUCT_DB_URL=postgresql://user:pass@host:port/db
```
