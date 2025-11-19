"""
Enhanced data source configuration with field-level controls.

This module defines how to extract and normalize data from each source.
All field mappings and normalizations happen at this staging level.
"""

from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field


@dataclass
class DataSourceConfig:
    """Configuration for a single data source with field-level control."""

    # Basic identification
    name: str
    source_type: str  # 'gsheet', 'excel', 'csv', 'postgres'
    source_id: str    # spreadsheet_id, file_path, or connection_string
    description: Optional[str] = None

    # Field-level controls (applied during extraction)
    column_mapping: Optional[Dict[str, str]] = None  # Source col -> target col
    include_fields: Optional[List[str]] = None       # Only load these (after mapping)
    exclude_fields: Optional[List[str]] = None       # Exclude these (after mapping)

    # Field normalizations (applied after mapping)
    field_normalizers: Optional[Dict[str, Callable]] = None  # {'phone_number': normalize_phone}

    # Sheet/table controls
    sheet_names: Optional[List[str]] = None          # Specific sheets/tables
    exclude_sheets: Optional[List[str]] = None       # Exclude these sheets

    # Postgres-specific
    query: Optional[str] = None                      # Custom SQL query
    schema: Optional[str] = None                     # Database schema

    # Metadata to carry through
    add_source_metadata: bool = True                 # Add 'data_source' column
    add_sheet_metadata: bool = True                  # Add 'sheet_name' column


# Global column mapping - handles common variations across all sources
# This gets applied at the STAGING level for each source
GLOBAL_COLUMN_MAPPING = {
    # Phone number fields
    'phone_number': 'phone_number',
    'no.hp/telp': 'phone_number',
    'no.telepon': 'phone_number',
    'no.telp': 'phone_number',

    # Email
    'email': 'email',
    'customer_email': 'email',

    # Phone number
    'customer_phone': 'phone_number',

    # Business name
    'nama_bisnis/usaha_yang_dimiliki': 'nama_usaha',
    'nama_usaha': 'nama_usaha',

    # Business owner name
    'nama_pemilik_usaha': 'nama_pemilik_usaha',

    # Social media account name
    'apa_nama_akun_media_sosial_jualan/usaha_kamu?': 'nama_akun_media_sosial',
    'apa_nama_akun_media_sosial_usaha_kamu?': 'nama_akun_media_sosial',
    'nama_akun_media_sosial_atau_marketplace_jualan_anda\n(cth._instagram,_tiktok,_facebook,_shopee,_tokopedia,_dll': 'nama_akun_media_sosial',

    # E-commerce/marketplace account name
    'apa_nama_akun_e-commerce_atau_marketplace_jualan/usaha_kamu?': 'nama_akun_ecommerce',

    # Business field/industry
    'bidang_usaha_yang_sedang_dijalani_saat_ini?_(cth._makanan,_fashion,_crafting,_jasa)': 'bidang_usaha',
    'bidang_usaha_yang_sedang_dijalankan_saat_ini?_cth:_pakaian,_makanan_ringan,_kerajinan_tangan,_dll': 'bidang_usaha',
    'jenis_produk_atau_jasa_yang_dihasilkan': 'bidang_usaha',

    # Business duration/age
    'lama_usaha/bisnis_telah_berdiri': 'lama_usaha',
    'berapa_lama_umkm_sudah_beroperasi?': 'lama_usaha',

    # Business location/province
    'di_provinsi_mana_usaha_kamu_beroperasi?': 'provinsi_usaha',
    'dimana_anda_menjalankan_usaha_(provinsi_domisili)': 'provinsi_usaha',

    # Monthly income
    'pendapatan_bulanan_(rata-rata)': 'pendapatan_bulanan',

    # UMKM community membership
    'apakah_anda_saat_ini_tergabung_dalam_komunitas_umkm?': 'tergabung_komunitas_umkm',
    'apakah_anda_saat_ini_tergabung_ke_dalam_komunitas_umkm?': 'tergabung_komunitas_umkm',

    # NIB certification
    'apakah_kamu_sudah_memiliki_nib?': 'memiliki_nib',
    'apakah_kamu_sudah_memiliki_nib?\n': 'memiliki_nib',

    # Halal certification
    'apa_kamu_sudah_memiliki_sertifikasi_halal?': 'memiliki_sertifikasi_halal',
    'apakah_kamu_sudah_memiliki_sertifikasi_halal?': 'memiliki_sertifikasi_halal',

    # Timestamp/Created time
    'timestamp': 'timestamp',
    'created_time': 'timestamp'
}


# Standard field normalizers applied after column mapping
# Import these when needed to avoid circular imports
def get_standard_normalizers():
    """Get standard field normalizers (lazy import to avoid circular dependency)."""
    from src.data_cleaning import normalize_phone_number, normalize_email
    return {
        'email': normalize_email,
        'phone_number': normalize_phone_number,
    }

STANDARD_NORMALIZERS = None  # Will be set lazily


# Define all data sources here
DATA_SOURCES = [
    # Temporarily commented out - Postgres not running
    DataSourceConfig(
        name='purchase_form_data',
        source_type='postgres',
        source_id='${PRODUCT_DB_URL}',
        schema='public',
        query='''
            select customer_first_name, customer_last_name, customer_email, customer_phone, paid_at
            from payments where paid_at is not null;
        ''',
        column_mapping=GLOBAL_COLUMN_MAPPING,
        # field_normalizers will be set in staging
        description='Product and category data from Supabase'
    ),

    DataSourceConfig(
        name='leads_ads_community',
        source_type='gsheet',
        source_id='1pRUUvUEUZkUJijw_f1EJTc9j5szUSv8NsVUsWQAcm6Y',
        column_mapping=GLOBAL_COLUMN_MAPPING,
        # field_normalizers will be set in staging
        description='Live registration data from Google Sheets - Ads Community'
    ),

    DataSourceConfig(
        name='website_form_responses',
        source_type='gsheet',
        source_id='1YKVuHcN2OE9b7Pnl5OJLSw0ZGAy594rDLb9UuL3FmYA',
        column_mapping=GLOBAL_COLUMN_MAPPING,
        # field_normalizers will be set in staging
        description='Live registration data from Google Sheets - Website Form'
    ),

    DataSourceConfig(
        name='leads_course_strategi_ads',
        source_type='gsheet',
        source_id='1RGEkNKiFA3k_CpVvu311_0V2C9TwRY35svEFG8jqllE',
        column_mapping=GLOBAL_COLUMN_MAPPING,
        # field_normalizers will be set in staging
        description='Live registration data from Google Sheets - Course Strategy Ads'
    ),

    # Temporarily commented out - causes Google Sheets API rate limit (too many tabs)
    DataSourceConfig(
        name='branding_level_up',
        source_type='gsheet',
        source_id='16p-addrDK67qIacUIEeSVQ06ZU1IuGa1phKtz19M5l4',
        column_mapping=GLOBAL_COLUMN_MAPPING,
        # field_normalizers will be set in staging
        sheet_names=['All Data'],  # Only load this sheet
        description='Live registration data from Google Sheets - Branding Level Up'
    ),
]


def get_data_source(name: str) -> Optional[DataSourceConfig]:
    """Get a data source configuration by name."""
    for source in DATA_SOURCES:
        if source.name == name:
            return source
    return None


def get_all_data_sources() -> List[DataSourceConfig]:
    """Get all configured data sources."""
    return DATA_SOURCES.copy()


def get_data_sources_by_type(source_type: str) -> List[DataSourceConfig]:
    """Get all data sources of a specific type."""
    return [source for source in DATA_SOURCES if source.source_type == source_type]
