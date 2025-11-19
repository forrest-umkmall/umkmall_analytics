"""
Column mappings and normalizations for data sources.

This module defines the global column mappings that handle common variations
across all data sources.
"""

from typing import Dict, Any
import pandas as pd
from .data_cleaning import normalize_phone_number, normalize_email


# Global column mapping - handles common variations across all sources
GLOBAL_COLUMN_MAPPING = {
    # Phone number fields
    'phone_number': 'phone_number',
    'no.hp/telp': 'phone_number',
    'no.telepon': 'phone_number',
    'no.telp': 'phone_number',
    'customer_phone': 'phone_number',

    # Email
    'email': 'email',
    'customer_email': 'email',

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
    'created_time': 'timestamp',

    # Customer name fields
    'customer_first_name': 'customer_first_name',
    'customer_last_name': 'customer_last_name',

    # Payment date
    'paid_at': 'paid_at',
}


def apply_column_mapping(df: pd.DataFrame, mapping: Dict[str, str] = None) -> pd.DataFrame:
    """
    Apply column mapping to a DataFrame.

    Args:
        df: Input DataFrame
        mapping: Column mapping dictionary (defaults to GLOBAL_COLUMN_MAPPING)

    Returns:
        DataFrame with renamed columns
    """
    if mapping is None:
        mapping = GLOBAL_COLUMN_MAPPING

    # Only rename columns that exist in the DataFrame
    rename_dict = {k: v for k, v in mapping.items() if k in df.columns}

    if rename_dict:
        df = df.rename(columns=rename_dict)

    return df


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply standard normalizations to a DataFrame.

    Normalizes:
    - email addresses (lowercase, trimmed)
    - phone numbers (Indonesian format +62XXXXXXXXX)

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with normalized columns
    """
    df = df.copy()

    # Normalize email if present
    if 'email' in df.columns:
        df['email'] = df['email'].apply(normalize_email)

    # Normalize phone if present
    if 'phone_number' in df.columns:
        df['phone_number'] = df['phone_number'].apply(normalize_phone_number)

    return df
