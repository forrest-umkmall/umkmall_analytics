{{
    config(
        materialized='view'
    )
}}

select
    email,
    phone_number,
    nama_usaha,
    nama_pemilik_usaha,
    nama_akun_media_sosial,
    nama_akun_ecommerce,
    bidang_usaha_yang_sedang_dijalankan_saat_ini_cth_pakaian_makana as bidang_usaha,
    lama_usaha,
    provinsi_usaha,
    pendapatan_bulanan,
    null as tergabung_komunitas_umkm,  -- not present in this source
    memiliki_nib,
    null as memiliki_sertifikasi_halal,  -- not present in this source
    timestamp,
    sheet_name,
    data_source,
    extracted_at

from {{ source('raw', 'leads_course_strategi_ads') }}

where email is not null
   or phone_number is not null
