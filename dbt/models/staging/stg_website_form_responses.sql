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
    null as nama_akun_ecommerce,  -- not present in this source
    bidang_usaha,
    lama_usaha,
    provinsi_usaha,
    pendapatan_bulanan,
    null as tergabung_komunitas_umkm,  -- not present in this source
    null as memiliki_nib,  -- not present in this source
    null as memiliki_sertifikasi_halal,  -- not present in this source
    timestamp,
    sheet_name,
    data_source,
    extracted_at

from {{ source('raw', 'website_form_responses') }}

where email is not null
   or phone_number is not null
