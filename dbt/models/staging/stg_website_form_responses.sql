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
    bidang_usaha,
    lama_usaha,
    provinsi_usaha,
    pendapatan_bulanan,
    tergabung_komunitas_umkm,
    memiliki_nib,
    memiliki_sertifikasi_halal,
    timestamp,
    sheet_name,
    data_source,
    extracted_at

from {{ source('raw', 'website_form_responses') }}

where email is not null
   or phone_number is not null
