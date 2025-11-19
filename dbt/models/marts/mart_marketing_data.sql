{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
Marketing data - deduplicated marketing leads only.
This includes all leads from Google Sheets sources, deduplicated by email/phone.
Equivalent to the "Marketing Data" Google Sheets output from the old pipeline.
*/

with marketing_leads as (
    select * from {{ ref('int_all_marketing_leads') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by
                coalesce(email, ''),
                coalesce(phone_number, '')
            order by extracted_at desc
        ) as row_num
    from marketing_leads
    where email is not null or phone_number is not null
)

select
    email,
    phone_number,
    nama_usaha,
    nama_pemilik_usaha,
    provinsi_usaha,
    bidang_usaha,
    nama_akun_media_sosial,
    nama_akun_ecommerce,
    lama_usaha,
    pendapatan_bulanan,
    tergabung_komunitas_umkm,
    memiliki_nib,
    memiliki_sertifikasi_halal,
    timestamp,
    sheet_name,
    data_source,
    extracted_at

from deduplicated
where row_num = 1
