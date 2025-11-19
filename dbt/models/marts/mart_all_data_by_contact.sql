{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
All data by contact - deduplicated enriched contacts.
This combines marketing leads and purchase data, deduplicated by email/phone.
Equivalent to the "All Data By Contact" Google Sheets output from the old pipeline.
*/

with enriched_contacts as (
    select * from {{ ref('int_enriched_contacts') }}
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
    from enriched_contacts
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
    customer_first_name,
    customer_last_name,
    marketing_timestamp,
    purchase_paid_at,
    marketing_sheet_name,
    marketing_data_source,
    purchase_data_source,
    extracted_at

from deduplicated
where row_num = 1
