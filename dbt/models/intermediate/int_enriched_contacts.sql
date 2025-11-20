{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

/*
Merge marketing leads with purchase data to create enriched contact records.
Uses outer join on email and phone_number to capture all contacts.
For overlapping columns, prefer values from marketing leads (first source).
*/

with marketing_leads as (
    select * from {{ ref('int_all_marketing_leads') }}
),

purchase_data as (
    select * from {{ ref('stg_purchase_form_data') }}
),

merged as (
    select
        -- Contact identifiers (deduplicated)
        coalesce(m.email, p.email) as email,
        coalesce(m.phone_number, p.phone_number) as phone_number,

        -- Business information (prefer marketing leads)
        coalesce(m.nama_usaha, null) as nama_usaha,
        coalesce(m.nama_pemilik_usaha, null) as nama_pemilik_usaha,
        coalesce(m.provinsi_usaha, null) as provinsi_usaha,
        coalesce(m.bidang_usaha, null) as bidang_usaha,

        -- Marketing-specific fields
        m.nama_akun_media_sosial,
        m.nama_akun_ecommerce,
        m.lama_usaha,
        m.pendapatan_bulanan,
        m.tergabung_komunitas_umkm,
        m.memiliki_nib,
        m.memiliki_sertifikasi_halal,

        -- Purchase-specific fields
        p.customer_first_name,
        p.customer_last_name,

        -- Timestamps (keep separate with source prefix)
        m.timestamp as marketing_timestamp,
        p.paid_at as purchase_paid_at,

        -- Metadata
        m.sheet_name as marketing_sheet_name,
        m.data_source as marketing_data_source,
        p.data_source as purchase_data_source,

        -- Most recent extraction time
        greatest(
            coalesce(m.extracted_at, '1970-01-01'::timestamp),
            coalesce(p.extracted_at, '1970-01-01'::timestamp)
        ) as extracted_at

    from marketing_leads m
    full outer join purchase_data p
        on (m.email = p.email or m.phone_number = p.phone_number)
)

select * from merged
