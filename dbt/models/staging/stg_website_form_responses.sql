{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'website_form_responses') }}
    where email is not null or phone_number is not null
),

normalized as (
    select
        -- Normalize email: lowercase and trim
        case
            when email is not null and email != ''
                 and email like '%@%.%'
            then lower(trim(email))
            else null
        end as email,

        -- Normalize Indonesian phone numbers to +62XXXXXXXXXX format
        case
            when phone_number is null or trim(phone_number) = '' then null
            else '+62' ||
                case
                    -- Remove +62 prefix
                    when regexp_replace(phone_number, '[^0-9+]', '', 'g') like '+62%'
                    then substring(regexp_replace(phone_number, '[^0-9]', '', 'g') from 3)
                    -- Remove 62 prefix
                    when regexp_replace(phone_number, '[^0-9]', '', 'g') like '62%'
                    then substring(regexp_replace(phone_number, '[^0-9]', '', 'g') from 3)
                    -- Remove leading 0
                    when regexp_replace(phone_number, '[^0-9]', '', 'g') like '0%'
                    then substring(regexp_replace(phone_number, '[^0-9]', '', 'g') from 2)
                    -- Already clean
                    else regexp_replace(phone_number, '[^0-9]', '', 'g')
                end
        end as phone_number,

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
    from source
),

with_composite_key as (
    select
        *,
        -- Create composite key for deduplication
        coalesce(email, '') || '|' || coalesce(phone_number, '') as composite_key
    from normalized
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by composite_key
            order by timestamp desc nulls last, extracted_at desc
        ) as row_num
    from with_composite_key
)

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
    extracted_at,
    composite_key
from deduplicated
where row_num = 1
