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
        {{ normalize_email('email') }} as email,
        {{ normalize_phone_number('phone_number') }} as phone_number,

        nama_usaha,
        nama_pemilik_usaha,
        nama_akun_media_sosial,
        null as nama_akun_ecommerce,  -- not present in this source
        bidang_usaha,
        {{ normalize_business_age('lama_usaha') }} as lama_usaha,
        provinsi_usaha,
        nullif(trim(di_kota_mana_anda_menjalankan_usaha_anda), '') as kota_kabupaten,
        {{ normalize_income('pendapatan_bulanan') }} as pendapatan_bulanan,
        {{ normalize_employee_count('nullif(trim(berapa_jumlah_karyawan_yang_anda_miliki), \'\')') }} as jumlah_karyawan,
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
    kota_kabupaten,
    pendapatan_bulanan,
    jumlah_karyawan,
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
