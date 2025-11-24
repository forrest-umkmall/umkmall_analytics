{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'leads_ads_community') }}
    where email is not null or phone_number is not null
),

normalized as (
    select
        {{ normalize_email('email') }} as email,
        {{ normalize_phone_number('phone_number') }} as phone_number,

        nama_usaha,
        nama_pemilik_usaha,
        nama_akun_media_sosial,
        nama_akun_ecommerce,
        bidang_usaha_yang_sedang_dijalankan_saat_ini_cth_pakaian_makana as bidang_usaha,
        {{ normalize_business_age('lama_usaha') }} as lama_usaha,
        provinsi_usaha,
        {{ normalize_income('pendapatan_bulanan') }} as pendapatan_bulanan,
        saat_ini_sudah_memiliki_berapa_karyawan as jumlah_karyawan,
        tergabung_komunitas_umkm,
        memiliki_nib,
        memiliki_sertifikasi_halal,
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
