{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
Unified contact mart - one row per email-phone pair.
Integrates data from marketing leads, purchases, and Eduqat.
*/

with marketing_leads as (
    select
        *,
        row_number() over (
            partition by
                coalesce(email, ''),
                coalesce(phone_number, '')
            order by extracted_at desc
        ) as row_num
    from {{ ref('int_all_marketing_leads') }}
    where email is not null or phone_number is not null
),

-- Deduplicated marketing leads (most recent per email-phone)
marketing_deduped as (
    select * from marketing_leads where row_num = 1
),

-- Purchase data aggregated per email
purchases as (
    select
        email,
        min(paid_at) as first_purchased_at,
        max(paid_at) as last_purchased_at,
        count(*) as total_purchases,
        -- Get customer name from most recent purchase
        (array_agg(customer_name order by paid_at desc))[1] as customer_name
    from {{ ref('stg_purchase_form_data') }}
    where email is not null
    group by email
),

-- Eduqat users (one row per email already)
eduqat_users as (
    select
        email,
        user_id as eduqat_user_id,
        name as eduqat_name,
        role as eduqat_role,
        status as eduqat_status,
        total_enrollment as eduqat_total_enrollments,
        last_loggin_at as eduqat_last_login_at,
        created_at as eduqat_created_at
    from {{ ref('stg_eduqat_users') }}
    where email is not null
),

-- Eduqat enrollments aggregated per email
eduqat_enrollments_agg as (
    select
        user_email as email,
        count(*) as enrolled_courses_count,
        sum(learning_time) as total_learning_time_seconds,
        avg(learning_progress) as avg_learning_progress,
        sum(certificate_count) as total_certificates,
        count(*) filter (where is_prakerja_user = true) as prakerja_enrollments_count,
        max(enrollment_created_at) as last_enrollment_at
    from {{ ref('stg_eduqat_enrollments') }}
    where user_email is not null
    group by user_email
),

-- Combine all sources using email as primary key
-- Start with marketing as base, then full outer join others
combined as (
    select
        -- Use email from any source
        coalesce(m.email, p.email, eu.email, ea.email) as email,
        m.phone_number,

        -- Full name: prefer purchase, then eduqat, then marketing
        coalesce(p.customer_name, eu.eduqat_name, m.nama_pemilik_usaha) as full_name,

        -- Purchase data
        p.first_purchased_at,
        p.last_purchased_at,
        p.total_purchases,

        -- Eduqat user data
        eu.eduqat_user_id,
        eu.eduqat_role,
        eu.eduqat_status,
        eu.eduqat_total_enrollments,
        eu.eduqat_last_login_at,
        eu.eduqat_created_at,

        -- Eduqat created at marketing week (ISO week: Monday-Sunday)
        extract(week from eu.eduqat_created_at)::int as eduqat_created_at_marketing_week,
        extract(isoyear from eu.eduqat_created_at)::int as eduqat_created_at_marketing_year,

        -- Eduqat enrollment aggregates
        ea.enrolled_courses_count,
        ea.total_learning_time_seconds,
        ea.avg_learning_progress,
        ea.total_certificates,
        ea.prakerja_enrollments_count,
        ea.last_enrollment_at,

        -- Marketing geography & business info
        m.provinsi_usaha,
        m.kota_kabupaten,
        m.bidang_usaha,
        m.pendapatan_bulanan,
        m.lama_usaha,
        m.jumlah_karyawan,

        -- Business details
        m.nama_usaha,
        m.nama_akun_media_sosial,
        m.nama_akun_ecommerce,

        -- Marketing questions
        m.tergabung_komunitas_umkm,
        m.memiliki_nib,
        m.memiliki_sertifikasi_halal,

        -- Source tracking
        m.data_source as marketing_data_source,
        m.sheet_name as marketing_sheet_name,
        m.timestamp as marketing_timestamp,

        -- Metadata
        greatest(
            coalesce(m.extracted_at::timestamp, '1970-01-01'::timestamp),
            coalesce(p.last_purchased_at, '1970-01-01'::timestamp),
            coalesce(eu.eduqat_created_at, '1970-01-01'::timestamp),
            coalesce(ea.last_enrollment_at, '1970-01-01'::timestamp)
        ) as last_activity_at

    from marketing_deduped m
    full outer join purchases p on m.email = p.email
    full outer join eduqat_users eu on coalesce(m.email, p.email) = eu.email
    full outer join eduqat_enrollments_agg ea on coalesce(m.email, p.email, eu.email) = ea.email
)

select
    -- Contact key
    coalesce(email, '') || '|' || coalesce(phone_number, '') as contact_key,

    -- All fields from combined
    *

from combined
where email is not null or phone_number is not null