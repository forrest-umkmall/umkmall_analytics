{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
Enrollment detail mart - one row per Eduqat course enrollment.
Can be joined to mart_contact via email for full contact context.
*/

select
    -- Enrollment identifiers
    enrollment_id,
    uid,
    course_id,
    price_id,
    schedule_id,
    order_uid,

    -- User info (for joining to mart_contact)
    user_id,
    user_email as email,
    user_phone as phone_number,
    user_name,

    -- Enrollment timing
    enrollment_started_at,
    completed_at,
    expires_at,
    created_at as enrolled_at,

    -- Progress metrics
    learning_progress,
    learning_time as learning_time_seconds,
    total_tracked_time as total_tracked_time_seconds,
    last_tracked_at,

    -- Enrollment metadata
    enrollment_type,
    timezone,

    -- Prakerja program fields
    prakerja_id,
    is_prakerja_user,
    prakerja_redeem_code,
    prakerja_redeem_at,

    -- Certificate info
    certificate_count,

    -- Preserve nested JSON for detailed analysis
    certificates,
    completions,
    order_data,

    -- Metadata
    data_source,
    extracted_at

from {{ ref('stg_eduqat_enrollments') }}