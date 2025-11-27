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
    e.enrollment_id,
    e.uid,
    e.course_id,
    e.price_id,
    e.schedule_id,
    e.order_uid,

    -- User info (for joining to mart_contact)
    e.user_id,
    e.user_email as email,
    e.user_phone as phone_number,
    e.user_name,

    -- Course info
    c.course_name,

    -- Enrollment timing
    e.enrollment_started_at,
    e.completed_at,
    e.expires_at,
    e.created_at as enrolled_at,

    -- Progress metrics
    e.learning_progress,
    e.learning_time as learning_time_seconds,
    e.total_tracked_time as total_tracked_time_seconds,
    e.last_tracked_at,

    -- Enrollment metadata
    e.enrollment_type,
    e.timezone,

    -- Prakerja program fields
    e.prakerja_id,
    e.is_prakerja_user,
    e.prakerja_redeem_code,
    e.prakerja_redeem_at,

    -- Certificate info
    e.certificate_count,

    -- Preserve nested JSON for detailed analysis
    e.certificates,
    e.completions,
    e.order_data,

    -- Metadata
    e.data_source,
    e.extracted_at

from {{ ref('stg_eduqat_enrollments') }} e
left join {{ ref('stg_eduqat_courses') }} c
    on e.course_id = c.course_id