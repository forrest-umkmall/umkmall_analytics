{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
Enrollment mart - one row per enrollment.
Can be joined to mart_contact via email for full contact context.
Can be joined to mart_materials via enrollment_id for material details.
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
    e.enrollment_completed_at,
    e.enrollment_expires_at,
    e.enrollment_created_at as enrolled_at,

    -- Marketing week metrics (ISO week: Monday-Sunday)
    extract(week from e.enrollment_started_at)::int as started_at_marketing_week,
    extract(isoyear from e.enrollment_started_at)::int as started_at_marketing_year,
    extract(week from e.enrollment_completed_at)::int as completed_at_marketing_week,
    extract(isoyear from e.enrollment_completed_at)::int as completed_at_marketing_year,

    -- Days to complete (null if not yet completed)
    case
        when e.enrollment_completed_at is not null and e.enrollment_started_at is not null
        then extract(day from (e.enrollment_completed_at - e.enrollment_started_at))::int
    end as days_to_complete,

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
    e.order_data,

    -- Metadata
    e.data_source,
    e.extracted_at

from {{ ref('stg_eduqat_enrollments') }} e
left join {{ ref('stg_eduqat_courses') }} c
    on e.course_id = c.course_id
