{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
Enrollment materials mart - one row per material per enrollment.
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

    -- Material info
    m.material_id,
    m.material_type,
    m.is_required as material_is_required,
    m.started_at as material_started_at,
    m.completed_at as material_completed_at,
    m.tracked_time_seconds as material_tracked_time_seconds,

    -- AI Tutor fields
    m.ai_tutor_status,
    m.conversation_id,
    m.conversation_started_at,
    m.conversation_ended_at,
    m.submission_count,

    -- Self-assessment fields
    m.self_assessment_id,
    m.self_assessment_library_id,
    m.require_feedback,
    m.require_file_upload,
    m.require_educator_approval,

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
left join {{ ref('stg_enrollment_materials') }} m
    on e.enrollment_id = m.enrollment_id