{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
Materials mart - one row per material per enrollment.
Can be joined to mart_enrollment via enrollment_id for enrollment context.
Can be joined to mart_contact via email for full contact context.
*/

select
    -- Identifiers
    m.enrollment_id,
    m.user_id,
    m.course_id,
    m.material_id,

    -- User info (for joining to mart_contact)
    e.user_email as email,
    e.user_phone as phone_number,
    e.user_name,

    -- Course info
    c.course_name,

    -- Material info
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

    -- Metadata
    e.data_source,
    e.extracted_at

from {{ ref('stg_enrollment_materials') }} m
left join {{ ref('stg_eduqat_enrollments') }} e
    on m.enrollment_id = e.enrollment_id
left join {{ ref('stg_eduqat_courses') }} c
    on m.course_id = c.course_id
