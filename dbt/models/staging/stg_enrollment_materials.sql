{{
    config(
        materialized='view'
    )
}}

with enrollment_materials as (
    select
        id as enrollment_id,
        user_id,
        course_id,
        jsonb_each(completions->'materials') as material_record
    from {{ source('raw', 'eduqat_enrollments') }}
    where completions->'materials' is not null
      and jsonb_typeof(completions->'materials') = 'object'
)

select
    -- Identifiers
    enrollment_id,
    user_id,
    course_id,
    (material_record).key::integer as material_id,

    -- Material type and status
    (material_record).value->>'type' as material_type,
    ((material_record).value->>'is_required')::boolean as is_required,

    -- Timing
    ((material_record).value->>'started_at')::timestamp as started_at,
    ((material_record).value->>'completed_at')::timestamp as completed_at,
    ((material_record).value->>'tracked_time')::float as tracked_time_seconds,

    -- AI Tutor specific fields
    (material_record).value->>'status' as ai_tutor_status,
    (material_record).value->>'conversation_id' as conversation_id,
    ((material_record).value->>'start_conversation')::timestamp as conversation_started_at,
    ((material_record).value->>'end_conversation')::timestamp as conversation_ended_at,
    ((material_record).value->>'count_submit')::integer as submission_count,

    -- Self-assessment specific fields
    ((material_record).value->>'self_assessment_id')::integer as self_assessment_id,
    (material_record).value->>'self_assessment_library_id' as self_assessment_library_id,
    ((material_record).value->>'require_feedback')::boolean as require_feedback,
    ((material_record).value->>'require_file_upload')::boolean as require_file_upload,
    ((material_record).value->>'require_educator_approval')::integer as require_educator_approval

from enrollment_materials
