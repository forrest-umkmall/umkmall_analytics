{{
    config(
        materialized='view'
    )
}}

select
    -- Primary identifiers
    id as enrollment_id,
    uid,
    user_id,
    course_id,
    price_id,
    schedule_id,
    order_uid,

    -- User info extracted from JSONB
    user_data->>'name' as user_name,
    user_data->>'email' as user_email,
    user_data->>'phone_number' as user_phone,
    user_data->>'role' as user_role,
    user_data->>'status' as user_status,
    (user_data->>'created_at')::timestamp as user_created_at,

    -- Enrollment timing
    (metadata->>'started_at')::timestamp as enrollment_started_at,
    completed_at as enrollment_completed_at,
    expires_at as enrollment_expires_at,
    created_at as enrollment_created_at,

    -- Progress metrics
    learning_progress,
    learning_time,
    (metadata->'tracked_time'->>'total')::float as total_tracked_time,
    (metadata->'tracked_time'->>'last_tracked_at')::timestamp as last_tracked_at,

    -- Enrollment metadata
    metadata->>'type' as enrollment_type,
    timezone,
    order_data,

    -- Prakerja program fields (Indonesian government program)
    metadata->>'prakerja_id' as prakerja_id,
    (metadata->>'is_prakerja_user')::boolean as is_prakerja_user,
    metadata->>'prakerja_redeem_code' as prakerja_redeem_code,
    (metadata->>'prakerja_redeem_at')::timestamp as prakerja_redeem_at,

    -- Certificate info (count of certificates)
    jsonb_array_length(certificates) as certificate_count,

    -- Preserve full nested JSON for detailed analysis
    user_data,
    metadata,
    certificates,
    user_groups,
    user_group_admins,

    -- Metadata
    data_source,
    extracted_at

from {{ source('raw', 'eduqat_enrollments') }}