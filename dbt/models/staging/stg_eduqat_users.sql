{{
    config(
        materialized='view'
    )
}}

select
    -- Primary identifiers
    id,
    user_id,
    subid,

    -- User info
    name,
    user_name,
    email,
    phone_number,
    phone_country,
    phone_country_calling_code,
    description,
    avatar_url,

    -- Account status
    role,
    status,

    -- Engagement metrics
    total_course,
    total_enrollment,

    -- Timestamps
    pre_signup_at,
    confirmed_at,
    last_loggin_at,
    created_at,

    -- Preserve JSONB for detailed analysis
    stripe_customer_ids,
    metadata,

    -- Ingestion metadata
    data_source,
    extracted_at

from {{ source('raw', 'eduqat_users') }}