{{
    config(
        materialized='view'
    )
}}

select
    id,
    email,
    created_at,
    updated_at,
    role,
    eduqat_user_id,
    name,
    mobile,
    mobile_verified,
    mobile_verified_at,
    avatar_url,
    birth_date,
    address,
    city,
    province,
    postal_code,
    language_preference,
    ai_tone_preference,
    interests,
    level,
    voice_preference,
    latitude,
    longitude,
    segment,
    business_name,
    data_source,
    extracted_at

from {{ source('raw', 'users') }}
