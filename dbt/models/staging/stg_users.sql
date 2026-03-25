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
    mobile in (
        '6285121391537', -- Forrest
        '62895325161356', -- Diva
        '6285782707018', -- Jeremy
        '6285961130185', -- Jeremy 2
        '6282122906314', -- Leo
        '6281296466046', -- Agil
        '6289658090398' -- Nurul
    ) as is_internal_user,
    data_source,
    extracted_at

from {{ source('raw', 'users') }}
