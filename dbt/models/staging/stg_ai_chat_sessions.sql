{{
    config(
        materialized='view'
    )
}}

select
    id,
    user_id,
    guest_session_id,
    title,
    created_at,
    updated_at,
    data_source,
    extracted_at

from {{ source('raw', 'ai_chat_sessions') }}
