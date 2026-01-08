{{
    config(
        materialized='view'
    )
}}

select
    id,
    session_id,
    role,
    content,
    message_order,
    created_at,
    data_source,
    extracted_at

from {{ source('raw', 'ai_chat_messages') }}
