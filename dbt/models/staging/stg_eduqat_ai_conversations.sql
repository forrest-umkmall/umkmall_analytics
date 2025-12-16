{{
    config(
        materialized='view'
    )
}}

with conversations as (
    select * from {{ source('raw', 'eduqat_ai_conversations') }}
),

messages_unnested as (
    select
        c.id as submission_id,
        c.conversation_id,
        c.user_id,
        c.enrollment_id,
        c.course_id,
        c.material_id,
        c.status as conversation_status,
        c.score,
        c.content,
        c.audio_url,
        c.x_site_id,
        c.educator_id,
        c.created_at as conversation_created_at,
        c.updated_at as conversation_updated_at,
        c.data_source,
        c.extracted_at,
        -- Unnested message fields
        msg.value->>'session_id' as message_session_id,
        msg.value->>'sender' as message_sender,
        msg.value->>'value' as message_content,
        (msg.value->>'timestamp')::timestamp as message_timestamp,
        msg.ordinality as message_order
    from conversations c
    cross join lateral jsonb_array_elements(coalesce(c.messages, '[]'::jsonb)) with ordinality as msg(value, ordinality)
)

select
    -- Primary identifiers
    submission_id,
    conversation_id,
    user_id,
    enrollment_id,
    course_id,
    material_id,

    -- Conversation metadata
    conversation_status,
    score,
    content,
    audio_url,
    x_site_id,
    educator_id,
    conversation_created_at,
    conversation_updated_at,

    -- Message fields (one row per message)
    message_session_id,
    message_sender,
    message_content,
    message_timestamp,
    message_order,

    -- Metadata
    data_source,
    extracted_at

from messages_unnested
