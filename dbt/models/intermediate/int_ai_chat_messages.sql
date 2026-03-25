{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

/*
Join AI chat messages with sessions and users to include user information.
This allows analysis of messages by user.
*/

with messages as (
    select * from {{ ref('stg_ai_chat_messages') }}
),

sessions as (
    select * from {{ ref('stg_ai_chat_sessions') }}
),

users as (
    select * from {{ ref('stg_users') }}
)

select
    m.id as message_id,
    m.session_id,
    s.user_id,
    u.email as user_email,
    u.mobile as user_mobile,
    s.guest_session_id,
    s.title as session_title,
    m.role,
    m.content,
    m.message_order,
    m.created_at as message_created_at,
    s.created_at as session_created_at,
    s.updated_at as session_updated_at

from messages m
left join sessions s on m.session_id = s.id
left join users u on s.user_id = u.id
