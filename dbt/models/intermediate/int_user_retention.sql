{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

/*
Per-user retention flags based on whether the user sent at least one message
after N days from account creation. Excludes internal users and AI responses.
*/

with user_messages as (
    select
        user_id,
        user_email,
        user_mobile,
        user_created_at,
        message_created_at
    from {{ ref('int_ai_chat_messages') }}
    where role = 'user'
      and is_internal_user = false
      and user_id is not null
)

select
    user_id,
    user_email,
    user_mobile,
    user_created_at,
    min(message_created_at) as first_message_at,
    max(message_created_at) as last_message_at,
    count(*) as total_messages,
    count(distinct date_trunc('day', message_created_at)) as active_days,
    bool_or(message_created_at >= user_created_at + interval '7 days')::int as retained_7d,
    bool_or(message_created_at >= user_created_at + interval '14 days')::int as retained_14d,
    bool_or(message_created_at >= user_created_at + interval '30 days')::int as retained_30d,
    bool_or(message_created_at >= user_created_at + interval '60 days')::int as retained_60d,
    bool_or(message_created_at >= user_created_at + interval '90 days')::int as retained_90d
from user_messages
group by user_id, user_email, user_mobile, user_created_at
