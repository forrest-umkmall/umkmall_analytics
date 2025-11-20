{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

/*
Merge marketing leads with purchase data to create enriched contact records.
Uses LEFT JOIN on email to capture contacts with purchase history.
*/

with marketing_leads as (
    select * from {{ ref('int_all_marketing_leads') }}
),

purchase_data as (
    select * from {{ ref('stg_purchase_form_data') }}
),

-- Join marketing leads with purchase data on email
merged as (
    select
        m.*,
        p.customer_first_name,
        p.customer_last_name,
        p.paid_at as purchase_paid_at,
        p.data_source as purchase_data_source

    from marketing_leads m
    left join purchase_data p
        on m.email = p.email
        and m.email is not null
        and m.email != ''
)

select * from merged
