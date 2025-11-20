{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
Marketing data - deduplicated marketing leads only.
This includes all leads from Google Sheets sources, deduplicated by email/phone.
Equivalent to the "Marketing Data" Google Sheets output from the old pipeline.
*/

with marketing_leads as (
    select * from {{ ref('int_all_marketing_leads') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by
                coalesce(email, ''),
                coalesce(phone_number, '')
            order by extracted_at::timestamp desc nulls last
        ) as row_num
    from marketing_leads
    where (email is not null and email != '')
       or (phone_number is not null and phone_number != '')
)

select
    email,
    phone_number,
    data_source,
    extracted_at
from deduplicated
where row_num = 1
