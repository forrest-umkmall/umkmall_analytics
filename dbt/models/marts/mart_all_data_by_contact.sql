{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
All data by contact - deduplicated enriched contacts.
This combines marketing leads and purchase data, deduplicated by email/phone.
Equivalent to the "All Data By Contact" Google Sheets output from the old pipeline.
*/

with enriched_contacts as (
    select * from {{ ref('int_enriched_contacts') }}
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
    from enriched_contacts
    where (email is not null and email != '')
       or (phone_number is not null and phone_number != '')
)

select
    email,
    phone_number,
    customer_first_name,
    customer_last_name,
    purchase_paid_at,
    purchase_data_source,
    data_source,
    extracted_at
from deduplicated
where row_num = 1
