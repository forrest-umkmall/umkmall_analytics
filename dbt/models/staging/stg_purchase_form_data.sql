{{
    config(
        materialized='view'
    )
}}

select
    customer_first_name,
    customer_last_name,
    email,
    phone_number,
    paid_at,
    data_source,
    extracted_at

from {{ source('raw', 'purchase_form_data') }}

-- Filter out records without email or phone
where email is not null
   or phone_number is not null
