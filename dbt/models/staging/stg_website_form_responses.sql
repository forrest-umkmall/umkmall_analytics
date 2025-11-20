{{
    config(
        materialized='view'
    )
}}

select
    *
from {{ source('raw', 'website_form_responses') }}

where (email is not null and email != '')
   or (phone_number is not null and phone_number != '')
