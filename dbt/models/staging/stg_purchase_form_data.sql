{{
    config(
        materialized='view'
    )
}}

select
    *
from {{ source('raw', 'purchase_form_data') }}

where (email is not null and email != '')
   or (phone_number is not null and phone_number != '')
