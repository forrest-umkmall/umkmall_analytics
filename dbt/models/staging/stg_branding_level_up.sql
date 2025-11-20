{{
    config(
        materialized='view'
    )
}}

select
    *
from {{ source('raw', 'branding_level_up') }}

where (email is not null and email != '')
   or (phone_number is not null and phone_number != '')
