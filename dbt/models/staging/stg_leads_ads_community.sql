{{
    config(
        materialized='view'
    )
}}

select
    *
from {{ source('raw', 'leads_ads_community') }}

-- Filter out records without email or phone
where (email is not null and email != '')
   or (phone_number is not null and phone_number != '')
