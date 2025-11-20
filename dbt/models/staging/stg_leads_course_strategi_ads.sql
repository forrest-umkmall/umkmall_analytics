{{
    config(
        materialized='view'
    )
}}

select
    *
from {{ source('raw', 'leads_course_strategi_ads') }}

where (email is not null and email != '')
   or (phone_number is not null and phone_number != '')
