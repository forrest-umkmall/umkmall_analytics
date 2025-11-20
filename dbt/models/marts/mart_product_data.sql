{{
    config(
        materialized='table',
        schema='marts'
    )
}}

/*
Product/purchase data - customer purchases from the product database.
This includes all purchase transactions without deduplication.
Equivalent to the "Product Data" Google Sheets output from the old pipeline.
*/

select
    *
from {{ ref('stg_purchase_form_data') }}
