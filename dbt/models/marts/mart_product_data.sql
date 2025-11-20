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
    customer_first_name,
    customer_last_name,
    email,
    phone_number,
    paid_at,
    data_source,
    extracted_at

from {{ ref('stg_purchase_form_data') }}
