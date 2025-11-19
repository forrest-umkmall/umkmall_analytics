-- Example staging model
-- Uncomment and customize when you have raw data

-- {{ config(materialized='view') }}

-- select
--     id,
--     created_at,
--     updated_at
-- from {{ source('raw', 'example_table') }}
