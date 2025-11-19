-- Example mart model
-- Business logic and final analytics tables
-- Uncomment and customize when you have staging models

-- {{ config(materialized='table') }}

-- select
--     id,
--     created_at,
--     updated_at
-- from {{ ref('stg_example') }}
