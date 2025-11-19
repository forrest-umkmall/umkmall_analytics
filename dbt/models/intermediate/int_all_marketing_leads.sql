{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

/*
Union all marketing lead sources into a single table.
This combines all Google Sheets lead sources with consistent columns.
*/

with leads_ads_community as (
    select * from {{ ref('stg_leads_ads_community') }}
),

website_form_responses as (
    select * from {{ ref('stg_website_form_responses') }}
),

leads_course_strategi_ads as (
    select * from {{ ref('stg_leads_course_strategi_ads') }}
),

branding_level_up as (
    select * from {{ ref('stg_branding_level_up') }}
),

unioned as (
    select * from leads_ads_community
    union all
    select * from website_form_responses
    union all
    select * from leads_course_strategi_ads
    union all
    select * from branding_level_up
)

select * from unioned
