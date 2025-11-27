{{
    config(
        materialized='view'
    )
}}

select
    -- Primary identifiers
    id as course_id,
    slug,

    -- Course info
    name as course_name,
    description,
    type as course_type,
    status as course_status,

    -- Course metrics
    duration as duration_seconds,
    total_student,
    rating,

    -- Author and relationships
    author,
    parent as parent_course_id,

    -- Settings
    timezone,
    progress_status,

    -- Timestamps
    start_date,
    end_date,
    published_at,

    -- Preserve JSONB for detailed analysis
    language_codes,
    categories,
    educators,
    images,
    prices,
    tags,
    metadata,

    -- Ingestion metadata
    data_source,
    extracted_at

from {{ source('raw', 'eduqat_courses') }}
