{{
    config(
        materialized='view'
    )
}}

/*
    Staged survey results from Eduqat API.

    This model unnests the survey elements array to create one row per question per survey response.
    Each row contains the question, response type, and the actual responses array.

    Source: raw.eduqat_survey_results (fetched via /manage/v2/admin/enrollments/{enrollment_id}/survey/{material_id})
*/

with survey_responses as (
    select
        enrollment_id,
        material_id,
        survey_id,
        course_id,
        user_id,
        survey_type,
        survey_title,
        completed_at,
        elements,
        data_source,
        extracted_at
    from {{ source('raw', 'eduqat_survey_results') }}
),

-- Unnest elements array to get one row per question
unnested_elements as (
    select
        sr.enrollment_id,
        sr.material_id,
        sr.survey_id,
        sr.course_id,
        sr.user_id,
        sr.survey_type,
        sr.survey_title,
        sr.completed_at,

        -- Question metadata
        (elem.ordinality - 1) as question_index,
        elem.value->>'question' as question_text,
        elem.value->>'description' as question_description,
        elem.value->>'type' as question_type,

        -- Responses array (kept as JSONB for flexibility - different types have different structures)
        elem.value->'responses' as responses,

        sr.data_source,
        sr.extracted_at
    from survey_responses sr,
         jsonb_array_elements(sr.elements) with ordinality as elem(value, ordinality)
)

select
    -- Primary key components
    enrollment_id,
    material_id,
    question_index,

    -- Survey context
    survey_id,
    course_id,
    user_id,
    survey_type,
    survey_title,

    -- Question details
    question_text,
    question_description,
    question_type,

    -- Response data
    responses,

    -- Completion info
    completed_at,

    -- Metadata
    data_source,
    extracted_at
from unnested_elements
