-- This is a jinja ref that sets the materialization to incremental
-- The 'fail' instruction is to catch potential errors if something changes upstream
{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}
WITH src_reviews AS (
    SELECT * FROM {{ ref('src_reviews') }}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} as review_id,
    * 
FROM src_reviews
WHERE review_text IS NOT NULL
-- How does it know what records to (incrementally) add?
-- With a jinja if-statement
{% if is_incremental()%} -- boolean
    AND review_date > (SELECT MAX(review_date) FROM {{ this }}) -- refers to itself, 'fct_reviews.sql'
{% endif %}