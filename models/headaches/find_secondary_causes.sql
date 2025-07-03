WITH headache_visits AS (
    SELECT DISTINCT visit_occurrence_id
    FROM {{ ref('find_all_headaches') }}
),

diagnosis_visits AS (
    SELECT
        co.visit_occurrence_id,
        co.person_id,
        co.condition_concept_id,
        c.concept_name
    FROM {{ source('omop', 'condition_occurrence') }} co
    JOIN headache_visits hv ON co.visit_occurrence_id = hv.visit_occurrence_id
    JOIN {{ source('vocab', 'concept') }} c ON co.condition_concept_id = c.concept_id
    WHERE LOWER(c.concept_name) LIKE '%meningitis%'
       OR LOWER(c.concept_name) LIKE '%Subarachnoid%hemorrhage%'
       OR LOWER(c.concept_name) LIKE '%encephalitis%'
       OR LOWER(c.concept_name) LIKE '%intracranial%hypertension%'
       OR LOWER(c.concept_name) LIKE '%iih%'
       OR LOWER(c.concept_name) LIKE '%venous sinus thrombosis%'
)

SELECT *
FROM diagnosis_visits
