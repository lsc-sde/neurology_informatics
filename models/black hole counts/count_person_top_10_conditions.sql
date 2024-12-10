WITH cohort_conditions AS (
  SELECT
    co.condition_concept_id,
    COUNT(DISTINCT co.person_id) AS patient_count
  FROM
    {{ source('omop', 'condition_occurrence') }} AS co
  INNER JOIN
    {{ ref('cohort') }} AS c
    ON
      co.person_id = c.person_id
  WHERE
    co.condition_start_date >= {{ var('clinic_visit_start_date') }}
    AND co.condition_start_date <= {{ var('clinic_visit_end_date') }}
  GROUP BY
    co.condition_concept_id
),

top_conditions AS (
  SELECT TOP 10
    condition_concept_id,
    patient_count
  FROM
    cohort_conditions
  ORDER BY
    patient_count DESC
)

SELECT
  t.condition_concept_id,
  t.patient_count,
  c.concept_name
FROM
  top_conditions AS t
LEFT JOIN
  {{ source('vocab', 'concept') }} AS c
  ON
    t.condition_concept_id = c.concept_id;
