WITH cohort_observations AS (
  SELECT
    o.observation_concept_id,
    COUNT(DISTINCT o.person_id) AS patient_count
  FROM
    {{ source('omop', 'observation') }} AS o
  INNER JOIN
    {{ ref('cohort') }} AS c
    ON
      o.person_id = c.person_id
  WHERE
    o.observation_date >= {{ var('clinic_visit_start_date') }}
    AND o.observation_date <= {{ var('clinic_visit_end_date') }}
  GROUP BY
    o.observation_concept_id
),

top_observations AS (
  SELECT TOP 10
    observation_concept_id,
    patient_count
  FROM
    cohort_observations
  ORDER BY
    patient_count DESC
)

SELECT
  t.observation_concept_id,
  t.patient_count,
  c.concept_name
FROM
  top_observations AS t
LEFT JOIN
  {{ source('vocab', 'concept') }} AS c
  ON
    t.observation_concept_id = c.concept_id;