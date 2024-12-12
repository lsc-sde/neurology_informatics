WITH cohort_measurements AS (
  SELECT
    m.measurement_concept_id,
    COUNT(DISTINCT m.person_id) AS patient_count
  FROM
    {{ source('omop', 'measurement') }} AS m
  INNER JOIN
    {{ ref('cohort') }} AS c
    ON
      m.person_id = c.person_id
  WHERE
    m.measurement_date >= {{ var('clinic_visit_start_date') }}
    AND m.measurement_date <= {{ var('clinic_visit_end_date') }}
  GROUP BY
    m.measurement_concept_id
),

top_measurements AS (
  SELECT TOP 10
    measurement_concept_id,
    patient_count
  FROM
    cohort_measurements
  ORDER BY
    patient_count DESC
)

SELECT
  t.measurement_concept_id,
  t.patient_count,
  c.concept_name
FROM
  top_measurements AS t
LEFT JOIN
  {{ source('vocab', 'concept') }} AS c
  ON
    t.measurement_concept_id = c.concept_id;