WITH cohort_drugs AS (
  SELECT
    d.drug_concept_id,
    COUNT(*) AS drug_count
  FROM
    {{ source('omop', 'drug_exposure') }} AS d
  INNER JOIN
    {{ ref('cohort') }} AS c
    ON
      d.person_id = c.person_id
  WHERE
    d.drug_exposure_start_date >= {{ var('clinic_visit_start_date') }}
    AND d.drug_exposure_start_date <= {{ var('clinic_visit_end_date') }}
  GROUP BY
    d.drug_concept_id
),

top_drugs AS (
  SELECT TOP 10
    drug_concept_id,
    drug_count
  FROM
    cohort_drugs
  ORDER BY
    drug_count DESC
)

SELECT
  t.drug_concept_id,
  t.drug_count,
  c.concept_name
FROM
  top_drugs AS t
LEFT JOIN
  {{ source('vocab','concept') }} AS c
  ON
    t.drug_concept_id = c.concept_id
