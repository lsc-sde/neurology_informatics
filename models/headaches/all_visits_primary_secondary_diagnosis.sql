SELECT
    vo.person_id,
    vo.visit_occurrence_id,
    vo.visit_start_date,
    vo.visit_concept_id,
    co.condition_status_concept_id,
    c.concept_name AS diagnosis
FROM {{ source('omop', 'condition_occurrence') }} co
JOIN {{ source('vocab', 'concept') }} c
  ON co.condition_concept_id = c.concept_id
JOIN {{ source('omop', 'visit_occurrence') }} vo
  ON co.visit_occurrence_id = vo.visit_occurrence_id
WHERE co.condition_status_concept_id IN (32909, 32903)
  AND vo.visit_start_date BETWEEN '2021-01-01' AND '2023-12-31'
