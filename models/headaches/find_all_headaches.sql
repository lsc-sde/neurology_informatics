SELECT 
    v.person_id, 
    v.visit_start_date, 
    v.visit_occurrence_id,
    v.visit_concept_id, 
    c.condition_concept_id,
    c.condition_source_value, 
    cn.concept_name AS diagnosis
FROM {{ source('omop', 'visit_occurrence') }} v
JOIN {{ source('omop', 'condition_occurrence') }} c 
    ON v.visit_occurrence_id = c.visit_occurrence_id
JOIN {{ source('vocab', 'concept') }} cn 
    ON c.condition_concept_id = cn.concept_id
JOIN {{ ref('unique_headache_codes') }} hmc
    ON c.condition_concept_id = hmc.condition_concept_id
WHERE v.visit_concept_id IN (9201, 9203, 262)
  AND v.visit_start_date BETWEEN '2021-01-01' AND '2023-12-31';
