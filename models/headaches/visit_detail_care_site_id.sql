SELECT 
    vd.person_id, 
    v.visit_occurrence_id,
    vd.visit_detail_id,
    vd.visit_detail_start_date,
    vd.visit_detail_end_date,
    vd.visit_detail_source_value,
    v.visit_concept_id, 
    c.condition_concept_id,
    c.condition_source_value, 
    cn.concept_name AS diagnosis,
    vd.care_site_id,
    cs.care_site_name
FROM {{ source('omop', 'visit_detail') }} vd
JOIN {{ source('omop', 'visit_occurrence') }} v
    ON vd.visit_occurrence_id = v.visit_occurrence_id
JOIN {{ source('omop', 'condition_occurrence') }} c 
    ON v.visit_occurrence_id = c.visit_occurrence_id
JOIN {{ source('vocab', 'concept') }} cn 
    ON c.condition_concept_id = cn.concept_id
JOIN {{ ref('headache_codes') }} hmc
    ON c.condition_concept_id = hmc.condition_concept_id
LEFT JOIN {{ source('omop', 'care_site') }} cs
    ON vd.care_site_id = cs.care_site_id
WHERE v.visit_concept_id IN (9201, 9203, 262)
  AND v.visit_start_date BETWEEN '2021-01-01' AND '2023-12-31';


