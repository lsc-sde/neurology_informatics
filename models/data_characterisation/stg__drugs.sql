select
  de.person_id,
  de.drug_concept_id,
  de.visit_occurrence_id,
  de.drug_exposure_start_date,
  c.concept_name as drug_concept_name,
  pr.provider_name,
  pr.specialty_source_value,
  vo.visit_source_value
from {{ source('omop', 'drug_exposure') }} as de
inner join {{ ref('stg__distinct_patients') }} as sdp
  on de.person_id = sdp.person_id
inner join {{ source('omop', 'visit_occurrence') }} as vo
  on de.visit_occurrence_id = vo.visit_occurrence_id
inner join {{ source('vocab', 'concept') }} as c
  on de.drug_concept_id = c.concept_id
inner join {{ source('omop', 'provider') }} as pr
  on de.provider_id = pr.provider_id