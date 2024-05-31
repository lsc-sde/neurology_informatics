select
  co.person_id,
  co.visit_occurrence_id,
  co.condition_occurrence_id,
  co.condition_start_date,
  co.condition_concept_id,
  c.concept_name as condition_concept_name,
  c_condition_status.concept_name as condition_status_concept_name,
  c_provider.concept_name as specialty_concept_name
from {{ source('omop', 'condition_occurrence') }} as co
inner join {{ ref('stg__distinct_patients') }} as sdp
  on co.person_id = sdp.person_id
inner join {{ source('vocab', 'concept') }} as c
  on co.condition_concept_id = c.concept_id
inner join {{ source('vocab', 'concept') }} as c_condition_status
  on co.condition_status_concept_id = c_condition_status.concept_id
inner join {{ source('omop', 'provider') }} as pr
  on co.provider_id = pr.provider_id
inner join {{ source('vocab', 'concept') }} as c_provider
  on pr.specialty_concept_id = c_provider.concept_id
