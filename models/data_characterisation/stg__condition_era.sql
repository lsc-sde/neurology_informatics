select
  ce.person_id,
  ce.condition_era_id,
  ce.condition_era_start_date,
  ce.condition_era_end_date,
  ce.condition_occurrence_count,
  ce.condition_concept_id,
  c.concept_name as condition_concept_name
from {{ source('omop', 'condition_era') }} as ce
inner join {{ ref('stg__distinct_patients') }} as sdp
  on ce.person_id = sdp.person_id
inner join {{ source('vocab', 'concept') }} as c
  on ce.condition_concept_id = c.concept_id
