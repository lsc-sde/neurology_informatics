select
  vo.person_id,
  vo.visit_occurrence_id,
  vo.visit_source_value,
  vo.visit_start_date,
  vo.visit_end_date,
  c.concept_name,
  pr.provider_name,
  pr.specialty_source_value,
  c_condition_status.concept_name as discharge_diagnosis_type
from {{ source('omop', 'visit_occurrence') }} as vo
inner join {{ source('omop', 'condition_occurrence') }} as co
  on vo.visit_occurrence_id = co.visit_occurrence_id
inner join {{ source('vocab', 'concept') }} as c
  on co.condition_concept_id = c.concept_id
inner join {{ source('omop', 'provider') }} as pr
  on co.provider_id = pr.provider_id
inner join {{ source('vocab', 'concept') }} as c_condition_status
  on co.condition_status_concept_id = c_condition_status.concept_id
where
  vo.visit_source_value = 'IP'
  and vo.visit_start_date >= {{ var('clinic_visit_start_date') }}
  and vo.visit_start_date <= {{ var('clinic_visit_end_date') }}
  and vo.person_id in (
    select person_id from {{ ref('stg__distinct_patients') }}
  )
