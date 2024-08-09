select 
    o.person_id, 
    o.value_as_number,
    o.value_as_string, 
    o.observation_source_value,
    c.concept_name as observation_concept_name,
    vo.visit_occurrence_id,
    vo.visit_start_date as outpatient_appointment_date,
    vo.provider_name as outpatient_provider,
    vo.specialty_source_value as outpatient_specialty
from  {{ source('omop', 'observation') }} as o
inner join {{ ref('stg__visit_occurrence_outpatients') }} as vo 
    on o.visit_occurrence_id = vo.visit_occurrence_id
inner join {{ source('vocab', 'concept') }} as c
    on o.observation_concept_id = c.concept_id
inner join {{ source('omop', 'provider') }} as p
    on p.provider_id = o.provider_id 
where o.observation_date >=  {{ var('clinic_visit_start_date') }}