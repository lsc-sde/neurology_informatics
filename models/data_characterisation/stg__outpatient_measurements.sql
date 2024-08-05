select 
    m.person_id, 
    m.measurement_source_value,
    c.concept_name as measurement_concept_name,
    vo.visit_occurrence_id,
    vo.visit_start_date as outpatient_appointment_date,
    vo.provider_name as outpatient_provider,
    vo.specialty_source_value as outpatient_specialty
from  {{ source('omop', 'measurement') }} as m
inner join {{ ref('stg__visit_occurrence_outpatients') }} as vo 
    on m.visit_occurrence_id = vo.visit_occurrence_id
inner join {{ source('vocab', 'concept') }} as c
    on m.measurement_concept_id = c.concept_id
inner join {{ source('omop', 'provider') }} as p
    on p.provider_id = m.provider_id 
where m.measurement_date >=  {{ var('clinic_visit_start_date') }}