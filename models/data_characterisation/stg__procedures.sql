select 
    po.person_id, 
    po.procedure_date, 
    po.procedure_source_value,
    p.provider_name as procedure_provider,
    p.specialty_source_value as procedure_specialty,
    c.concept_name as procedure_concept_name,
    vo.visit_occurrence_id,
    vo.visit_start_date as outpatient_appointment_date,
    vo.provider_name as outpatient_provider,
    vo.specialty_source_value as outpatient_specialty
from  {{ source('omop', 'procedure_occurrence') }} as po
inner join {{ ref('stg__visit_occurrence_outpatients') }} as vo 
    on po.visit_occurrence_id = vo.visit_occurrence_id
inner join {{ source('vocab', 'concept') }} as c
    on po.procedure_concept_id = c.concept_id
inner join {{ source('omop', 'provider') }} as p
    on p.provider_id = po.provider_id 
where po.procedure_date >=  {{ var('clinic_visit_start_date') }}