select 
    po.procedure_occurrence_id, 
    po.person_id, 
    po.procedure_concept_id, 
    po.procedure_date, 
    po.procedure_source_value,
    c.concept_name
from  {{ source('omop', 'procedure_occurrence') }} as po
inner join {{ ref('stg__distinct_patients') }} as do 
    on po.person_id = do.person_id
inner join {{ source('vocab', 'concept') }} as c
  on po.procedure_concept_id = c.concept_id
where po.procedure_date >=  {{ var('clinic_visit_start_date') }}