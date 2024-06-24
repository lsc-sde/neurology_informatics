select 
    specialty_concept_name, 
    condition_concept_name, 
    count(person_id) as number_of_patients 
from {{ ref('stg__condition_occurrence') }} 
where condition_start_date >=  {{ var('clinic_visit_start_date') }}
group by 
    specialty_concept_name, 
    condition_concept_name
order by 
    number_of_patients desc OFFSET 0 rows