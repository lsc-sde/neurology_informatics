select 
    condition_concept_name, 
    count(person_id) as number_of_patients 
from {{ ref('stg__condition_occurrence') }} 
where 
    condition_start_date >=  {{ var('clinic_visit_start_date') }} 
    and specialty_concept_name = 'Neurology'
group by 
    condition_concept_name
order by 
    number_of_patients desc OFFSET 0 rows