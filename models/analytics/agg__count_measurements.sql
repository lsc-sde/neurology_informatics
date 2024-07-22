select 
    measurement_concept_name, 
    count(person_id) as number_of_patients 
from {{ ref('stg__measurements') }} 
group by 
    measurement_concept_name
order by 
    number_of_patients desc OFFSET 0 rows