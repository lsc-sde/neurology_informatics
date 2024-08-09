select 
    observation_concept_name, 
    count(person_id) as number_of_patients 
from {{ ref('stg__outpatient_observations') }} 
group by 
    observation_concept_name
order by 
    number_of_patients desc OFFSET 0 rows