select 
    drug_concept_name, 
    specialty_source_value,
    count(person_id) as number_of_patients 
from {{ ref('stg__drugs') }} 
group by 
    drug_concept_name,
    specialty_source_value
order by 
    number_of_patients desc OFFSET 0 rows