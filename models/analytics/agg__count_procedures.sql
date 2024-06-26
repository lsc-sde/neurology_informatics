select 
    procedure_source_value, 
    concept_name,
    count(person_id) as number_of_patients 
from {{ ref('stg__procedures') }} 
group by 
    procedure_source_value,
    concept_name
order by 
    number_of_patients desc OFFSET 0 rows
    