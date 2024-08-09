select 
    procedure_source_value, 
    procedure_concept_name,
    procedure_specialty,
    count(person_id) as number_of_patients 
from {{ ref('stg__outpatient_procedures') }} 
group by 
    procedure_source_value,
    procedure_concept_name,
    procedure_specialty
order by 
    number_of_patients desc OFFSET 0 rows
    