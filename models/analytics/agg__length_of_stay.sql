select 
    person_id, 
    visit_occurrence_id, 
    visit_start_date, 
    visit_end_date, 
    datediff(day, visit_start_date, visit_end_date) as length_of_stay, 
    concept_name, 
    specialty_source_value
from {{ ref('stg__inpatient_occurrences') }}
where discharge_diagnosis_type = 'Primary discharge diagnosis'
order by length_of_stay DESC offset 0 rows
