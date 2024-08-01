select 
    concept_name as primary_discharge_diagnosis, 
    AVG(length_of_stay) as average_length_of_stay,
    COUNT(visit_occurrence_id) as number_of_stays,
    COUNT(DISTINCT(person_id)) as number_of_patients,
    MIN(length_of_stay) as minimum_length,
    MAX(length_of_stay) as max_length
from {{ ref('agg__length_of_stay') }}
group by 
    concept_name
order by 
    number_of_stays desc,
    average_length_of_stay,
    number_of_patients,
    minimum_length,
    max_length
offset 0 rows



