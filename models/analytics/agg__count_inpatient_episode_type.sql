select
    specialty_source_value,
    count(person_id) as num_visits
from {{ ref('stg__inpatient_episodes') }}
group by 
    specialty_source_value
order by 
    num_visits desc OFFSET 0 rows