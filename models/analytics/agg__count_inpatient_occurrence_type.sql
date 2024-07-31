select
    specialty_source_value,
    count(distinct(visit_occurrence_id)) as num_visits
from {{ ref('stg__inpatient_occurrences') }}
group by 
    specialty_source_value
order by 
    num_visits desc OFFSET 0 rows