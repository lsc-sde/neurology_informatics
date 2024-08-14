select 
    count(visit_occurrence_id) as count
from 
    {{ ref ('outpatient_visit_ids')}}