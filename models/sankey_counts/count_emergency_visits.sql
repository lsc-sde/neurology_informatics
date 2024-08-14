select 
    count(visit_occurrence_id) as count
from 
    {{ ref ('emergency_visit_ids')}}