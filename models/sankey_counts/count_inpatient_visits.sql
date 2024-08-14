select 
    count(visit_occurrence_id) as count
from 
    {{ ref ('inpatient_visit_ids')}}