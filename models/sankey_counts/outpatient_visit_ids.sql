select 
    distinct(visit_occurrence_id)
from 
    {{ source('omop', 'visit_occurrence') }} as vo
where 
    vo.visit_source_value = 'OP'
    and person_id in (select person_id from {{ ref('cohort') }})
    and vo.visit_start_date >= {{ var('clinic_visit_start_date') }}
    and vo.visit_start_date <= {{ var('clinic_visit_end_date') }}