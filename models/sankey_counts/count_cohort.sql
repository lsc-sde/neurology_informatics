select(count(distinct(person_id))) as count
from {{ ref('cohort')}}