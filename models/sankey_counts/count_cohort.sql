select(count(person_id)) as count
from {{ ref('cohort')}}