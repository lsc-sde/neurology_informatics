with counts as (
  select
    'All' as year,
    count(*) as number_of_visits,
    count(distinct vo.person_id) as number_of_patients
  from {{ ref('stg__inpatient_episodes') }} as vo

  union

  select
    cast(year(vo.visit_start_date) as varchar) as year,
    count(*) as number_of_visits,
    count(distinct vo.person_id) as number_of_patients
  from {{ ref('stg__inpatient_episodes') }} as vo
  group by year(vo.visit_start_date)
)

select
  year,
  number_of_patients,
  number_of_visits
from counts