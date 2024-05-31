select
  vo.*,
  p.provider_name,
  p.specialty_source_value
from {{ source('omop', 'visit_occurrence') }} as vo
inner join {{ ref('lkup__provider') }} as p
  on vo.provider_id = p.provider_id
where vo.visit_source_value = 'OP'

union

select
    vo.person_id,
  vo.visit_occurrence_id,
  vo.visit_start_date,
  p.provider_name,
  p.specialty_source_value
from {{ source('omop', 'visit_occurrence') }} as vo
inner join {{ ref('lkup__provider') }} as p
  on vo.provider_id = p.provider_id
where vo.visit_source_value = 'OP'

