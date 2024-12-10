select distinct person_id
from {{ source('omop', 'visit_occurrence') }} as vo
inner join {{ source('omop', 'provider') }} as pr
  on vo.provider_id = pr.provider_id
where
  vo.visit_source_value = 'OP'
  and vo.visit_start_date >= '2022-05-01'
  and vo.visit_start_date <= '2023-04-30'
  and vo.provider_id in (
    select provider_id from {{ ref('Current_Neurology_providers') }}
  )
