select distinct(person_id)
from {{ source('omop', 'visit_occurrence') }} as vo
inner join {{ source('omop', 'provider') }} as pr
  on vo.provider_id = pr.provider_id
where
  vo.visit_source_value = 'OP'
  and vo.visit_start_date >= {{ var('clinic_visit_start_date') }}
  and vo.visit_start_date <= {{ var('clinic_visit_end_date') }}
  and vo.provider_id in (
      select provider_id from {{ ref('Current_Neurology_providers') }}
    )
