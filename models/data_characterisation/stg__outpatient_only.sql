select person_id
from {{ ref('stg__distinct_patients') }}
where
  person_id not in (
    select person_id from {{ ref('stg__inpatient_occurrences') }}
  )
