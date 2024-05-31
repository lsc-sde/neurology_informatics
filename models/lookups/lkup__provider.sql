select
  p.provider_id,
  p.provider_name,
  p.specialty_concept_id,
  p.specialty_source_value
from {{ source('omop', 'provider') }} as p
where lower(p.specialty_source_value) like '%neurol%'
