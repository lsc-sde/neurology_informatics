select
  vo.visit_occurrence_id,
  vo.visit_start_date,
  vo.person_id,
  p.year_of_birth,
  c_gender.concept_name as gender,
  c_race.concept_name as ethnicity,
  vo.visit_source_value as visit_type_source_value,
  pr.provider_name,
  pr.specialty_source_value,
  cs.care_site_name
from {{ source('omop', 'visit_occurrence') }} as vo
inner join {{ source('omop', 'person') }} as p
  on vo.person_id = p.person_id
inner join {{ source('vocab', 'concept') }} as c_gender
  on p.gender_concept_id = c_gender.concept_id
inner join {{ source('vocab', 'concept') }} as c_race
  on p.race_concept_id = c_race.concept_id
inner join {{ source('omop', 'care_site') }} as cs
  on vo.care_site_id = cs.care_site_id
inner join {{ source('omop', 'provider') }} as pr
  on vo.provider_id = pr.provider_id
where
  vo.visit_source_value = 'OP'
  and vo.visit_start_date >= {{ var('clinic_visit_start_date') }}
  and vo.visit_start_date <= {{ var('clinic_visit_end_date') }}
  and (
    vo.provider_id in (
      select distinct provider_id from {{ ref('lkup__provider') }}
    )
    or vo.care_site_id in (
      select distinct care_site_id from {{ ref('lkup__care_site_outpatients') }}
    )
  )
