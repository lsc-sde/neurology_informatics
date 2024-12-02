select
  c.person_id,
  case
    when
      c.person_id in (
        select person_id
        from {{ source('omop', 'visit_occurrence') }}
        where
          visit_source_value = 'OP'
          and visit_start_date >= {{ var('clinic_visit_start_date') }}
          and visit_start_date <= {{ var('clinic_visit_end_date') }}
      )
      then 'yes'
  end as OP,
  case
    when
      c.person_id in (
        select person_id
        from {{ source('omop', 'visit_occurrence') }}
        where
          visit_source_value like '%IP%'
          and visit_start_date >= {{ var('clinic_visit_start_date') }}
          and visit_start_date <= {{ var('clinic_visit_end_date') }}
      )
      then 'yes'
  end as IP,
  case
    when
      c.person_id in (
        select person_id
        from {{ source('omop', 'visit_occurrence') }}
        where
          visit_source_value like '%ER%'
          and visit_start_date >= {{ var('clinic_visit_start_date') }}
          and visit_start_date <= {{ var('clinic_visit_end_date') }}
      )
      then 'yes'
  end as ED,
  isnull(conditions, 0) as "Number of conditions",
  isnull(procedures, 0) as "Number of procedures",
  isnull(drugs, 0) as "Number of drugs",
  isnull(measurements, 0) as "Number of measurements",
  isnull(observations, 0) as "Number of observations"
from {{ ref('cohort') }} as c
left join (
  select
    person_id,
    count(*) as conditions
  from {{ source('omop', 'condition_occurrence') }}
  where
    condition_start_date >= {{ var('clinic_visit_start_date') }}
    and condition_start_date <= {{ var('clinic_visit_end_date') }}
  group by person_id
) as co
  on c.person_id = co.person_id
left join (
  select
    person_id,
    count(*) as procedures
  from {{ source('omop', 'procedure_occurrence') }}
  where
    procedure_date >= {{ var('clinic_visit_start_date') }}
    and procedure_date <= {{ var('clinic_visit_end_date') }}
  group by person_id
) as po
  on c.person_id = po.person_id
left join (
  select
    person_id,
    count(*) as drugs
  from {{ source('omop', 'drug_exposure') }}
  where
    drug_exposure_start_date >= {{ var('clinic_visit_start_date') }}
    and drug_exposure_start_date <= {{ var('clinic_visit_end_date') }}
  group by person_id
) as de
  on c.person_id = de.person_id
left join (
  select
    person_id,
    count(*) as measurements
  from {{ source('omop', 'measurement') }}
  where
    measurement_date >= {{ var('clinic_visit_start_date') }}
    and measurement_date <= {{ var('clinic_visit_end_date') }}
  group by person_id
) as m
  on c.person_id = m.person_id
left join (
  select
    person_id,
    count(*) as observations
  from {{ source('omop', 'observation') }}
  where
    observation_date >= {{ var('clinic_visit_start_date') }}
    and observation_date <= {{ var('clinic_visit_end_date') }}
  group by person_id
) as o
  on c.person_id = o.person_id
