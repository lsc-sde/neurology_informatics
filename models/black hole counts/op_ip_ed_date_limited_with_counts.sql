select
  c.person_id,
  isnull(op_appointments, 0) as "Number of OP appointments",
  isnull(ip_appointments, 0) as "Number of IP stays",
  isnull(ed_appointments, 0) as "Number of ED visits",
  isnull(conditions, 0) as "Number of conditions",
  isnull(procedures, 0) as "Number of procedures",
  isnull(drugs, 0) as "Number of drugs",
  isnull(measurements, 0) as "Number of measurements",
  isnull(observations, 0) as "Number of observations"
from {{ ref('cohort') }} as c
left join (
  select
    person_id,
    count(*) as op_appointments
  from {{ source('omop', 'visit_occurrence') }}
  where
    visit_source_value = 'OP'
    and visit_start_date >= {{ var('clinic_visit_start_date') }}
    and visit_start_date <= {{ var('clinic_visit_end_date') }}
  group by person_id
) as op
  on c.person_id = op.person_id
left join (
  select
    person_id,
    count(*) as ip_appointments
  from {{ source('omop', 'visit_occurrence') }}
  where
    visit_source_value like '%IP%'
    and visit_start_date >= {{ var('clinic_visit_start_date') }}
    and visit_start_date <= {{ var('clinic_visit_end_date') }}
  group by person_id
) as ip
  on c.person_id = ip.person_id
left join (
  select
    person_id,
    count(*) as ed_appointments
  from {{ source('omop', 'visit_occurrence') }}
  where
    visit_source_value like '%ER%'
    and visit_start_date >= {{ var('clinic_visit_start_date') }}
    and visit_start_date <= {{ var('clinic_visit_end_date') }}
  group by person_id
) as ed
  on c.person_id = ed.person_id
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
