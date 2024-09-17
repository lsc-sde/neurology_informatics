select
    c.person_id,
    case when c.person_id in (select person_id from {{ source('omop', 'visit_occurrence') }} where visit_source_value = 'OP')
    then 'yes'
    end as OP,
    case when c.person_id in (select person_id from {{ source('omop', 'visit_occurrence') }} where visit_source_value like '%IP%')
    then 'yes'
    end as IP,
    case when c.person_id in (select person_id from {{ source('omop', 'visit_occurrence') }} where visit_source_value like '%ER%')
    then 'yes'
    end as ED,
	isnull(conditions, 0) as "Number of conditions",
	isnull(procedures, 0) as "Number of procedures",
	isnull(drugs, 0) as "Number of drugs",
	isnull(measurements, 0) as "Number of measurements"
from {{ ref('cohort') }} as c
left join (
	select person_id, count(*) as "conditions" from {{ source('omop', 'condition_occurrence') }} group by person_id
	) co
on c.person_id = co.person_id
left join (
	select person_id, count(*) as "procedures" from {{ source('omop', 'procedure_occurrence') }} group by person_id
	) po
on c.person_id = po.person_id
left join (
	select person_id, count(*) as "drugs" from {{ source('omop', 'drug_exposure') }} group by person_id
	) de
on c.person_id = de.person_id
left join (
	select person_id, count(*) as "measurements" from {{ source('omop', 'measurement') }} group by person_id
	) m
on c.person_id = m.person_id