select 
    ip.person_id, 
    ip.visit_occurrence_id, 
    ip.visit_start_date, 
    ip.visit_end_date, 
    ob.observation_concept_id, 
    ob.observation_date, 
    ob.provider_id, 
    c.concept_name, 
    p.provider_name
from {{ source('omop', 'observation') }} as ob
inner join {{ ref('stg__inpatient_occurrences')}} as ip 
    on ob.visit_occurrence_id = ip.visit_occurrence_id
inner join {{ source('vocab', 'concept')}} as c 
    on ob.observation_concept_id = c.concept_id
inner join {{ source ('omop', 'provider')}} as p 
    on ob.provider_id = p.provider_id
where ob.provider_id in 
    (select provider_id from {{ ref('Neurology_providers_incl_registrars') }})
