Select *
from {{source('omop', 'drug_exposure')}}
where drug_concept_id in (select Concept_id from {{ref('DMTs')}})
