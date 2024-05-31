select distinct person_id
from {{ ref('stg__visit_occurrence_outpatients') }}
