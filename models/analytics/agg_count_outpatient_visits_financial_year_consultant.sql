select 
    financial_year, 
    provider_name,
    count(person_id) as number_of_visits 
from {{ ref('stg__visit_occurrence_outpatients') }} 
group by 
    financial_year,
    provider_name
order by 
    financial_year desc OFFSET 0 rows