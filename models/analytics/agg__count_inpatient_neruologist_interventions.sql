select 
    count(observation_id) as num_interventions, 
    provider_name, 
    ob_proc_meas
from {{ ref ('stg__inpatient_neurologist_interventions')}}
group by 
    provider_name, 
    ob_proc_meas
order by num_interventions desc offset 0 rows