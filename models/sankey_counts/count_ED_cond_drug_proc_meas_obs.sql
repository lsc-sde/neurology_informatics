WITH condition_counts AS (
    SELECT 
        COUNT(DISTINCT condition_occurrence_id) AS condition_count
    FROM 
        {{ source ('omop', 'condition_occurrence')}}
    WHERE 
        visit_occurrence_id IN (SELECT visit_occurrence_id FROM {{ ref ('emergency_visit_ids')}})
),
procedure_counts AS (
    SELECT 
        COUNT(DISTINCT procedure_occurrence_id) AS procedure_count
    FROM 
        {{ source ('omop', 'procedure_occurrence')}}
    WHERE 
        visit_occurrence_id IN (SELECT visit_occurrence_id FROM {{ ref ('emergency_visit_ids')}})
),
observation_counts AS (
    SELECT 
        COUNT(DISTINCT observation_id) AS observation_count
    FROM 
        {{ source ('omop', 'observation')}}
    WHERE 
        visit_occurrence_id IN (SELECT visit_occurrence_id FROM {{ ref ('emergency_visit_ids')}})
),
measurement_counts AS (
    SELECT 
        COUNT(DISTINCT measurement_id) AS measurement_count
    FROM 
        {{ source ('omop', 'measurement')}}
    WHERE 
        visit_occurrence_id IN (SELECT visit_occurrence_id FROM {{ ref ('emergency_visit_ids')}})
),
drug_counts AS (
    SELECT 
        COUNT(DISTINCT drug_exposure_id) AS drug_count
    FROM 
        {{ source ('omop', 'drug_exposure')}}
    WHERE 
        visit_occurrence_id IN (SELECT visit_occurrence_id FROM {{ ref ('emergency_visit_ids')}})
)
SELECT 
    co.condition_count,
    pc.procedure_count,
    oc.observation_count,
    mc.measurement_count,
    dc.drug_count
FROM 
    condition_counts co,
    procedure_counts pc,
    observation_counts oc,
    measurement_counts mc,
    drug_counts dc;