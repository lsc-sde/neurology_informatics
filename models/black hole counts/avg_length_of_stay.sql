WITH cohort AS (
    SELECT person_id
    FROM {{ ref('cohort') }}
),
inpatient_visits AS (
    SELECT 
        vo.visit_occurrence_id,
        vo.person_id,
        vo.visit_start_date,
        vo.visit_end_date,
        DATEDIFF(DAY, vo.visit_start_date, vo.visit_end_date) AS length_of_stay
    FROM {{ source('omop', 'visit_occurrence') }} vo
    WHERE vo.visit_source_value LIKE '%IP%' 
    AND vo.person_id IN (SELECT person_id FROM cohort)
    AND vo.visit_start_date >= {{ var('clinic_visit_start_date') }}
    AND vo.visit_start_date <= {{ var('clinic_visit_end_date') }}
)
SELECT 
    person_id,
    COUNT(visit_occurrence_id) AS num_stays,  
    AVG(length_of_stay) AS avg_length_of_stay,  
    MIN(length_of_stay) AS min_length_of_stay, 
    MAX(length_of_stay) AS max_length_of_stay   
FROM inpatient_visits
GROUP BY person_id;