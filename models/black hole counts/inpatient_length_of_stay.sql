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
    visit_occurrence_id,
    person_id,
    visit_start_date,
    visit_end_date,
    length_of_stay
FROM inpatient_visits;