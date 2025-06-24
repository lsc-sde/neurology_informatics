WITH headache_visits AS (
    SELECT *
    FROM {{ ref('find_all_headaches') }}
),

all_conditions AS (
    SELECT
        co.person_id,
        co.visit_occurrence_id,
        co.condition_concept_id,
        co.condition_status_concept_id,
        co.condition_source_value,  -- added
        cn.concept_name AS diagnosis
    FROM {{ source('omop', 'condition_occurrence') }} co
    JOIN {{ source('vocab', 'concept') }} cn
      ON co.condition_concept_id = cn.concept_id
    WHERE co.visit_occurrence_id IN (
        SELECT visit_occurrence_id FROM {{ ref('find_all_headaches') }}
    )
),

headache_diagnoses AS (
    SELECT
        ac.person_id,
        ac.visit_occurrence_id,
        ac.condition_concept_id,   -- added
        ac.condition_source_value, -- added
        ac.diagnosis AS headache_diagnosis,
        CASE
            WHEN ac.condition_status_concept_id = 32909 THEN 'primary'
            WHEN ac.condition_status_concept_id = 32903 THEN 'secondary'
            ELSE 'other'
        END AS headache_role
    FROM all_conditions ac
    JOIN headache_visits hv
      ON ac.visit_occurrence_id = hv.visit_occurrence_id
     AND ac.condition_concept_id = hv.condition_concept_id
),

primary_non_headache AS (
    SELECT DISTINCT
        ac.visit_occurrence_id,
        ac.diagnosis
    FROM all_conditions ac
    LEFT JOIN headache_visits hv
      ON ac.visit_occurrence_id = hv.visit_occurrence_id
     AND ac.condition_concept_id = hv.condition_concept_id
    WHERE ac.condition_status_concept_id = 32909
      AND hv.condition_concept_id IS NULL
),

secondary_non_headache AS (
    SELECT DISTINCT
        ac.visit_occurrence_id,
        ac.diagnosis
    FROM all_conditions ac
    LEFT JOIN headache_visits hv
      ON ac.visit_occurrence_id = hv.visit_occurrence_id
     AND ac.condition_concept_id = hv.condition_concept_id
    WHERE ac.condition_status_concept_id = 32903
      AND hv.condition_concept_id IS NULL
),

primary_non_headache_agg AS (
    SELECT
        p.visit_occurrence_id,
        STRING_AGG(p.diagnosis, '; ') AS other_primary_diagnoses
    FROM primary_non_headache p
    GROUP BY p.visit_occurrence_id
),

secondary_non_headache_agg AS (
    SELECT
        s.visit_occurrence_id,
        STRING_AGG(s.diagnosis, '; ') AS other_secondary_diagnoses
    FROM secondary_non_headache s
    GROUP BY s.visit_occurrence_id
),

visit_info AS (
    SELECT
        visit_occurrence_id,
        visit_start_date,
        visit_concept_id
    FROM {{ source('omop', 'visit_occurrence') }}
    WHERE visit_occurrence_id IN (SELECT visit_occurrence_id FROM headache_visits)
)

SELECT
    hd.person_id,
    hd.visit_occurrence_id,
    vi.visit_start_date,
    vi.visit_concept_id,
    hd.condition_concept_id,
    hd.condition_source_value,
    hd.headache_diagnosis,
    hd.headache_role,
    pnh.other_primary_diagnoses,
    snh.other_secondary_diagnoses
FROM headache_diagnoses hd
LEFT JOIN primary_non_headache_agg pnh
  ON hd.visit_occurrence_id = pnh.visit_occurrence_id
LEFT JOIN secondary_non_headache_agg snh
  ON hd.visit_occurrence_id = snh.visit_occurrence_id
LEFT JOIN visit_info vi
  ON hd.visit_occurrence_id = vi.visit_occurrence_id;
