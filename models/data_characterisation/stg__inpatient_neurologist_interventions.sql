WITH observation_cte AS (
  SELECT
    ip.person_id,
    ip.visit_occurrence_id,
    ip.visit_start_date,
    ip.visit_end_date,
    datediff(DAY, visit_start_date, visit_end_date) AS length_of_stay,
    ip.concept_name AS primary_discharge_diagnosis,
    ob.observation_id,
    ob.observation_date,
    datediff(DAY, visit_start_date, observation_date) AS time_to_involvement,
    c.concept_name,
    p.provider_name,
    'observation' AS ob_proc_meas
  FROM {{ source('omop', 'observation') }} AS ob
  INNER JOIN {{ ref('stg__inpatient_occurrences') }} AS ip
    ON ob.visit_occurrence_id = ip.visit_occurrence_id
  INNER JOIN {{ source('vocab', 'concept') }} AS c
    ON ob.observation_concept_id = c.concept_id
  INNER JOIN {{ source('omop', 'provider') }} AS p
    ON ob.provider_id = p.provider_id
  WHERE ob.provider_id IN (
    SELECT provider_id
    FROM {{ ref('Neurology_providers_incl_registrars') }}
  )
  AND discharge_diagnosis_type = 'Primary discharge diagnosis'
),

procedure_cte AS (
  SELECT
    ip.person_id,
    ip.visit_occurrence_id,
    ip.visit_start_date,
    ip.visit_end_date,
    datediff(DAY, visit_start_date, visit_end_date) AS length_of_stay,
    ip.concept_name AS primary_discharge_diagnosis,
    pr.procedure_occurrence_id,
    pr.procedure_date,
    datediff(DAY, visit_start_date, procedure_date) AS time_to_involvement,
    c.concept_name,
    p.provider_name,
    'procedure' AS ob_proc_meas
  FROM {{ source('omop', 'procedure_occurrence') }} AS pr
  INNER JOIN {{ ref('stg__inpatient_occurrences') }} AS ip
    ON pr.visit_occurrence_id = ip.visit_occurrence_id
  INNER JOIN {{ source('vocab', 'concept') }} AS c
    ON pr.procedure_concept_id = c.concept_id
  INNER JOIN {{ source('omop', 'provider') }} AS p
    ON pr.provider_id = p.provider_id
  WHERE pr.provider_id IN (
    SELECT provider_id
    FROM {{ ref('Neurology_providers_incl_registrars') }}
  )
  AND discharge_diagnosis_type = 'Primary discharge diagnosis'
),

measurement_cte AS (
  SELECT
    ip.person_id,
    ip.visit_occurrence_id,
    ip.visit_start_date,
    ip.visit_end_date,
    datediff(DAY, visit_start_date, visit_end_date) AS length_of_stay,
    ip.concept_name AS primary_discharge_diagnosis,
    m.measurement_id,
    m.measurement_date,
    datediff(DAY, visit_start_date, measurement_date) AS time_to_involvement,
    c.concept_name,
    p.provider_name,
    'measurement' AS ob_proc_meas
  FROM {{ source('omop', 'measurement') }} AS m
  INNER JOIN {{ ref('stg__inpatient_occurrences') }} AS ip
    ON m.visit_occurrence_id = ip.visit_occurrence_id
  INNER JOIN {{ source('vocab', 'concept') }} AS c
    ON m.measurement_concept_id = c.concept_id
  INNER JOIN {{ source('omop', 'provider') }} AS p
    ON m.provider_id = p.provider_id
  WHERE m.provider_id IN (
    SELECT provider_id
    FROM {{ ref('Neurology_providers_incl_registrars') }}
  )
  AND discharge_diagnosis_type = 'Primary discharge diagnosis'
),

final AS (
  SELECT *
  FROM observation_cte
  WHERE length_of_stay > 0
  UNION ALL
  SELECT *
  FROM procedure_cte
  WHERE length_of_stay > 0
  UNION ALL
  SELECT *
  FROM measurement_cte
  WHERE length_of_stay > 0
)

SELECT
  f.*,
  p.gender_source_value,
  datediff(YEAR, p.birth_datetime, f.visit_start_date) AS age_at_visit
FROM final AS f
INNER JOIN {{ source('omop', 'person') }} AS p ON f.person_id = p.person_id
