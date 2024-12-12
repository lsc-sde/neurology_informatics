SELECT person_id
FROM
  {{ ref('op_ip_ed') }}
WHERE
  OP = 'yes' AND (IP = '' OR IP IS NULL) AND (ED = '' OR ED IS NULL)
