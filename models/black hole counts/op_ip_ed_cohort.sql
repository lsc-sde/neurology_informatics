SELECT person_id
FROM
  {{ ref('op_ip_ed') }}
WHERE
  OP = 'yes' AND IP = 'yes' AND ED = 'yes'
