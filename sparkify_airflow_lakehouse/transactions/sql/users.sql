SELECT DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM raw.logs
WHERE data_interval = '{{ data_interval }}'
  AND userId IS NOT NULL
