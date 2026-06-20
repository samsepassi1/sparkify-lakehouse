-- Reference SQL (analytics Glue script uses PySpark DataFrames, not SQL)
SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    u.gender,
    u.level,
    count(e.event_id) AS event_count,
    count(DISTINCT e.session_id) AS session_count
FROM transactions.users u
LEFT JOIN transactions.events e ON u.user_id = e.user_id
GROUP BY 1, 2, 3, 4, 5
