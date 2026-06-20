-- Reference SQL (analytics Glue script uses PySpark DataFrames, not SQL)
SELECT
    date(from_unixtime(ts / 1000)) AS activity_date,
    user_id,
    count(*) AS events,
    count(distinct session_id) AS sessions
FROM transactions.events
GROUP BY 1, 2
