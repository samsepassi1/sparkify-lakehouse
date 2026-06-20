-- Reference SQL (analytics Glue script uses PySpark DataFrames, not SQL)
SELECT
    a.artist_id,
    a.artist_name,
    count(*) AS plays
FROM transactions.events e
JOIN transactions.artists a ON e.artist_id = a.artist_id
GROUP BY 1, 2
