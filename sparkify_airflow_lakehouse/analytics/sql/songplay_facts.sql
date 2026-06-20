-- Reference SQL (analytics Glue script uses PySpark DataFrames, not SQL)
SELECT e.ts, e.user_id, e.session_id, e.song_id, e.artist_id, e.level
FROM transactions.events e
WHERE e.page = 'NextSong'
