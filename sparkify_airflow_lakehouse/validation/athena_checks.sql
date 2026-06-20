-- Athena validation queries for the Sparkify lakehouse
-- Database names: raw, transactions, analytics (per rubric requirements)

-- Raw layer checks
SELECT count(*) AS raw_logs_count FROM raw.logs;
SELECT count(*) AS raw_songs_count FROM raw.songs;

-- Transaction layer checks
SELECT count(*) AS events_count FROM transactions.events;
SELECT count(*) - count(DISTINCT event_id) AS duplicate_events FROM transactions.events;
SELECT count(*) AS users_count FROM transactions.users;
SELECT count(*) AS artists_count FROM transactions.artists;
SELECT count(*) AS songs_count FROM transactions.songs;

-- Analytics layer checks
SELECT count(*) AS songplay_facts_count FROM analytics.songplay_facts;
SELECT count(*) AS user_activity_daily_count FROM analytics.user_activity_daily;
SELECT count(*) AS artist_popularity_count FROM analytics.artist_popularity;
SELECT * FROM analytics.user_facts ORDER BY event_count DESC LIMIT 20;

-- Referential integrity: events should reference valid users
SELECT count(*) AS orphan_user_events
FROM transactions.events e
LEFT JOIN transactions.users u ON e.user_id = u.user_id
WHERE u.user_id IS NULL AND e.user_id IS NOT NULL;
