SELECT
    concat_ws('-', l.ts, l.userId, l.sessionId, l.page) AS event_id,
    l.ts,
    l.userId AS user_id,
    l.sessionId AS session_id,
    l.page,
    l.song,
    s.song_id,
    s.artist_id,
    l.level
FROM raw.logs l
LEFT JOIN transactions.songs s ON l.song = s.title
WHERE l.data_interval = '{{ data_interval }}'
