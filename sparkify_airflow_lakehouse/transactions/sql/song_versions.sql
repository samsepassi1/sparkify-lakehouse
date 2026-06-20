SELECT
    song_id,
    title,
    artist_id,
    year,
    duration,
    current_timestamp() AS version_loaded_at
FROM raw.songs
WHERE data_interval = '{{ data_interval }}'
