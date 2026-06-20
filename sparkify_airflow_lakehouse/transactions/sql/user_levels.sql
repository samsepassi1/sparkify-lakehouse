SELECT
    user_id,
    level,
    current_timestamp() AS effective_at
FROM transactions.users
