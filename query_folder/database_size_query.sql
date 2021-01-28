SELECT 
    COUNT(*) AS total_entries,
    COUNT(DISTINCT user_name) AS total_users,
    COUNT(DISTINCT artist_msid) AS total_artists,
    MIN(listened_at) AS first_entry,
    MAX(listened_at) AS last_entry
FROM listen

