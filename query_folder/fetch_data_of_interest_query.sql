SELECT 
    user_name,
    artist_name,
    artist_msid,
    COUNT(*) AS number_of_listens
FROM listen
WHERE listened_at BETWEEN TIMESTAMP("2018-06-23 00:00:00+00") AND TIMESTAMP("2018-07-23 00:00:00+00")
GROUP BY user_name, artist_name, artist_msid
ORDER BY user_name, artist_name, artist_msid

