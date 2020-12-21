SELECT 
    COUNT(DISTINCT visitorId)      AS unique_visitorID,
    COUNT(DISTINCT visitNumber)    AS unique_visitNumber,
    COUNT(DISTINCT visitId)        AS unique_visitId,
    COUNT(DISTINCT visitStartTime) AS unique_visitStartTime,
    COUNT(DISTINCT date)           AS unique_date,
    COUNT(DISTINCT totals)         AS unique_totals,
    COUNT(DISTINCT trafficSource)  AS unique_trafficSource,
    COUNT(DISTINCT device)         AS unique_device
FROM `ga_sessions_*`
LIMIT 5;    