SELECT TOP 10
  DATE_TRUNC('minute', EventTime) AS M,
  COUNT(*) AS PageViews
FROM hits
WHERE CounterID = 62
  AND EventDate >= '2013-07-14'
  AND EventDate <= '2013-07-15'
  AND IsRefresh = 0
  AND DontCountHits = 0
GROUP BY DATE_TRUNC('minute', EventTime)
ORDER BY DATE_TRUNC('minute', EventTime)
OFFSET 1000 ROWS;