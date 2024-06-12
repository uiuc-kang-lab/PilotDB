SELECT TOP 10
  SearchEngineID,
  ClientIP,
  COUNT(*) AS c,
  SUM(IsRefresh),
  AVG(ResolutionWidth)
FROM hits
WHERE SearchPhrase <> ''
GROUP BY SearchEngineID,
  ClientIP
ORDER BY c DESC;