SELECT TOP 10
  ClientIP,
  ClientIP - 1,
  ClientIP - 2,
  ClientIP - 3,
  COUNT(*) AS c
FROM hits
GROUP BY ClientIP,
  ClientIP - 1,
  ClientIP - 2,
  ClientIP - 3
ORDER BY c DESC;