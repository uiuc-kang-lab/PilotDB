SELECT TOP 10
  UserID,
  extract(
    MINUTE
    FROM EventTime
  ) AS m,
  SearchPhrase,
  COUNT(*)
FROM hits
GROUP BY UserID,
  m,
  SearchPhrase
ORDER BY COUNT(*) DESC;