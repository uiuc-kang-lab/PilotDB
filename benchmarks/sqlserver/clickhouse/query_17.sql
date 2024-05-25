SELECT TOP 10
  UserID,
  SearchPhrase,
  COUNT(*)
FROM hits
GROUP BY UserID,
  SearchPhrase
ORDER BY COUNT(*) DESC;