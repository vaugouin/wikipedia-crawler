-- CONTENT repair (topic wikipedia-rich-sections): 60 French films with a stored Format line, to compare
-- what tmdb-movie-preprocess process 1 parses from today's text and from the repaired text. Read-only.
SELECT m.ID_MOVIE, m.ID_WIKIDATA, p.WIKIPEDIA_PAGE_TITLE, m.DAT_WIKIPEDIA_FORMAT_LINE,
  REPLACE(REPLACE(m.WIKIPEDIA_FORMAT_LINE, CHAR(10), ' / '), CHAR(9), ' ') AS FORMAT_LINE
FROM T_WC_TMDB_MOVIE m
JOIN T_WC_WIKIPEDIA_PAGE_LANG p ON p.ID_WIKIDATA = m.ID_WIKIDATA AND p.LANG = 'fr'
WHERE m.WIKIPEDIA_FORMAT_LINE IS NOT NULL AND m.WIKIPEDIA_FORMAT_LINE <> ''
ORDER BY RAND(20261015)
LIMIT 60;
