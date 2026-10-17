-- Read-only, run by the agent (WIKIPEDIA-CRAWLER-024): the first snapshot --write run (frwiki chunk 75,
-- 2026-10-17 17:35 to 17:48 Paris time) printed many empty "Format :" lines for French movies.
-- 1. Share of empty WIKIPEDIA_FORMAT_LINE: movies written by the run against movies the live crawl wrote.
-- 2. For ten run movies with an empty line, the "Format" part of the stored "Fiche technique" text,
--    to see whether the snapshot text has a format the extraction misses.

SET @t0 = '2026-10-17 17:35:00';
SET @t1 = '2026-10-17 17:48:00';

SELECT IF(DAT_WIKIPEDIA_FORMAT_LINE BETWEEN @t0 AND @t1, 'snapshot run', 'live crawl') AS SOURCE,
       COUNT(*) AS MOVIES,
       SUM(COALESCE(WIKIPEDIA_FORMAT_LINE, '') = '') AS EMPTY_FORMAT,
       ROUND(100 * SUM(COALESCE(WIKIPEDIA_FORMAT_LINE, '') = '') / COUNT(*), 1) AS PCT_EMPTY
FROM T_WC_TMDB_MOVIE
WHERE DAT_WIKIPEDIA_FORMAT_LINE IS NOT NULL
GROUP BY SOURCE;

SELECT m.ID_MOVIE, s.ID_WIKIDATA,
       HEX(SUBSTRING(s.CONTENT, GREATEST(1, LOCATE('Format', s.CONTENT) - 40), 300)) AS CONTENT_AROUND_FORMAT_HEX,
       HEX(SUBSTRING(s.CONTENT_MD, GREATEST(1, LOCATE('Format', s.CONTENT_MD) - 40), 300)) AS MD_AROUND_FORMAT_HEX
FROM T_WC_TMDB_MOVIE m
JOIN T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
  ON s.ID_WIKIDATA = m.ID_WIKIDATA AND s.LANG = 'fr' AND s.TITLE = 'Fiche technique'
WHERE m.DAT_WIKIPEDIA_FORMAT_LINE BETWEEN @t0 AND @t1
  AND COALESCE(m.WIKIPEDIA_FORMAT_LINE, '') = ''
ORDER BY m.ID_MOVIE
LIMIT 10;
