-- WIKIPEDIA-CRAWLER-019: clear T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL values shared by 25+ distinct entities.
-- SQL twin of migrations/clear_shared_main_images.py, restricted to the column that script missed
-- until 2026-10-14. A real lead image is close to unique; a portal decoration such as
-- Apollo_11_Crew.jpg ("Portail:Années 1960") repeats thousands of times.
-- Cleared to '' ("no lead image"): tmdb-movie-preprocess process 71 copies '' into T2S (1.1.5),
-- and the crawler writes the real lead image back on the next pass if the page has one.
-- Writes outside the agent's five tables: run by Philippe with runsqlvaugouindb.sh.

CREATE TEMPORARY TABLE TMP_SHARED_MAIN_IMAGE AS
SELECT MAIN_IMAGE_URL, COUNT(DISTINCT ID_WIKIDATA) AS ENTITY_COUNT
FROM T_WC_WIKIPEDIA_PAGE_LANG
WHERE MAIN_IMAGE_URL IS NOT NULL AND MAIN_IMAGE_URL <> ''
GROUP BY MAIN_IMAGE_URL
HAVING ENTITY_COUNT >= 25;

-- 1. The trace: every URL about to be cleared, most shared first (read it: a legitimate image here would be a surprise).
SELECT ENTITY_COUNT, MAIN_IMAGE_URL FROM TMP_SHARED_MAIN_IMAGE ORDER BY ENTITY_COUNT DESC;

SELECT COUNT(*) AS URLS, SUM(ENTITY_COUNT) AS ENTITIES FROM TMP_SHARED_MAIN_IMAGE;

-- 2. The clear.
UPDATE T_WC_WIKIPEDIA_PAGE_LANG p
INNER JOIN TMP_SHARED_MAIN_IMAGE s ON s.MAIN_IMAGE_URL = p.MAIN_IMAGE_URL
SET p.MAIN_IMAGE_URL = '';

SELECT ROW_COUNT() AS ROWS_CLEARED;

-- 3. Check: nothing left above the threshold, no Apollo left.
SELECT COUNT(*) AS STILL_SHARED FROM (
  SELECT MAIN_IMAGE_URL FROM T_WC_WIKIPEDIA_PAGE_LANG
  WHERE MAIN_IMAGE_URL IS NOT NULL AND MAIN_IMAGE_URL <> ''
  GROUP BY MAIN_IMAGE_URL HAVING COUNT(DISTINCT ID_WIKIDATA) >= 25
) x;

SELECT COUNT(*) AS APOLLO_LEFT FROM T_WC_WIKIPEDIA_PAGE_LANG WHERE MAIN_IMAGE_URL LIKE '%Apollo_11_Crew%';

DROP TEMPORARY TABLE TMP_SHARED_MAIN_IMAGE;
