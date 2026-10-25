-- Why a page shows no CONTENT_MD on the front (asked by Philippe on 2026-10-25 for
-- Inception, movie.php?movieid=27205, Q25188). Read-only (voie 1). Change @QID to reuse.
-- The front (f_wikipediallsections) renders CONTENT_MD when one section of the page in the
-- UI language carries it, and falls back to CONTENT otherwise.

SET @QID = 'Q25188';

-- A. The page per language: is it known, and when was it written?
SELECT LANG, WIKIPEDIA_PAGE_TITLE, PAGE_EXISTS, HTTP_STATUS, LAST_CRAWLED_AT, LAST_SUCCESS_AT
FROM T_WC_WIKIPEDIA_PAGE_LANG
WHERE ID_WIKIDATA = @QID COLLATE utf8mb4_unicode_ci
ORDER BY LANG;

-- B. Its sections: how many carry CONTENT, how many CONTENT_MD, and the last write
SELECT LANG, COUNT(*) AS SECTIONS,
       SUM(CONTENT IS NOT NULL AND CONTENT <> '') AS WITH_CONTENT,
       SUM(CONTENT_MD IS NOT NULL AND CONTENT_MD <> '') AS WITH_CONTENT_MD,
       MIN(TIM_UPDATED) AS FIRST_WRITE, MAX(TIM_UPDATED) AS LAST_WRITE
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION
WHERE ID_WIKIDATA = @QID COLLATE utf8mb4_unicode_ci
GROUP BY LANG
ORDER BY LANG;

-- C. Its links and footnotes, written only by the CONTENT_MD path
SELECT 'link' AS KIND, LANG, COUNT(*) AS ROWS_, MAX(TIM_UPDATED) AS LAST_WRITE
FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK WHERE ID_WIKIDATA = @QID COLLATE utf8mb4_unicode_ci GROUP BY LANG
UNION ALL
SELECT 'reference', LANG, COUNT(*), MAX(TIM_UPDATED)
FROM T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE WHERE ID_WIKIDATA = @QID COLLATE utf8mb4_unicode_ci GROUP BY LANG
ORDER BY KIND, LANG;

-- D. Where the snapshot ingest stands
SELECT VAR_NAME, LEFT(VAR_VALUE, 120) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE VAR_NAME LIKE 'strwikipediadumpingest%'
ORDER BY VAR_NAME;

-- E. CONTENT_MD coverage of the movie pages, per language
SELECT s.LANG, COUNT(DISTINCT s.ID_WIKIDATA) AS PAGES,
       COUNT(DISTINCT IF(s.CONTENT_MD IS NOT NULL AND s.CONTENT_MD <> '', s.ID_WIKIDATA, NULL)) AS PAGES_WITH_MD
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
JOIN T_WC_T2S_MOVIE m ON m.ID_WIKIDATA = s.ID_WIKIDATA
WHERE s.LANG IN ('en', 'fr')
GROUP BY s.LANG;
