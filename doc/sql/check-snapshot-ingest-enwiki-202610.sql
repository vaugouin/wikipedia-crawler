-- Read-only, run by the agent: what the first automatic run (run-if-new-snapshot.sh, wikipedia_dump_ingest.py
-- --auto, 1.1.12, WIKIPEDIA-CRAWLER-025) stored for the enwiki snapshot of 2026-10-01: 435 chunks, one forked
-- process per chunk, from 2026-10-19 07:34 to 2026-10-21 03:47 Paris time (server variables
-- strwikipediadumpingesten*), main crawler stopped by the INGEST_IN_PROGRESS flag. Same queries as
-- check-snapshot-ingest-batch3.sql; tmp_ing keeps LANG en only.
-- LAST_CRAWLED_AT is written in Paris time by the Python code.

SET @t0 = '2026-10-19 07:30:00';
SET @t1 = '2026-10-21 03:49:00';

SELECT 'window_span' AS PART, MIN(LAST_CRAWLED_AT) AS FIRST_WRITE, MAX(LAST_CRAWLED_AT) AS LAST_WRITE
FROM T_WC_WIKIPEDIA_PAGE_LANG
WHERE LAST_CRAWLED_AT BETWEEN @t0 AND @t1;

SELECT 'per_hour' AS PART, LANG, DATE_FORMAT(LAST_CRAWLED_AT, '%Y-%m-%d %H:00') AS HOUR_START,
       COUNT(*) AS PAGES, SUM(HTTP_STATUS = 200) AS HTTP_200
FROM T_WC_WIKIPEDIA_PAGE_LANG
WHERE LAST_CRAWLED_AT BETWEEN @t0 AND @t1
GROUP BY LANG, HOUR_START
ORDER BY HOUR_START, LANG;

-- 1. Anything else written in the window (should be the ingest only, LANG en)
SELECT 'window_by_lang' AS PART, LANG, COUNT(*) AS PAGES
FROM T_WC_WIKIPEDIA_PAGE_LANG
WHERE LAST_CRAWLED_AT BETWEEN @t0 AND @t1
GROUP BY LANG;

CREATE TEMPORARY TABLE tmp_ing (PRIMARY KEY (ID_WIKIDATA, LANG)) AS
SELECT ID_WIKIDATA, LANG, ITEM_TYPE, HTTP_STATUS, MAIN_IMAGE_URL
FROM T_WC_WIKIPEDIA_PAGE_LANG
WHERE LAST_CRAWLED_AT BETWEEN @t0 AND @t1 AND LANG = 'en';

-- 2. Pages per family and HTTP status (all 200)
SELECT 'pages' AS PART, ITEM_TYPE, HTTP_STATUS, COUNT(*) AS PAGES,
       SUM(MAIN_IMAGE_URL IS NOT NULL AND MAIN_IMAGE_URL <> '') AS WITH_MAIN_IMAGE
FROM tmp_ing
GROUP BY ITEM_TYPE, HTTP_STATUS
ORDER BY PAGES DESC;

-- 3. Sections (none left without Markdown)
SELECT 'sections' AS PART,
       COUNT(*) AS SECTIONS,
       SUM(s.CONTENT_MD IS NOT NULL AND s.CONTENT_MD <> '') AS WITH_MD,
       SUM(s.CONTENT_MD IS NULL OR s.CONTENT_MD = '') AS WITHOUT_MD,
       COUNT(DISTINCT s.ID_WIKIDATA) AS PAGES_WITH_SECTIONS
FROM tmp_ing t
JOIN T_WC_WIKIPEDIA_PAGE_LANG_SECTION s ON s.ID_WIKIDATA = t.ID_WIKIDATA AND s.LANG = t.LANG;

-- 4. Links and how they resolved (title join only, no API call)
SELECT 'links' AS PART, COALESCE(l.RESOLVED_BY, '(none)') AS RESOLVED_BY,
       COUNT(*) AS LINKS,
       SUM(l.TARGET_ID_WIKIDATA IS NOT NULL) AS WITH_QID,
       SUM(l.IS_REDLINK = 1) AS REDLINKS,
       SUM(l.IS_DATE = 1) AS DATES
FROM tmp_ing t
JOIN T_WC_WIKIPEDIA_PAGE_LANG_LINK l ON l.ID_WIKIDATA = t.ID_WIKIDATA AND l.LANG = t.LANG
GROUP BY COALESCE(l.RESOLVED_BY, '(none)')
ORDER BY LINKS DESC;

-- 5. References 
SELECT 'refs' AS PART, COUNT(*) AS REFS, COUNT(DISTINCT r.ID_WIKIDATA) AS PAGES_WITH_REFS
FROM tmp_ing t
JOIN T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE r ON r.ID_WIKIDATA = t.ID_WIKIDATA AND r.LANG = t.LANG;

-- 6. Images kept (keep_images): rows still there, none rewritten in the window
SELECT 'images' AS PART,
       COUNT(*) AS IMAGE_ROWS,
       COUNT(DISTINCT i.ID_WIKIDATA) AS PAGES_WITH_IMAGES,
       SUM(i.LAST_CRAWLED_AT BETWEEN @t0 AND @t1) AS ROWS_WRITTEN_IN_WINDOW
FROM tmp_ing t
JOIN T_WC_WIKIPEDIA_PAGE_LANG_IMAGE i ON i.ID_WIKIDATA = t.ID_WIKIDATA AND i.LANG = t.LANG;

