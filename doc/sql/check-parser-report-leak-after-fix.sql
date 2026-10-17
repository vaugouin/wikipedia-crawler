-- Read-only, run by the agent after fix-parser-report-leak.sql: the one section left with an empty
-- CONTENT_MD (from the cut, or older?), and whether the crawler, restarted on 1.1.3, still writes
-- the report (expected: 0 since 2026-10-17 12:00).

SELECT 'empty_md' AS PART, ID_WIKIDATA, LANG, ITEM_TYPE, DISPLAY_ORDER, TITLE, TIM_UPDATED,
       CHAR_LENGTH(CONTENT) AS LEN_CONTENT, LEFT(CONTENT, 120) AS CONTENT_START
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION
WHERE TIM_UPDATED >= '2026-10-14' AND CONTENT_MD IS NOT NULL AND TRIM(CONTENT_MD) = '';

SELECT 'since_restart' AS PART, COUNT(*) AS SECTIONS, COUNT(DISTINCT ID_WIKIDATA, LANG) AS PAGES,
       SUM(CONTENT_MD LIKE '%NewPP limit report%' OR CONTENT LIKE '%NewPP limit report%') AS WITH_REPORT,
       MIN(TIM_UPDATED) AS FIRST, MAX(TIM_UPDATED) AS LAST
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION
WHERE TIM_UPDATED >= '2026-10-17 12:00';
