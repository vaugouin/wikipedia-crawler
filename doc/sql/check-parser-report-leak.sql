-- Read-only, run by the agent: how many sections carry the "NewPP limit report" comment that
-- wikipedia_content_md.py 1.1.0 to 1.1.2 rendered as text (fixed in 1.1.3). action=parse ends the
-- page with that HTML comment, so the report lands at the end of the page's last section, in
-- CONTENT_MD and in CONTENT (repaired from the Markdown). CONTENT_MD exists since 2026-10-15, hence
-- the TIM_UPDATED bound. Also checks what a cleanup must rely on: the report is in the last
-- section only, once, and a section whose whole content is the report would have to be deleted.

CREATE TEMPORARY TABLE tmp_hit AS
SELECT s.ID_ROW, s.ID_WIKIDATA, s.LANG, s.ITEM_TYPE, s.DISPLAY_ORDER, s.TIM_UPDATED,
       LOCATE('NewPP limit report', s.CONTENT_MD) AS POS_MD,
       LOCATE('NewPP limit report', s.CONTENT) AS POS_TXT,
       CHAR_LENGTH(s.CONTENT_MD) - CHAR_LENGTH(REPLACE(s.CONTENT_MD, 'NewPP limit report', '')) AS N_MARKERS_X18,
       TRIM(SUBSTRING(s.CONTENT_MD, 1, GREATEST(LOCATE('NewPP limit report', s.CONTENT_MD) - 1, 0))) = '' AS ONLY_REPORT,
       RIGHT(TRIM(s.CONTENT_MD), 60) AS TAIL_MD
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
WHERE s.TIM_UPDATED >= '2026-10-14'
  AND (s.CONTENT_MD LIKE '%NewPP limit report%' OR s.CONTENT LIKE '%NewPP limit report%');

SELECT 'scanned' AS PART, COUNT(*) AS SECTIONS_SINCE_2026_10_14, SUM(CONTENT_MD IS NOT NULL) AS WITH_MD,
       COUNT(DISTINCT ID_WIKIDATA, LANG) AS PAGES
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION
WHERE TIM_UPDATED >= '2026-10-14';

SELECT 'hits' AS PART, LANG, ITEM_TYPE, COUNT(*) AS SECTIONS, COUNT(DISTINCT ID_WIKIDATA) AS PAGES,
       SUM(POS_MD > 0) AS IN_MD, SUM(POS_TXT > 0) AS IN_CONTENT, SUM(ONLY_REPORT) AS WHOLE_SECTION_IS_REPORT,
       SUM(N_MARKERS_X18 > 18) AS MORE_THAN_ONE_REPORT, MIN(TIM_UPDATED) AS FIRST_SEEN, MAX(TIM_UPDATED) AS LAST_SEEN
FROM tmp_hit
GROUP BY LANG, ITEM_TYPE WITH ROLLUP;

-- A cleanup cuts at the marker: it must be the page's last section (expected: 0 rows here).
SELECT 'not_last_section' AS PART, COUNT(*) AS SECTIONS
FROM tmp_hit h
WHERE EXISTS (SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
              WHERE s.ID_WIKIDATA = h.ID_WIKIDATA AND s.LANG = h.LANG AND s.DISPLAY_ORDER > h.DISPLAY_ORDER
                AND COALESCE(s.DELETED, 0) = 0);

-- And nothing but the report after it: how the sections end (expected: the "api-parse" cache line).
SELECT 'tails' AS PART, SUBSTRING_INDEX(TAIL_MD, ' ', -3) AS ENDS_WITH, COUNT(*) AS SECTIONS
FROM tmp_hit
WHERE POS_MD > 0
GROUP BY SUBSTRING_INDEX(TAIL_MD, ' ', -3)
ORDER BY SECTIONS DESC
LIMIT 10;

-- Links or references filed under a section that is only the report (expected: 0).
SELECT 'rows_under_report_sections' AS PART,
       (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK k JOIN tmp_hit h
          ON k.ID_WIKIDATA = h.ID_WIKIDATA AND k.LANG = h.LANG AND k.SECTION_DISPLAY_ORDER = h.DISPLAY_ORDER
        WHERE h.ONLY_REPORT = 1) AS LINKS;
