-- Run by Philippe (writes T_WC_WIKIPEDIA_PAGE_LANG_SECTION), AFTER the crawler runs 1.1.3:
-- removes the "NewPP limit report" that wikipedia_content_md.py 1.1.0 to 1.1.2 wrote as text at
-- the end of each page's last section. Measured by check-parser-report-leak.sql on 2026-10-17:
-- 96,302 sections (one per page), the report always in the page's last section, once, and
-- always running to the end of it; 19,524 of those sections hold nothing but the report (a
-- section the converter would have dropped as empty) and own no links.
-- 1. delete the sections that are only the report; 2. cut the others at the marker, in CONTENT_MD
-- and CONTENT alike, trailing blank space included. TIM_UPDATED is left as it was.

SELECT 'before' AS STEP, COUNT(*) AS SECTIONS_WITH_REPORT
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION
WHERE TIM_UPDATED >= '2026-10-14'
  AND (CONTENT_MD LIKE '%NewPP limit report%' OR CONTENT LIKE '%NewPP limit report%');

DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION
WHERE TIM_UPDATED >= '2026-10-14'
  AND CONTENT_MD LIKE '%NewPP limit report%'
  AND TRIM(LEFT(CONTENT_MD, LOCATE('NewPP limit report', CONTENT_MD) - 1)) = '';
SELECT 'deleted' AS STEP, ROW_COUNT() AS SECTIONS;

UPDATE T_WC_WIKIPEDIA_PAGE_LANG_SECTION
SET CONTENT_MD = IF(LOCATE('NewPP limit report', CONTENT_MD) > 0,
                    REGEXP_REPLACE(LEFT(CONTENT_MD, LOCATE('NewPP limit report', CONTENT_MD) - 1), '[[:space:]]+$', ''),
                    CONTENT_MD),
    CONTENT = IF(LOCATE('NewPP limit report', CONTENT) > 0,
                 REGEXP_REPLACE(LEFT(CONTENT, LOCATE('NewPP limit report', CONTENT) - 1), '[[:space:]]+$', ''),
                 CONTENT),
    TIM_UPDATED = TIM_UPDATED
WHERE TIM_UPDATED >= '2026-10-14'
  AND (CONTENT_MD LIKE '%NewPP limit report%' OR CONTENT LIKE '%NewPP limit report%');
SELECT 'cut' AS STEP, ROW_COUNT() AS SECTIONS;

-- Expected: 0, and no section left empty by the cut.
SELECT 'after' AS STEP, COUNT(*) AS SECTIONS_WITH_REPORT,
       (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION
        WHERE TIM_UPDATED >= '2026-10-14' AND CONTENT_MD IS NOT NULL AND TRIM(CONTENT_MD) = '') AS EMPTY_MD_SECTIONS
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION
WHERE TIM_UPDATED >= '2026-10-14'
  AND (CONTENT_MD LIKE '%NewPP limit report%' OR CONTENT LIKE '%NewPP limit report%');
