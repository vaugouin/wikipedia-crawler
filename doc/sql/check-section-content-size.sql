-- Size of the Wikipedia section text today, to estimate what a CONTENT_MD column would add (topic wikipedia-rich-sections).
-- Read-only.

-- 1. On-disk footprint of the tables concerned (InnoDB estimates; DATA_LENGTH includes off-page mediumtext).
SELECT TABLE_NAME, ENGINE, ROW_FORMAT, TABLE_ROWS,
  ROUND(DATA_LENGTH / 1048576) AS DATA_MB, ROUND(INDEX_LENGTH / 1048576) AS INDEX_MB,
  ROUND(DATA_FREE / 1048576) AS FREE_MB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('T_WC_WIKIPEDIA_PAGE_LANG', 'T_WC_WIKIPEDIA_PAGE_LANG_SECTION', 'T_WC_WIKIPEDIA_PAGE_LANG_IMAGE');

-- 2. Bytes of CONTENT on 40 primary-key windows of 10,000 IDs spread evenly over the ID range:
--    the full SUM exceeded the agent's 600 s statement cap on 2026-10-14, and the two end slices
--    first tried were skewed (oldest rows: one short section per page; newest IDs: sparse).
--    Extrapolate with TABLE_ROWS above.
SET @MIN_ID = (SELECT MIN(ID_ROW) FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION);
SET @STEP = ((SELECT MAX(ID_ROW) FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION) - @MIN_ID) DIV 40;

SELECT COUNT(*) AS SECTIONS, COUNT(DISTINCT s.ID_WIKIDATA, s.LANG) AS PAGES,
  SUM(s.DELETED = 1) AS DELETED_SECTIONS,
  ROUND(AVG(LENGTH(s.CONTENT))) AS AVG_SECTION_BYTES,
  ROUND(SUM(LENGTH(s.CONTENT)) / COUNT(DISTINCT s.ID_WIKIDATA, s.LANG)) AS AVG_PAGE_BYTES,
  ROUND(SUM(LENGTH(s.CONTENT) > 8000) / COUNT(*) * 100, 1) AS PCT_OVER_8KB
FROM seq_0_to_39 w
JOIN T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
  ON s.ID_ROW BETWEEN @MIN_ID + w.seq * @STEP AND @MIN_ID + w.seq * @STEP + 9999;

SELECT s.LANG, COUNT(*) AS SECTIONS, COUNT(DISTINCT s.ID_WIKIDATA) AS PAGES,
  ROUND(SUM(LENGTH(s.CONTENT)) / COUNT(DISTINCT s.ID_WIKIDATA)) AS AVG_PAGE_BYTES
FROM seq_0_to_39 w
JOIN T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
  ON s.ID_ROW BETWEEN @MIN_ID + w.seq * @STEP AND @MIN_ID + w.seq * @STEP + 9999
GROUP BY s.LANG;

-- Sections per page and bytes per page over the whole table, from the TITLE-free page count
-- (index-only scan on the UNIQUE key, no CONTENT read).
SELECT COUNT(*) AS PAGES_WITH_SECTIONS FROM (
  SELECT ID_WIKIDATA, LANG FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION GROUP BY ID_WIKIDATA, LANG
) x;

-- 3. Pages crawled in all, for the scale of a link table.
SELECT LANG, COUNT(*) AS PAGES FROM T_WC_WIKIPEDIA_PAGE_LANG GROUP BY LANG WITH ROLLUP;
