-- DDL, run by Philippe (runsqlvaugouindb.sh): WIKIPEDIA-CRAWLER-026, point 3, step B
-- applied to the footnote table (asked by Philippe on 2026-10-24, after the link table
-- went through alter-link-primary-key-026.sql the same day).
--
-- Same shape as the link table: the unique key (ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER,
-- DISPLAY_ORDER) becomes the primary key and the surrogate ID_ROW goes. Audit of
-- 2026-10-24: no code reads ID_ROW; the writer upserts on the unique key
-- (f_sqlbulkupsert) and deletes by page; the two readers, the API (main.py,
-- section footnotes) and the front (f_wikipediareferences), both read
-- WHERE ID_WIKIDATA = ? AND LANG = ? ORDER BY SECTION_DISPLAY_ORDER, DISPLAY_ORDER,
-- which is exactly the new clustered order. ITEM_TYPE is written but never read, so its
-- index goes in the same rebuild. TIM_UPDATED stays, as on the link table.
--
-- BEFORE RUNNING:
--   * free disk: the rebuild writes a full copy of the table (about 8.5 GB on
--     2026-10-24) before dropping the old one; `df -h /` must show that plus the guard
--     margin (40 GB);
--   * the Wikipedia crawler and any ingest stopped;
--   * expect a few minutes to an hour on about 19 M rows.
-- If the server refuses LOCK=NONE, the statement fails at once and nothing changes:
-- then rerun with LOCK=SHARED (reads go on, writes wait).

SELECT ROUND(DATA_LENGTH / POW(1024, 3), 1) AS DATA_GB,
       ROUND(INDEX_LENGTH / POW(1024, 3), 1) AS INDEX_GB,
       ROUND(DATA_FREE / POW(1024, 3), 1) AS FREE_GB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'vaugouindb' AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE';

ALTER TABLE T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE
  DROP PRIMARY KEY,
  DROP COLUMN ID_ROW,
  ADD PRIMARY KEY (ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER),
  DROP INDEX UQ_ID_WIKIDATA_LANG_SECTION_DISPLAY_ORDER,
  DROP INDEX IF EXISTS ITEM_TYPE,
  ALGORITHM=INPLACE, LOCK=NONE;

ANALYZE TABLE T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE;

SHOW INDEX FROM T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE;

SELECT ROUND(DATA_LENGTH / POW(1024, 3), 1) AS DATA_GB,
       ROUND(INDEX_LENGTH / POW(1024, 3), 1) AS INDEX_GB,
       ROUND(DATA_FREE / POW(1024, 3), 1) AS FREE_GB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'vaugouindb' AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE';

EXPLAIN SELECT SECTION_DISPLAY_ORDER, LABEL, URL, TITLE, REFERENCE_TEXT
FROM T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE
WHERE ID_WIKIDATA = 'Q25188' AND LANG = 'en'
ORDER BY SECTION_DISPLAY_ORDER, DISPLAY_ORDER;
