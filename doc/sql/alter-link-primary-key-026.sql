-- DDL, run by Philippe (runsqlvaugouindb.sh): WIKIPEDIA-CRAWLER-026, point 3, step B
-- (decided by Philippe on 2026-10-24).
--
-- The unique key (ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER) becomes the
-- primary key, and the surrogate ID_ROW goes. No code reads ID_ROW on this table (audit of
-- 2026-10-21); the upsert (INSERT ... ON DUPLICATE KEY UPDATE) matches on any unique key,
-- so it behaves the same; the four columns are NOT NULL. One B-tree disappears, and the
-- per-page reads of the API and the front become direct reads of the clustered index.
-- The two unread indexes of step A go in the same rebuild if they are still there.
-- TIM_UPDATED stays (one reader, the data-monitoring metric).
--
-- BEFORE RUNNING:
--   * free disk: the rebuild writes a full copy of the table before dropping the old
--     one; check-link-indexes-026.sql gives its size, `df -h /` must show at least that
--     much plus the guard margin (40 GB);
--   * the Wikipedia crawler and any ingest stopped: concurrent writes are logged during
--     the rebuild and abort it beyond innodb_online_alter_log_max_size;
--   * expect tens of minutes to a few hours on about 100 M rows; runsqlvaugouindb.sh has
--     no time limit.
-- If the server refuses LOCK=NONE for this combination, the statement fails at once and
-- nothing changes: then rerun with LOCK=SHARED (reads go on, writes wait).

SELECT ROUND(DATA_LENGTH / POW(1024, 3), 1) AS DATA_GB,
       ROUND(INDEX_LENGTH / POW(1024, 3), 1) AS INDEX_GB,
       ROUND(DATA_FREE / POW(1024, 3), 1) AS FREE_GB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'vaugouindb' AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_LINK';

ALTER TABLE T_WC_WIKIPEDIA_PAGE_LANG_LINK
  DROP PRIMARY KEY,
  DROP COLUMN ID_ROW,
  ADD PRIMARY KEY (ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER),
  DROP INDEX UQ_ID_WIKIDATA_LANG_SECTION_DISPLAY_ORDER,
  DROP INDEX IF EXISTS LANG_TARGET_TITLE,
  DROP INDEX IF EXISTS ITEM_TYPE,
  ALGORITHM=INPLACE, LOCK=NONE;

ANALYZE TABLE T_WC_WIKIPEDIA_PAGE_LANG_LINK;

SHOW INDEX FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK;

SELECT ROUND(DATA_LENGTH / POW(1024, 3), 1) AS DATA_GB,
       ROUND(INDEX_LENGTH / POW(1024, 3), 1) AS INDEX_GB,
       ROUND(DATA_FREE / POW(1024, 3), 1) AS FREE_GB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'vaugouindb' AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_LINK';

EXPLAIN SELECT TARGET_ID_WIKIDATA FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK
WHERE ID_WIKIDATA = 'Q25188' AND LANG = 'en';
