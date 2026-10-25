-- DDL, run by Philippe (runsqlvaugouindb.sh): WIKIPEDIA-CRAWLER-026, point 3.
--
-- T_WC_WIKIPEDIA_PAGE_LANG_LINK held 12.8 GB of data for 20.9 GB of indexes on 2026-10-20
-- (check-table-sizes-20261020.txt). The audit of 2026-10-21 (every query on the table in
-- wikipedia-crawler, manual-refresh, fastapi-text2sql, tmdb-front, data-monitoring) found
-- no reader for two of them:
--   LANG_TARGET_TITLE (LANG, TARGET_TITLE): link resolution looks titles up in
--     T_WC_WIKIPEDIA_PAGE_LANG, never here; TARGET_TITLE is only written and selected.
--   ITEM_TYPE: only written; the data-monitoring manifest says "Not filtered on ITEM_TYPE".
-- Their creation DDL (alter-content-md-link-reference.sql) already meant "no index on the
-- audit columns".
--
-- Kept: the unique key (the upsert conflict key and every per-page read), TARGET_LANG_SOURCE
-- (the "mentioned in" lookup), TIM_UPDATED (read by the data-monitoring metric
-- content_md_link_resolution_last_day; its fate is a separate decision).
--
-- Dropping a secondary index is INPLACE with no rebuild: quick, the crawler keeps writing.
-- The freed pages stay in the .ibd file as DATA_FREE, reused by the table's growth; the file
-- itself only shrinks with a full rebuild, which needs as much free disk as the table.

SELECT ROUND(DATA_LENGTH / POW(1024, 3), 1) AS DATA_GB,
       ROUND(INDEX_LENGTH / POW(1024, 3), 1) AS INDEX_GB,
       ROUND(DATA_FREE / POW(1024, 3), 1) AS FREE_GB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'vaugouindb' AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_LINK';

ALTER TABLE T_WC_WIKIPEDIA_PAGE_LANG_LINK
  DROP INDEX LANG_TARGET_TITLE,
  DROP INDEX ITEM_TYPE,
  ALGORITHM=INPLACE, LOCK=NONE;

ANALYZE TABLE T_WC_WIKIPEDIA_PAGE_LANG_LINK;

SHOW INDEX FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK;

SELECT ROUND(DATA_LENGTH / POW(1024, 3), 1) AS DATA_GB,
       ROUND(INDEX_LENGTH / POW(1024, 3), 1) AS INDEX_GB,
       ROUND(DATA_FREE / POW(1024, 3), 1) AS FREE_GB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'vaugouindb' AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_LINK';
