-- Voie 2 (Philippe, runsqlvaugouindb.sh), JUST BEFORE re-ingesting the English chunks
-- 360 to 365 (WIKIPEDIA-CRAWLER-026, follow-up of the disk-full crash of 2026-10-20).
--
-- check-content-md-crash-window-20261025.txt: every English page the snapshot ingest
-- stamped from 2026-10-20 09:36:26 to the crash (about 10:12) lost its rewrite. About
-- 5 150 have no CONTENT_MD at all and about 900 more have their sections but neither
-- links nor footnotes. The ingest log puts writer errors on chunks 360 to 365, and
-- counted them done anyway.
--
-- Why this reset is needed: wikipedia_dump_ingest.py skips a page when db_is_fresher()
-- says the base already holds a newer Markdown copy (LAST_SUCCESS_AT after the record's
-- revision AND one section with CONTENT_MD). The 5 150 pages without CONTENT_MD would be
-- taken again anyway; the ~900 half-written ones would be skipped, since their
-- LAST_SUCCESS_AT of 2026-10-20 is later than the snapshot of 2026-10-01. Setting it to
-- NULL for the window makes the re-ingest take every page of it. The re-ingest writes
-- it back. Until then the API shows no "updated_at" for those pages: run this file only
-- right before the re-ingest.

SELECT COUNT(*) AS PAGES_IN_WINDOW, MIN(LAST_CRAWLED_AT) AS FIRST_, MAX(LAST_CRAWLED_AT) AS LAST_
FROM T_WC_WIKIPEDIA_PAGE_LANG
WHERE LANG = 'en'
  AND LAST_CRAWLED_AT >= '2026-10-20 09:36:00' AND LAST_CRAWLED_AT < '2026-10-20 12:00:00';

UPDATE T_WC_WIKIPEDIA_PAGE_LANG
SET LAST_SUCCESS_AT = NULL
WHERE LANG = 'en'
  AND LAST_CRAWLED_AT >= '2026-10-20 09:36:00' AND LAST_CRAWLED_AT < '2026-10-20 12:00:00';

SELECT ROW_COUNT() AS ROWS_RESET;

SELECT COUNT(*) AS STILL_WITH_LAST_SUCCESS
FROM T_WC_WIKIPEDIA_PAGE_LANG
WHERE LANG = 'en'
  AND LAST_CRAWLED_AT >= '2026-10-20 09:36:00' AND LAST_CRAWLED_AT < '2026-10-20 12:00:00'
  AND LAST_SUCCESS_AT IS NOT NULL;
