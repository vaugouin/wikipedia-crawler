-- DDL, run by Philippe (runsqlvaugouindb.sh): the index behind the "mentioned in" lookup.
--
-- tmdb-front 1.0.9, fastapi-text2sql and voice-agent read T_WC_WIKIPEDIA_PAGE_LANG_LINK
-- backwards: which pages link TO a given entity, in one language
--   SELECT DISTINCT ID_WIKIDATA FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK
--   WHERE TARGET_ID_WIKIDATA = ? AND LANG = ?
-- On the index TARGET_ID_WIKIDATA alone, every matching row is read from the table to test
-- LANG and fetch ID_WIKIDATA (check-link-backward-volume-20261016.txt: 36,268 rows for Q30,
-- "Using index condition; Using where; Using temporary"), with 3.5 % of the pages crawled.
-- The covering index (TARGET_ID_WIKIDATA, LANG, ID_WIKIDATA) answers it from the index alone,
-- in order. It replaces TARGET_ID_WIKIDATA, which is its prefix.
--
-- Built now, on about 4.5 M rows, rather than after the backfill (100 M rows or more).
-- INPLACE and LOCK=NONE: the crawler keeps writing while the index builds.

ALTER TABLE T_WC_WIKIPEDIA_PAGE_LANG_LINK
  ADD INDEX TARGET_LANG_SOURCE (TARGET_ID_WIKIDATA, LANG, ID_WIKIDATA),
  ALGORITHM=INPLACE, LOCK=NONE;

ALTER TABLE T_WC_WIKIPEDIA_PAGE_LANG_LINK
  DROP INDEX TARGET_ID_WIKIDATA,
  ALGORITHM=INPLACE, LOCK=NONE;

SHOW INDEX FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK;

EXPLAIN SELECT DISTINCT ID_WIKIDATA FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK WHERE TARGET_ID_WIKIDATA = 'Q30' AND LANG = 'en';
