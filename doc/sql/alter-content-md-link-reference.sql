-- CONTENT_MD, link table, reference table (topic wikipedia-rich-sections, decisions of 2026-10-15).
-- DDL, run by Philippe with runsqlvaugouindb.sh. The crawler can keep running: nothing here rewrites
-- an existing table (the column add is instant, the index is built online).
--
-- 1. T_WC_WIKIPEDIA_PAGE_LANG_SECTION.CONTENT_MD: the section as Markdown, links written by Wikipedia
--    title ([words](Title)), compressed like CONTENT (zlib, about 2.3 times on this text).
-- 2. T_WC_WIKIPEDIA_PAGE_LANG_LINK: one row per internal link of a section, resolved to a Qid.
--    IS_DATE flags the French date links ([20](20_octobre) [octobre](Octobre_2009)), kept on purpose.
-- 3. T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE: one row per footnote cited in a section, with its text,
--    since 43 % of references have no URL (books, short citations).
-- 4. An index on T_WC_WIKIPEDIA_PAGE_LANG (LANG, WIKIPEDIA_PAGE_TITLE) for the local title lookup
--    that resolves 84 % of links without the Wikipedia API.
--
-- Both new tables are keyed like the sections, (ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER),
-- so the writer upserts them in place and prunes the tail, as it does for sections.
-- Two deliberate departures from the T_WC_ conventions: ID_ROW is BIGINT (the link table will hold
-- 60 to 120 million rows rewritten on every recrawl, and the section table's AUTO_INCREMENT is already
-- at 217 million), and only the indexes a query needs are created (no index on the audit columns).

-- The ALTERs need a brief metadata lock: never queue readers behind them for long.
SET SESSION lock_wait_timeout = 120;

-- 1. CONTENT_MD, added at the end of the row: instant, no table rewrite (refused rather than rebuilt).
ALTER TABLE T_WC_WIKIPEDIA_PAGE_LANG_SECTION
  ADD COLUMN CONTENT_MD mediumtext COMPRESSED CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  ALGORITHM=INSTANT;

-- 2. Links.
CREATE TABLE IF NOT EXISTS T_WC_WIKIPEDIA_PAGE_LANG_LINK (
  ID_ROW bigint(20) NOT NULL AUTO_INCREMENT,
  ID_WIKIDATA varchar(50) NOT NULL,
  LANG varchar(10) NOT NULL,
  ITEM_TYPE varchar(20) DEFAULT NULL,
  SECTION_DISPLAY_ORDER int(5) NOT NULL,
  DISPLAY_ORDER int(5) NOT NULL,
  ANCHOR varchar(500) DEFAULT NULL,
  TARGET_TITLE varchar(300) NOT NULL,
  FRAGMENT varchar(300) DEFAULT NULL,
  TARGET_FINAL_TITLE varchar(300) DEFAULT NULL,
  VIA_REDIRECT tinyint(1) DEFAULT 0,
  IS_REDLINK tinyint(1) DEFAULT 0,
  IS_DATE tinyint(1) DEFAULT 0,
  TARGET_ID_WIKIDATA varchar(50) DEFAULT NULL,
  RESOLVED_BY varchar(20) DEFAULT NULL,
  DELETED int(5) DEFAULT NULL,
  ID_CREATOR int(5) DEFAULT NULL,
  DAT_CREAT date DEFAULT NULL,
  ID_OWNER int(5) DEFAULT NULL,
  TIM_UPDATED datetime DEFAULT NULL,
  ID_USER_UPDATED int(5) DEFAULT NULL,
  PRIMARY KEY (ID_ROW),
  UNIQUE KEY UQ_ID_WIKIDATA_LANG_SECTION_DISPLAY_ORDER (ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER),
  KEY TARGET_ID_WIKIDATA (TARGET_ID_WIKIDATA),
  KEY LANG_TARGET_TITLE (LANG, TARGET_TITLE),
  KEY ITEM_TYPE (ITEM_TYPE),
  KEY TIM_UPDATED (TIM_UPDATED)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. References.
CREATE TABLE IF NOT EXISTS T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE (
  ID_ROW bigint(20) NOT NULL AUTO_INCREMENT,
  ID_WIKIDATA varchar(50) NOT NULL,
  LANG varchar(10) NOT NULL,
  ITEM_TYPE varchar(20) DEFAULT NULL,
  SECTION_DISPLAY_ORDER int(5) NOT NULL,
  DISPLAY_ORDER int(5) NOT NULL,
  NOTE_KEY varchar(300) DEFAULT NULL,
  LABEL varchar(50) DEFAULT NULL,
  URL text DEFAULT NULL,
  TITLE varchar(1000) DEFAULT NULL,
  REFERENCE_TEXT mediumtext COMPRESSED CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  DELETED int(5) DEFAULT NULL,
  ID_CREATOR int(5) DEFAULT NULL,
  DAT_CREAT date DEFAULT NULL,
  ID_OWNER int(5) DEFAULT NULL,
  TIM_UPDATED datetime DEFAULT NULL,
  ID_USER_UPDATED int(5) DEFAULT NULL,
  PRIMARY KEY (ID_ROW),
  UNIQUE KEY UQ_ID_WIKIDATA_LANG_SECTION_DISPLAY_ORDER (ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER),
  KEY ITEM_TYPE (ITEM_TYPE),
  KEY TIM_UPDATED (TIM_UPDATED)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. Title lookup index, built online (reads and writes carry on).
ALTER TABLE T_WC_WIKIPEDIA_PAGE_LANG
  ADD INDEX LANG_WIKIPEDIA_PAGE_TITLE (LANG, WIKIPEDIA_PAGE_TITLE),
  ALGORITHM=INPLACE, LOCK=NONE;

-- 5. Check: the new column, the two new tables, the new index.
SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, IS_NULLABLE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_SECTION' AND COLUMN_NAME IN ('CONTENT', 'CONTENT_MD');

SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, TABLE_ROWS
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('T_WC_WIKIPEDIA_PAGE_LANG_LINK', 'T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE');

SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG' AND INDEX_NAME = 'LANG_WIKIPEDIA_PAGE_TITLE'
ORDER BY SEQ_IN_INDEX;

SHOW CREATE TABLE T_WC_WIKIPEDIA_PAGE_LANG_LINK;
SHOW CREATE TABLE T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE;
