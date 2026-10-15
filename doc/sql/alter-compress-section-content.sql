-- Compress T_WC_WIKIPEDIA_PAGE_LANG_SECTION.CONTENT with MariaDB column compression (zlib),
-- topic wikipedia-rich-sections, decided by Philippe on 2026-10-15.
-- Pre-check: check-compress-section-content.sql (2,000 sections round-tripped byte for byte, 0 mismatch).
-- Transparent to readers: MariaDB decompresses on read. Measured zlib ratio on the text: 2.34.
-- The ALTER rewrites the whole table (about 23.5 GB): stop wikipedia-crawler first, and keep
-- about 10 GB free on the Docker data volume while the old and new copies coexist.
-- DDL, run by Philippe with runsqlvaugouindb.sh (no 600 s cap there).

-- 1. Before: footprint (statistics and real file size) and a checksum on 40 primary-key windows.
SELECT 'before' AS STEP, TABLE_ROWS, ROUND(DATA_LENGTH / 1048576) AS DATA_MB,
  ROUND(INDEX_LENGTH / 1048576) AS INDEX_MB, ROUND(DATA_FREE / 1048576) AS FREE_MB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_SECTION';

SELECT 'before' AS STEP, ROUND(FILE_SIZE / 1048576) AS FILE_MB, ROUND(ALLOCATED_SIZE / 1048576) AS ALLOCATED_MB
FROM information_schema.INNODB_SYS_TABLESPACES
WHERE NAME = CONCAT(DATABASE(), '/T_WC_WIKIPEDIA_PAGE_LANG_SECTION');

SET @MIN_ID = (SELECT MIN(ID_ROW) FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION);
SET @STEP = ((SELECT MAX(ID_ROW) FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION) - @MIN_ID) DIV 40;

SELECT 'before' AS STEP, COUNT(*) AS SECTIONS, SUM(LENGTH(s.CONTENT)) AS RAW_BYTES, SUM(CRC32(s.CONTENT)) AS CHECKSUM
FROM seq_0_to_39 w
JOIN T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
  ON s.ID_ROW BETWEEN @MIN_ID + w.seq * @STEP AND @MIN_ID + w.seq * @STEP + 9999;

-- 2. The change: same type, charset, collation and default, plus COMPRESSED.
--    LOCK=SHARED keeps the table readable (API, front) during the copy; writes wait.
--    The ALTER needs an exclusive metadata lock twice, briefly, at the start and at the final swap.
--    While it waits for one, every new reader queues behind it: the API and the front freeze.
--    The default wait is a day; 120 s caps the freeze, and the ALTER fails (rerun later) instead of
--    hanging behind a long query or a backup (mariadb-dump --single-transaction holds the lock).
SET SESSION lock_wait_timeout = 120;
ALTER TABLE T_WC_WIKIPEDIA_PAGE_LANG_SECTION
  MODIFY CONTENT mediumtext COMPRESSED CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  ALGORITHM=COPY, LOCK=SHARED;

ANALYZE TABLE T_WC_WIKIPEDIA_PAGE_LANG_SECTION;

-- 3. After: the same three measures. SECTIONS, RAW_BYTES and CHECKSUM must equal the "before" line.
SELECT 'after' AS STEP, TABLE_ROWS, ROUND(DATA_LENGTH / 1048576) AS DATA_MB,
  ROUND(INDEX_LENGTH / 1048576) AS INDEX_MB, ROUND(DATA_FREE / 1048576) AS FREE_MB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_SECTION';

SELECT 'after' AS STEP, ROUND(FILE_SIZE / 1048576) AS FILE_MB, ROUND(ALLOCATED_SIZE / 1048576) AS ALLOCATED_MB
FROM information_schema.INNODB_SYS_TABLESPACES
WHERE NAME = CONCAT(DATABASE(), '/T_WC_WIKIPEDIA_PAGE_LANG_SECTION');

SELECT 'after' AS STEP, COUNT(*) AS SECTIONS, SUM(LENGTH(s.CONTENT)) AS RAW_BYTES, SUM(CRC32(s.CONTENT)) AS CHECKSUM
FROM seq_0_to_39 w
JOIN T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
  ON s.ID_ROW BETWEEN @MIN_ID + w.seq * @STEP AND @MIN_ID + w.seq * @STEP + 9999;

SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, IS_NULLABLE, COLUMN_DEFAULT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG_SECTION' AND COLUMN_NAME = 'CONTENT';

SHOW CREATE TABLE T_WC_WIKIPEDIA_PAGE_LANG_SECTION;
