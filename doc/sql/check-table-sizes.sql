-- Disk held by the database, table by table (incident of 2026-10-20: the disk
-- filled up during the enwiki dump ingest). Read-only. DATA_LENGTH and
-- INDEX_LENGTH are InnoDB estimates, close enough to rank tables; DATA_FREE is
-- space allocated to the file but unused (rows deleted, never given back).
-- Run again after a few chunks to see how fast each table grows.

SELECT NOW() AS NOW_;

SELECT CASE WHEN TABLE_NAME LIKE 'T\_WC\_WIKIPEDIA%' THEN 'Wikipedia'
            WHEN TABLE_NAME LIKE 'T\_WC\_WIKIDATA%' THEN 'Wikidata'
            WHEN TABLE_NAME LIKE 'STG\_%' THEN 'Wikidata staging'
            WHEN TABLE_NAME LIKE 'T\_WC\_TMDB%' THEN 'TMDb'
            WHEN TABLE_NAME LIKE 'T\_WC\_T2S%' THEN 'T2S'
            ELSE 'other' END AS FAMILY,
       COUNT(*) AS TABLES_,
       ROUND(SUM(DATA_LENGTH + INDEX_LENGTH) / POW(1024, 3), 1) AS GB,
       ROUND(SUM(DATA_FREE) / POW(1024, 3), 1) AS FREE_GB
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'vaugouindb'
GROUP BY FAMILY
ORDER BY GB DESC;

SELECT TABLE_NAME, TABLE_ROWS, ROW_FORMAT,
       ROUND(DATA_LENGTH / POW(1024, 3), 1) AS DATA_GB,
       ROUND(INDEX_LENGTH / POW(1024, 3), 1) AS INDEX_GB,
       ROUND(DATA_FREE / POW(1024, 3), 1) AS FREE_GB,
       CREATE_TIME, UPDATE_TIME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'vaugouindb'
ORDER BY DATA_LENGTH + INDEX_LENGTH DESC
LIMIT 25;

SELECT VAR_NAME, VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE VAR_NAME LIKE 'strwikipediadumpingest%'
ORDER BY VAR_NAME;
