-- How well does the section text compress with zlib? (topic wikipedia-rich-sections, CONTENT_MD sizing)
-- Read-only. Same 40 primary-key windows as check-section-content-size.sql; COMPRESS() is the zlib
-- that a COMPRESSED column attribute would use. Sections under 100 bytes are left raw by the column
-- attribute (column_compression_threshold), so they are counted at their raw size.
SET @MIN_ID = (SELECT MIN(ID_ROW) FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION);
SET @STEP = ((SELECT MAX(ID_ROW) FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION) - @MIN_ID) DIV 40;

SELECT s.LANG, COUNT(*) AS SECTIONS,
  ROUND(SUM(LENGTH(s.CONTENT)) / 1048576, 1) AS RAW_MB,
  ROUND(SUM(IF(LENGTH(s.CONTENT) < 100, LENGTH(s.CONTENT), LENGTH(COMPRESS(s.CONTENT)))) / 1048576, 1) AS ZLIB_MB,
  ROUND(SUM(LENGTH(s.CONTENT)) / SUM(IF(LENGTH(s.CONTENT) < 100, LENGTH(s.CONTENT), LENGTH(COMPRESS(s.CONTENT)))), 2) AS RATIO
FROM seq_0_to_39 w
JOIN T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
  ON s.ID_ROW BETWEEN @MIN_ID + w.seq * @STEP AND @MIN_ID + w.seq * @STEP + 9999
GROUP BY s.LANG WITH ROLLUP;

SELECT @@column_compression_threshold AS THRESHOLD, @@column_compression_zlib_level AS ZLIB_LEVEL, @@have_compress AS HAVE_COMPRESS;
