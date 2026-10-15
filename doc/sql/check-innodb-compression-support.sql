-- Can the section table use InnoDB compression? (topic wikipedia-rich-sections, CONTENT_MD sizing)
-- Read-only: server version and the compression settings and providers available.
SELECT VERSION() AS VERSION, @@innodb_page_size AS PAGE_SIZE, @@innodb_file_per_table AS FILE_PER_TABLE,
  @@innodb_compression_algorithm AS PAGE_COMPRESSION_ALGO, @@innodb_compression_default AS COMPRESSION_DEFAULT,
  @@innodb_compression_level AS COMPRESSION_LEVEL;
SELECT PLUGIN_NAME, PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME LIKE 'provider%';
SELECT TABLE_NAME, ROW_FORMAT, CREATE_OPTIONS FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND (CREATE_OPTIONS LIKE '%compress%' OR ROW_FORMAT = 'Compressed');
