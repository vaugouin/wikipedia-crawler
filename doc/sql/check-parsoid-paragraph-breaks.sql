-- Wikipedia switched action=parse to Parsoid HTML around 2026-10-15; since then the crawler wrote
-- paragraphs without the blank line between them ("\n \n"), fixed in 1.0.22. Per hour of crawl:
-- multi-paragraph sections that still carry a paragraph break, to date the switch and size the
-- pages to recrawl. Read-only.
SELECT DATE_FORMAT(s.TIM_UPDATED, '%Y-%m-%d %H:00') AS HOUR_UPDATED,
  COUNT(DISTINCT s.ID_WIKIDATA, s.LANG) AS PAGES,
  COUNT(*) AS LONG_SECTIONS,
  SUM(LOCATE(CONCAT(CHAR(10), ' ', CHAR(10)), s.CONTENT) > 0) AS WITH_PARAGRAPH_BREAK
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
WHERE s.TIM_UPDATED >= '2026-10-14 00:00:00'
  AND LENGTH(s.CONTENT) > 1500
GROUP BY HOUR_UPDATED
ORDER BY HOUR_UPDATED;
