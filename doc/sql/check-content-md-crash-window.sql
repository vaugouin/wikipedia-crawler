-- Which English pages lost their CONTENT_MD rewrite in the disk-full crash of 2026-10-20
-- (MariaDB down around 10:12, Paris time), and is the loss confined to a time window?
-- Asked by Philippe on 2026-10-25, after check-page-content-md-20261025.txt showed
-- Inception (Q25188): page row stamped 2026-10-20 09:56 by the snapshot ingest, sections
-- still the plain text of 2026-09-30, no link and no footnote row. Read-only (voie 1).
--
-- LAST_CRAWLED_AT is stamped on the page row (wikipedia_page_writer.py); the sections,
-- links and footnotes are written after it. A page whose row was stamped but whose
-- sections were not rewritten is the signature of the loss.
--
-- A uses a cheap proxy over every English page: no link row and no footnote row, read on
-- the primary keys (ID_WIKIDATA, LANG, ...) since WIKIPEDIA-CRAWLER-026. B checks
-- CONTENT_MD itself, but only on the pages stamped around the crash.

-- A. English pages per hour of their last visit, and how many have neither links nor
--    footnotes (proxy for "no CONTENT_MD"), from 2026-10-19 (start of the enwiki ingest)
SELECT DATE_FORMAT(p.LAST_CRAWLED_AT, '%Y-%m-%d %H:00') AS HOUR_,
       COUNT(*) AS PAGES,
       SUM(NOT EXISTS (SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK l
                       WHERE l.ID_WIKIDATA = p.ID_WIKIDATA AND l.LANG = p.LANG)
           AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE r
                           WHERE r.ID_WIKIDATA = p.ID_WIKIDATA AND r.LANG = p.LANG)) AS NO_LINK_NO_REF
FROM T_WC_WIKIPEDIA_PAGE_LANG p
WHERE p.LANG = 'en' AND p.PAGE_EXISTS = 1
  AND p.LAST_CRAWLED_AT >= '2026-10-19 00:00:00'
GROUP BY HOUR_
ORDER BY HOUR_;

-- A2. The same proxy for the pages last visited before the ingest, by day, for scale
SELECT DATE(p.LAST_CRAWLED_AT) AS DAY_, COUNT(*) AS PAGES
FROM T_WC_WIKIPEDIA_PAGE_LANG p
WHERE p.LANG = 'en' AND p.PAGE_EXISTS = 1
  AND (p.LAST_CRAWLED_AT < '2026-10-19 00:00:00' OR p.LAST_CRAWLED_AT IS NULL)
GROUP BY DAY_
ORDER BY DAY_ DESC
LIMIT 15;

-- B. Around the crash, per 10 minutes: pages stamped, and how many have no section with
--    CONTENT_MD, plus the last write of their sections (a 2026-09 date means not rewritten)
SELECT CONCAT(LEFT(DATE_FORMAT(p.LAST_CRAWLED_AT, '%Y-%m-%d %H:%i'), 15), '0') AS SLOT_,
       COUNT(*) AS PAGES,
       SUM(NOT EXISTS (SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
                       WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.LANG = p.LANG
                         AND s.CONTENT_MD IS NOT NULL AND s.CONTENT_MD <> '')) AS NO_CONTENT_MD
FROM T_WC_WIKIPEDIA_PAGE_LANG p
WHERE p.LANG = 'en' AND p.PAGE_EXISTS = 1
  AND p.LAST_CRAWLED_AT >= '2026-10-20 08:00:00' AND p.LAST_CRAWLED_AT < '2026-10-20 12:00:00'
GROUP BY SLOT_
ORDER BY SLOT_;

-- C. The first and last page stamped in the hour before the crash that lack CONTENT_MD,
--    to bound the window
SELECT p.ID_WIKIDATA, p.WIKIPEDIA_PAGE_TITLE, p.LAST_CRAWLED_AT
FROM T_WC_WIKIPEDIA_PAGE_LANG p
WHERE p.LANG = 'en' AND p.PAGE_EXISTS = 1
  AND p.LAST_CRAWLED_AT >= '2026-10-20 08:00:00' AND p.LAST_CRAWLED_AT < '2026-10-20 12:00:00'
  AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION s
                  WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.LANG = p.LANG
                    AND s.CONTENT_MD IS NOT NULL AND s.CONTENT_MD <> '')
ORDER BY p.LAST_CRAWLED_AT
LIMIT 5;
