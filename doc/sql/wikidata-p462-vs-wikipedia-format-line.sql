-- wikidata-p462-vs-wikipedia-format-line.sql
--
-- Audit: where does Wikidata's colour property (P462) disagree with the French Wikipedia
-- Format line, as parsed into T_WC_TMDB_MOVIE by tmdb-movie-preprocess?
--
-- Written 2026-09-12 for the Substack piece #7 (Nestor: editions/video-7-color-article.md, §4).
-- Motivation: Philippe remembers, from the V1 crawl of 2024, films marked "color" on Wikidata
-- because a colourised version exists. On Humphrey Bogart's filmography that is no longer
-- observed (83 films with P462 on 2026-09-12: 72 black and white, 10 colour, 1 both). These
-- queries look for the cases on the whole base.
--
-- Values: Q22006653 = color, Q838368 = black-and-white.
-- Tables: T_WC_TMDB_MOVIE (ID_WIKIDATA, WIKIPEDIA_FORMAT_LINE, IS_COLOR, IS_BLACK_AND_WHITE),
--         T_WC_WIKIDATA_STATEMENT (V2, ID_WIKIDATA, ID_PROPERTY, ID_STATEMENT, DELETED),
--         T_WC_WIKIDATA_ITEM_VALUE (V2, ID_STATEMENT, ID_ITEM).
--
-- How to run on the VPS, from the tools stack (read-only, no write):
--   mariadb --default-character-set=utf8mb4 -u <user> -p vaugouindb \
--     < wikidata-p462-vs-wikipedia-format-line.sql \
--     > wikidata-p462-vs-wikipedia-format-line-$(date +%Y%m%d).txt
-- Then hand the .txt back; it is archived in Nestor under editions/video-7-chatbot-results/.
--
-- Each block starts with a one-row label so the .txt reads on its own.

SET NAMES utf8mb4;

-- ---------------------------------------------------------------------------------------------
SELECT '(a) Wikidata says colour, the Format line says black and white and not colour' AS query;

SELECT m.ID_MOVIE, m.TITLE, m.RELEASE_YEAR, m.ID_WIKIDATA, m.WIKIPEDIA_FORMAT_LINE,
       m.IS_COLOR, m.IS_BLACK_AND_WHITE
FROM T_WC_TMDB_MOVIE m
JOIN T_WC_WIKIDATA_STATEMENT s
  ON s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P462' AND COALESCE(s.DELETED, 0) = 0
JOIN T_WC_WIKIDATA_ITEM_VALUE v
  ON v.ID_STATEMENT = s.ID_STATEMENT AND v.ID_ITEM = 'Q22006653'
WHERE m.WIKIPEDIA_FORMAT_LINE IS NOT NULL AND m.WIKIPEDIA_FORMAT_LINE <> ''
  AND LOWER(m.WIKIPEDIA_FORMAT_LINE) LIKE '%noir et blanc%'
  AND LOWER(m.WIKIPEDIA_FORMAT_LINE) NOT LIKE '%couleur%'
ORDER BY m.RELEASE_YEAR, m.ID_MOVIE;

-- ---------------------------------------------------------------------------------------------
SELECT '(b) Wikidata says colour, the Format line mentions a colourisation' AS query;

SELECT m.ID_MOVIE, m.TITLE, m.RELEASE_YEAR, m.ID_WIKIDATA, m.WIKIPEDIA_FORMAT_LINE,
       m.IS_COLOR, m.IS_BLACK_AND_WHITE
FROM T_WC_TMDB_MOVIE m
JOIN T_WC_WIKIDATA_STATEMENT s
  ON s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P462' AND COALESCE(s.DELETED, 0) = 0
JOIN T_WC_WIKIDATA_ITEM_VALUE v
  ON v.ID_STATEMENT = s.ID_STATEMENT AND v.ID_ITEM = 'Q22006653'
WHERE LOWER(m.WIKIPEDIA_FORMAT_LINE) REGEXP 'coloris'
ORDER BY m.RELEASE_YEAR, m.ID_MOVIE;

-- ---------------------------------------------------------------------------------------------
SELECT '(c) Wikidata says colour, the parser flagged black and white only' AS query;

SELECT m.ID_MOVIE, m.TITLE, m.RELEASE_YEAR, m.ID_WIKIDATA, m.WIKIPEDIA_FORMAT_LINE
FROM T_WC_TMDB_MOVIE m
JOIN T_WC_WIKIDATA_STATEMENT s
  ON s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P462' AND COALESCE(s.DELETED, 0) = 0
JOIN T_WC_WIKIDATA_ITEM_VALUE v
  ON v.ID_STATEMENT = s.ID_STATEMENT AND v.ID_ITEM = 'Q22006653'
WHERE m.IS_BLACK_AND_WHITE = 1 AND COALESCE(m.IS_COLOR, 0) = 0
ORDER BY m.RELEASE_YEAR, m.ID_MOVIE;

-- ---------------------------------------------------------------------------------------------
SELECT '(d) the other way round: Wikidata knows both values, the flags know one or none' AS query;

SELECT m.ID_MOVIE, m.TITLE, m.RELEASE_YEAR, m.ID_WIKIDATA, m.WIKIPEDIA_FORMAT_LINE,
       m.IS_COLOR, m.IS_BLACK_AND_WHITE
FROM T_WC_TMDB_MOVIE m
JOIN T_WC_WIKIDATA_STATEMENT s1
  ON s1.ID_WIKIDATA = m.ID_WIKIDATA AND s1.ID_PROPERTY = 'P462' AND COALESCE(s1.DELETED, 0) = 0
JOIN T_WC_WIKIDATA_ITEM_VALUE v1
  ON v1.ID_STATEMENT = s1.ID_STATEMENT AND v1.ID_ITEM = 'Q22006653'
JOIN T_WC_WIKIDATA_STATEMENT s2
  ON s2.ID_WIKIDATA = m.ID_WIKIDATA AND s2.ID_PROPERTY = 'P462' AND COALESCE(s2.DELETED, 0) = 0
JOIN T_WC_WIKIDATA_ITEM_VALUE v2
  ON v2.ID_STATEMENT = s2.ID_STATEMENT AND v2.ID_ITEM = 'Q838368'
WHERE NOT (COALESCE(m.IS_COLOR, 0) = 1 AND COALESCE(m.IS_BLACK_AND_WHITE, 0) = 1)
ORDER BY m.RELEASE_YEAR, m.ID_MOVIE;

-- ---------------------------------------------------------------------------------------------
SELECT '(e) counts, for scale' AS query;

SELECT
  (SELECT COUNT(*) FROM T_WC_TMDB_MOVIE
    WHERE WIKIPEDIA_FORMAT_LINE IS NOT NULL AND WIKIPEDIA_FORMAT_LINE <> '') AS movies_with_format_line,
  (SELECT COUNT(DISTINCT s.ID_WIKIDATA) FROM T_WC_WIKIDATA_STATEMENT s
    WHERE s.ID_PROPERTY = 'P462' AND COALESCE(s.DELETED, 0) = 0) AS wikidata_items_with_p462,
  (SELECT COUNT(DISTINCT m.ID_MOVIE) FROM T_WC_TMDB_MOVIE m
    JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P462'
      AND COALESCE(s.DELETED, 0) = 0) AS movies_with_p462,
  (SELECT COUNT(*) FROM T_WC_TMDB_MOVIE WHERE IS_COLOR = 1 AND IS_BLACK_AND_WHITE = 1) AS movies_both_flags;
