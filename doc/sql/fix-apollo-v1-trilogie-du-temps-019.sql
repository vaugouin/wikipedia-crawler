-- WIKIPEDIA-CRAWLER-019: the last wrong Apollo_11_Crew.jpg, on Q468018 "Trilogie du temps".
-- Its French page has no lead image (MAIN_IMAGE_URL = '' since the 2026-10-14 sweep) and it has
-- no English page, so f_getwikidataimagepath (tmdb-movie-preprocess) falls back on the V1 column,
-- never blanked, which still holds the "Portail:Années 1960" photo (two rows).
-- Q190706 (Moon landing conspiracy theories) keeps its Apollo image: it is its real lead image.
-- Writes T_WC_WIKIDATA_ITEM_V1 (outside the agent's five tables): run by Philippe.

UPDATE T_WC_WIKIDATA_ITEM_V1
SET WIKIPEDIA_IMAGE_PATH = ''
WHERE ID_WIKIDATA = 'Q468018' AND WIKIPEDIA_IMAGE_PATH LIKE '%/Apollo_11_Crew.jpg';
SELECT ROW_COUNT() AS V1_CLEARED;

-- The T2S copies, so the front is right today rather than after the next run.
UPDATE T_WC_T2S_COLLECTION
SET WIKIPEDIA_IMAGE_PATH = ''
WHERE ID_WIKIDATA = 'Q468018' AND WIKIPEDIA_IMAGE_PATH LIKE '%/Apollo_11_Crew.jpg';
SELECT ROW_COUNT() AS COLLECTION_CLEARED;

UPDATE T_WC_T2S_ITEM
SET WIKIPEDIA_IMAGE_PATH = ''
WHERE ID_WIKIDATA = 'Q468018' AND WIKIPEDIA_IMAGE_PATH LIKE '%/Apollo_11_Crew.jpg';
SELECT ROW_COUNT() AS ITEM_CLEARED;

SELECT
  (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1 WHERE WIKIPEDIA_IMAGE_PATH LIKE '%/Apollo_11_Crew.jpg') AS V1_LEFT,
  (SELECT COUNT(*) FROM T_WC_T2S_COLLECTION WHERE WIKIPEDIA_IMAGE_PATH LIKE '%/Apollo_11_Crew.jpg'
     OR WIKIPEDIA_MAIN_IMAGE_URL LIKE '%/Apollo_11_Crew.jpg' OR WIKIPEDIA_MAIN_IMAGE_URL_FR LIKE '%/Apollo_11_Crew.jpg') AS COLLECTION_LEFT,
  (SELECT COUNT(*) FROM T_WC_T2S_ITEM WHERE WIKIPEDIA_IMAGE_PATH LIKE '%/Apollo_11_Crew.jpg'
     OR WIKIPEDIA_MAIN_IMAGE_URL LIKE '%/Apollo_11_Crew.jpg' OR WIKIPEDIA_MAIN_IMAGE_URL_FR LIKE '%/Apollo_11_Crew.jpg') AS ITEM_LEFT;
