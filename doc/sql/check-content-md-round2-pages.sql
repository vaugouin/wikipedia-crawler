-- CONTENT_MD prototype, round 2: which entity types the crawled pages carry, and are the candidate
-- pages (series, award, location, keyword, company, collection) crawled. Read-only.
SELECT ITEM_TYPE, LANG, COUNT(*) AS PAGES FROM T_WC_WIKIPEDIA_PAGE_LANG GROUP BY ITEM_TYPE, LANG ORDER BY ITEM_TYPE, LANG;

SELECT p.ID_WIKIDATA, p.LANG, p.ITEM_TYPE, p.WIKIPEDIA_PAGE_TITLE, p.LAST_SUCCESS_AT,
  (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION s WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.LANG = p.LANG) AS SECTIONS
FROM T_WC_WIKIPEDIA_PAGE_LANG p
WHERE (p.LANG = 'en' AND p.WIKIPEDIA_PAGE_TITLE IN ('Twin Peaks', 'The Wire', 'Academy Award for Best Picture',
         'Palme d''Or', 'Monument Valley', 'Cinecittà', 'Film noir', 'Heist film', 'Hammer Film Productions',
         'Studio Ghibli', 'Three Colours trilogy', 'Before trilogy', 'James Bond in film'))
   OR (p.LANG = 'fr' AND p.WIKIPEDIA_PAGE_TITLE IN ('Breaking Bad', 'Les Soprano', 'César du meilleur film',
         'Lion d''or', 'Monument Valley', 'Cinecittà', 'Western spaghetti', 'Film noir', 'Gaumont (entreprise)',
         'Pathé', 'Trilogie Trois Couleurs', 'Trois Couleurs', 'Trilogie Before'))
ORDER BY p.ITEM_TYPE, p.LANG, p.WIKIPEDIA_PAGE_TITLE;
