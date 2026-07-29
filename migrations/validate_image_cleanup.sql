-- =====================================================================================
-- Validation des migrations d'images du 2026-07-29 (WIKIPEDIA-CRAWLER-019 et -020)
--
-- A executer d'un bloc. Renvoie UN SEUL tableau de resultats, une ligne par controle,
-- avec la valeur attendue, la valeur obtenue et un verdict lisible.
--
-- Les trois migrations couvertes :
--   1. clear_ui_chrome_images.py --apply          (icones d'interface MediaWiki)
--   2. clear_shared_main_images.py --min 50 --apply (images de portail, detectees par frequence)
--   3. add_main_image_url_to_page_lang.py --apply --backfill (nouvelle colonne + remplissage)
--
-- Deux controles ne sont pas des reussites de migration :
--   15 est un controle INVERSE : les visuels de series sous le seuil de 50 (Gumball,
--      SpongeBob, Alice Comedies) doivent etre PRESERVES. Un 0 y serait une mauvaise
--      nouvelle : il voudrait dire que le seuil a detruit des images legitimes.
--   30 et 31 disent ce qui RESTE a faire en aval : le preprocess n'a pas encore
--      propage le nettoyage vers les copies T2S servies au front et au voice-agent.
--
-- Duree : compter quelques minutes. Le controle 02 balaie T_WC_WIKIPEDIA_PAGE_LANG_IMAGE
-- (8,5 M de lignes) avec une expression reguliere, sans index possible. C'est le seul
-- long ; si le temps pose probleme, commenter son bloc.
-- =====================================================================================

-- La connexion doit parler la meme collation que les tables, sinon REGEXP echoue avec
-- "Illegal mix of collations": une variable utilisateur herite de collation_connection
-- (utf8mb4_general_ci par defaut) alors que les colonnes sont en utf8mb4_unicode_ci.
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SET @CHROME := _utf8mb4'Blue_pencil|OOjs_UI_icon|Question_book|Wiki_letter_w|Edit-clear|Edit-copy|Text_document_with|Crystal_Clear|Crystal_128|Crystal_Project|Crystal_energy|Crystal_personal|Crystal_kcontrol|Nuvola|Gnome-(mime|dev|fs|applications|document|searchtool|globe|html)|Ambox|Cscr-|Emblem-(money|risk|relax|question|web|WikiVote)|Symbol_[A-Za-z_]+_(class|vote)|2017-fr[.]wp-|Portal-puzzle|Portal_[A-Za-z_]+_[Ii]con[.]svg|Magnify-clip|Broom_icon|Searchtool[.]|Speaker_Icon[.]|Disambig|Increase2|Decrease2|Steady2?[.]svg|Yes_check[.]|X_mark[.]|Translation_([a-z_]*)arrow|Information_icon|Stub_icon|Oxygen[0-9]|(Commons|Wikidata|Wikiquote|Wikisource|Wiktionary|Wikinews|Wikipedia)-logo' COLLATE utf8mb4_unicode_ci;

SELECT * FROM (

-- =====================================================================================
-- MIGRATION 1 : clear_ui_chrome_images.py  (icones d'interface)
-- =====================================================================================

SELECT '01' AS ETAPE,
       'Migration 1 : icones d''interface restantes dans les 6 colonnes image principale' AS CONTROLE,
       '0' AS ATTENDU,
       CAST(( (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1      WHERE WIKIPEDIA_IMAGE_PATH   REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1     WHERE WIKIPEDIA_POSTER_PATH  REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE_V1     WHERE WIKIPEDIA_POSTER_PATH  REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1    WHERE WIKIPEDIA_PROFILE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_WIKIDATA_CHARACTER_V1 WHERE WIKIPEDIA_PROFILE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_T2S_TECHNICAL         WHERE WIKIPEDIA_IMAGE_PATH   REGEXP @CHROME)
            ) AS CHAR) AS OBTENU,
       CASE WHEN ( (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1      WHERE WIKIPEDIA_IMAGE_PATH   REGEXP @CHROME)
                 + (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1     WHERE WIKIPEDIA_POSTER_PATH  REGEXP @CHROME)
                 + (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE_V1     WHERE WIKIPEDIA_POSTER_PATH  REGEXP @CHROME)
                 + (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1    WHERE WIKIPEDIA_PROFILE_PATH REGEXP @CHROME)
                 + (SELECT COUNT(*) FROM T_WC_WIKIDATA_CHARACTER_V1 WHERE WIKIPEDIA_PROFILE_PATH REGEXP @CHROME)
                 + (SELECT COUNT(*) FROM T_WC_T2S_TECHNICAL         WHERE WIKIPEDIA_IMAGE_PATH   REGEXP @CHROME)
                 ) = 0 THEN 'OK' ELSE '>>> MIGRATION 1 A RELANCER' END AS VERDICT

UNION ALL SELECT '02',
       'Migration 1 : icones d''interface restantes dans la galerie (balayage long)',
       '0',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE WHERE IMAGE_URL REGEXP @CHROME) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE WHERE IMAGE_URL REGEXP @CHROME) = 0
            THEN 'OK' ELSE '>>> MIGRATION 1 A RELANCER' END

-- =====================================================================================
-- MIGRATION 2 : clear_shared_main_images.py --min 50  (images de portail, par frequence)
-- =====================================================================================

UNION ALL SELECT '10',
       'Migration 2 : images encore partagees par 50+ entites (ITEM)',
       '0',
       CAST((SELECT COUNT(*) FROM (
              SELECT 1 FROM T_WC_WIKIDATA_ITEM_V1
              WHERE WIKIPEDIA_IMAGE_PATH IS NOT NULL AND WIKIPEDIA_IMAGE_PATH <> ''
              GROUP BY WIKIPEDIA_IMAGE_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50) x) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM (
              SELECT 1 FROM T_WC_WIKIDATA_ITEM_V1
              WHERE WIKIPEDIA_IMAGE_PATH IS NOT NULL AND WIKIPEDIA_IMAGE_PATH <> ''
              GROUP BY WIKIPEDIA_IMAGE_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50) x) = 0
            THEN 'OK' ELSE '>>> MIGRATION 2 A RELANCER' END

UNION ALL SELECT '11',
       'Migration 2 : images encore partagees par 50+ entites (MOVIE)',
       '0',
       CAST((SELECT COUNT(*) FROM (
              SELECT 1 FROM T_WC_WIKIDATA_MOVIE_V1
              WHERE WIKIPEDIA_POSTER_PATH IS NOT NULL AND WIKIPEDIA_POSTER_PATH <> ''
              GROUP BY WIKIPEDIA_POSTER_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50) x) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM (
              SELECT 1 FROM T_WC_WIKIDATA_MOVIE_V1
              WHERE WIKIPEDIA_POSTER_PATH IS NOT NULL AND WIKIPEDIA_POSTER_PATH <> ''
              GROUP BY WIKIPEDIA_POSTER_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50) x) = 0
            THEN 'OK' ELSE '>>> MIGRATION 2 A RELANCER' END

UNION ALL SELECT '12',
       'Migration 2 : images encore partagees par 50+ entites (SERIE)',
       '0',
       CAST((SELECT COUNT(*) FROM (
              SELECT 1 FROM T_WC_WIKIDATA_SERIE_V1
              WHERE WIKIPEDIA_POSTER_PATH IS NOT NULL AND WIKIPEDIA_POSTER_PATH <> ''
              GROUP BY WIKIPEDIA_POSTER_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50) x) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM (
              SELECT 1 FROM T_WC_WIKIDATA_SERIE_V1
              WHERE WIKIPEDIA_POSTER_PATH IS NOT NULL AND WIKIPEDIA_POSTER_PATH <> ''
              GROUP BY WIKIPEDIA_POSTER_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50) x) = 0
            THEN 'OK' ELSE '>>> MIGRATION 2 A RELANCER' END

UNION ALL SELECT '13',
       'Migration 2 : images encore partagees par 50+ entites (PERSON)',
       '0',
       CAST((SELECT COUNT(*) FROM (
              SELECT 1 FROM T_WC_WIKIDATA_PERSON_V1
              WHERE WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> ''
              GROUP BY WIKIPEDIA_PROFILE_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50) x) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM (
              SELECT 1 FROM T_WC_WIKIDATA_PERSON_V1
              WHERE WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> ''
              GROUP BY WIKIPEDIA_PROFILE_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50) x) = 0
            THEN 'OK' ELSE '>>> MIGRATION 2 A RELANCER' END

UNION ALL SELECT '14',
       'Migration 2 : temoin Apollo 11 dans les colonnes image principale',
       '0',
       CAST(( (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1   WHERE WIKIPEDIA_IMAGE_PATH   LIKE '%Apollo\_11\_Crew.jpg')
            + (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1  WHERE WIKIPEDIA_POSTER_PATH  LIKE '%Apollo\_11\_Crew.jpg')
            + (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE_V1  WHERE WIKIPEDIA_POSTER_PATH  LIKE '%Apollo\_11\_Crew.jpg')
            + (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1 WHERE WIKIPEDIA_PROFILE_PATH LIKE '%Apollo\_11\_Crew.jpg')
            ) AS CHAR),
       CASE WHEN ( (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1   WHERE WIKIPEDIA_IMAGE_PATH   LIKE '%Apollo\_11\_Crew.jpg')
                 + (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1  WHERE WIKIPEDIA_POSTER_PATH  LIKE '%Apollo\_11\_Crew.jpg')
                 + (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE_V1  WHERE WIKIPEDIA_POSTER_PATH  LIKE '%Apollo\_11\_Crew.jpg')
                 + (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1 WHERE WIKIPEDIA_PROFILE_PATH LIKE '%Apollo\_11\_Crew.jpg')
                 ) = 0 THEN 'OK' ELSE '>>> MIGRATION 2 A RELANCER' END

UNION ALL SELECT '15',
       'Migration 2 : temoin inverse, les visuels de series sous le seuil sont PRESERVES',
       '> 0 (ne doit PAS etre 0)',
       CAST(( (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1  WHERE WIKIPEDIA_IMAGE_PATH  REGEXP 'Gumball|SpongeBob|LogoLesGriffin|South_Park|Law_.26_Order|Midsomer')
            + (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1 WHERE WIKIPEDIA_POSTER_PATH REGEXP 'Alice_Comedies')
            ) AS CHAR),
       CASE WHEN ( (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1  WHERE WIKIPEDIA_IMAGE_PATH  REGEXP 'Gumball|SpongeBob|LogoLesGriffin|South_Park|Law_.26_Order|Midsomer')
                 + (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1 WHERE WIKIPEDIA_POSTER_PATH REGEXP 'Alice_Comedies')
                 ) > 0 THEN 'OK (seuil 50 respecte)' ELSE '>>> ALERTE : seuil trop bas, visuels legitimes effaces' END

-- =====================================================================================
-- MIGRATION 3 : add_main_image_url_to_page_lang.py  (nouvelle colonne + backfill)
-- =====================================================================================

UNION ALL SELECT '20',
       'Migration 3 : la colonne MAIN_IMAGE_URL existe',
       '1',
       CAST((SELECT COUNT(*) FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG'
               AND COLUMN_NAME = 'MAIN_IMAGE_URL') AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM information_schema.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG'
                    AND COLUMN_NAME = 'MAIN_IMAGE_URL') = 1
            THEN 'OK' ELSE '>>> MIGRATION 3 NON EXECUTEE' END

UNION ALL SELECT '21',
       'Migration 3 : largeur de la colonne (le varchar(500) initial debordait)',
       '1000',
       CAST(COALESCE((SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG'
               AND COLUMN_NAME = 'MAIN_IMAGE_URL'), 0) AS CHAR),
       CASE WHEN COALESCE((SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_WIKIPEDIA_PAGE_LANG'
                    AND COLUMN_NAME = 'MAIN_IMAGE_URL'), 0) >= 1000
            THEN 'OK' ELSE '>>> RELANCER MIGRATION 3 (elargit la colonne)' END

UNION ALL SELECT '22',
       'Migration 3 : lignes restant a remplir (page ayant une image principale en galerie)',
       '0',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG p
             WHERE p.MAIN_IMAGE_URL IS NULL AND EXISTS (
               SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE g
               WHERE g.ID_WIKIDATA = p.ID_WIKIDATA AND g.LANG = p.LANG
                 AND g.IS_MAIN_IMAGE = 1 AND g.DELETED = 0)) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG p
                  WHERE p.MAIN_IMAGE_URL IS NULL AND EXISTS (
                    SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE g
                    WHERE g.ID_WIKIDATA = p.ID_WIKIDATA AND g.LANG = p.LANG
                      AND g.IS_MAIN_IMAGE = 1 AND g.DELETED = 0)) = 0
            THEN 'OK' ELSE '>>> BACKFILL A RELANCER (--apply --backfill)' END

UNION ALL SELECT '23',
       'Migration 3 : lignes effectivement remplies (informatif, ~790 000 attendues)',
       'informatif',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG WHERE MAIN_IMAGE_URL IS NOT NULL) AS CHAR),
       'informatif'

UNION ALL SELECT '24',
       'Migration 3 : aucun parametre de suivi ?utm_ n''a ete recopie',
       '0',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG WHERE MAIN_IMAGE_URL LIKE '%?utm\_%') AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG WHERE MAIN_IMAGE_URL LIKE '%?utm\_%') = 0
            THEN 'OK' ELSE '>>> ancienne version du backfill, relancer' END

UNION ALL SELECT '25',
       'Migration 3 : aucune icone d''interface n''a ete recopiee dans la nouvelle colonne',
       '0',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG WHERE MAIN_IMAGE_URL REGEXP @CHROME) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG WHERE MAIN_IMAGE_URL REGEXP @CHROME) = 0
            THEN 'OK' ELSE '>>> backfill fait AVANT le nettoyage : relancer migration 1 puis le backfill' END

-- =====================================================================================
-- CE QUI RESTE A FAIRE (pas des migrations : etat du reste de la chaine)
-- =====================================================================================

UNION ALL SELECT '30',
       'RESTE A FAIRE : copies T2S encore polluees (le preprocess n''a pas tourne)',
       '0 une fois tmdb-movie-preprocess passe',
       CAST(( (SELECT COUNT(*) FROM T_WC_T2S_COLLECTION WHERE WIKIPEDIA_IMAGE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_T2S_LIST       WHERE WIKIPEDIA_IMAGE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_T2S_TOPIC      WHERE WIKIPEDIA_IMAGE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_T2S_MOVEMENT   WHERE WIKIPEDIA_IMAGE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_T2S_AWARD      WHERE WIKIPEDIA_IMAGE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_T2S_NOMINATION WHERE WIKIPEDIA_IMAGE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_T2S_GROUP      WHERE WIKIPEDIA_IMAGE_PATH REGEXP @CHROME)
            + (SELECT COUNT(*) FROM T_WC_T2S_DEATH      WHERE WIKIPEDIA_IMAGE_PATH REGEXP @CHROME)
            ) AS CHAR),
       'TMDB-MOVIE-PREPROCESS-035 partie A'

UNION ALL SELECT '31',
       'RESTE A FAIRE : les 2 trilogies Leone (4845 doit se reparer au prochain crawl, 4840 restera vide)',
       'voir colonne OBTENU',
       CAST((SELECT GROUP_CONCAT(CONCAT(ID_T2S_COLLECTION, '=',
                     COALESCE(NULLIF(SUBSTRING_INDEX(WIKIPEDIA_IMAGE_PATH, '/', -1), ''), '(vide)'))
                   ORDER BY ID_T2S_COLLECTION SEPARATOR ' | ')
             FROM T_WC_T2S_COLLECTION WHERE ID_T2S_COLLECTION IN (4840, 4845)) AS CHAR),
       'informatif'

) AS RAPPORT ORDER BY ETAPE;
