-- =====================================================================================
-- Le code qui tourne est-il bien le bon ? Verification par les ECRITURES RECENTES.
--
-- Un `docker build` ne redemarre rien, et le code vient du volume monte : seul un
-- redemarrage du conteneur le charge. Ce fichier ne regarde pas le conteneur, il
-- regarde ce qu'il ECRIT, ce qui est la seule preuve qui compte.
--
-- Trois signatures distinguent l'ancien code du nouveau :
--   B  WIKIPEDIA-CRAWLER-020 : le crawler renseigne MAIN_IMAGE_URL. Avant, jamais.
--   C  WIKIPEDIA-CRAWLER-021 : son filtre attrape les vignettes et Logo_disambig.
--   D  WIKIPEDIA-CRAWLER-019 correctif B : plus de repli, donc plus jamais une image
--      de portail ecrite faute de mieux.
--
-- Fenetre temporelle : les intervalles partent du DERNIER enregistrement ecrit, pas de
-- NOW(). Le crawler horodate en heure de Paris (citizenphil.paris_tz) tandis que NOW()
-- suit le fuseau du serveur ; un decalage aurait vide toutes les fenetres et fait
-- conclure a tort. Le controle 00 affiche les deux horloges pour que l'ecart se voie.
--
-- A lancer pendant que le crawler tourne, sinon les fenetres sont vides et les verdicts
-- ne veulent rien dire (le controle A le signale).
-- =====================================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SET @CHROME := _utf8mb4'Blue_pencil|OOjs_UI_icon|Question_book|Wiki_letter_w|Edit-clear|Text_document_with|Crystal_Clear|Nuvola|Ambox|Cscr-|Emblem-(money|risk|relax|question|web|WikiVote)|Symbol_[A-Za-z_]+_(class|vote)|2017-fr[.]wp-|disambig|(Commons|Wikidata|Wikiquote|Wiktionary|Wikinews|Wikipedia)-logo' COLLATE utf8mb4_unicode_ci;

-- Reference temporelle : la derniere ecriture connue, cote page et cote personne.
SET @DERNIERPAGE   := (SELECT MAX(LAST_CRAWLED_AT) FROM T_WC_WIKIPEDIA_PAGE_LANG);
SET @DERNIERPERSON := (SELECT MAX(TIM_UPDATED)     FROM T_WC_WIKIDATA_PERSON_V1);

SELECT * FROM (

SELECT '00' AS ETAPE,
       'Horloges : derniere page crawlee / derniere personne ecrite / NOW() du serveur' AS CONTROLE,
       'informatif' AS ATTENDU,
       CAST(CONCAT(COALESCE(@DERNIERPAGE, 'aucune'), '  |  ',
                   COALESCE(@DERNIERPERSON, 'aucune'), '  |  ', NOW()) AS CHAR) AS OBTENU,
       'informatif' AS VERDICT

-- =====================================================================================
-- A. Le crawler travaille-t-il en ce moment ? Sans cela, rien d'autre n'est concluant.
-- =====================================================================================

UNION ALL SELECT 'A1',
       'Pages crawlees dans les 15 min precedant la derniere ecriture',
       '> 0, sinon le crawler est a l''arret et les verdicts ci-dessous sont vides',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG
             WHERE LAST_CRAWLED_AT >= @DERNIERPAGE - INTERVAL 15 MINUTE) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG
                  WHERE LAST_CRAWLED_AT >= @DERNIERPAGE - INTERVAL 15 MINUTE) > 0
            THEN 'OK, le crawler ecrit' ELSE '>>> a l''arret : relancer avant de conclure' END

-- =====================================================================================
-- B. Signature de WIKIPEDIA-CRAWLER-020 : le crawler renseigne MAIN_IMAGE_URL.
--    Le backfill l'a rempli pour l'existant, donc on ne regarde QUE les pages crawlees
--    apres lui. Si le nouveau code tourne, une bonne part d'entre elles doit l'avoir.
-- =====================================================================================

UNION ALL SELECT 'B1',
       'Parmi les pages crawlees dans la derniere heure, part ayant MAIN_IMAGE_URL',
       'nettement > 0 % si le code -020 tourne ; 0 % = ancien code',
       CAST(CONCAT(
         (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG
          WHERE LAST_CRAWLED_AT >= @DERNIERPAGE - INTERVAL 60 MINUTE AND MAIN_IMAGE_URL IS NOT NULL),
         ' / ',
         (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG
          WHERE LAST_CRAWLED_AT >= @DERNIERPAGE - INTERVAL 60 MINUTE)) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIPEDIA_PAGE_LANG
                  WHERE LAST_CRAWLED_AT >= @DERNIERPAGE - INTERVAL 60 MINUTE
                    AND MAIN_IMAGE_URL IS NOT NULL) > 0
            THEN 'OK, le code -020 ecrit la colonne'
            ELSE '>>> aucune : le conteneur n''a pas ete redemarre' END

-- =====================================================================================
-- C. Signature du filtre -021 : plus aucun chrome frais.
--    Le crawler ne touche TIM_UPDATED d'une entite que lorsqu'il y ECRIT une image :
--    ces lignes sont donc exactement les ecritures recentes, pas un simple passage.
-- =====================================================================================

UNION ALL SELECT 'C1',
       'Personnes dont l''image a ete ecrite dans la derniere heure ET qui portent du chrome',
       '0',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1
             WHERE TIM_UPDATED >= @DERNIERPERSON - INTERVAL 60 MINUTE
               AND WIKIPEDIA_PROFILE_PATH REGEXP @CHROME) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1
                  WHERE TIM_UPDATED >= @DERNIERPERSON - INTERVAL 60 MINUTE
                    AND WIKIPEDIA_PROFILE_PATH REGEXP @CHROME) = 0
            THEN 'OK' ELSE '>>> ANCIEN FILTRE : le conteneur tourne avec du code perime' END

UNION ALL SELECT 'C2',
       'Idem, exemples de ce qui vient d''etre ecrit (les 5 dernieres personnes)',
       'informatif',
       CAST(COALESCE((SELECT GROUP_CONCAT(F ORDER BY F SEPARATOR ' | ') FROM (
              SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(WIKIPEDIA_PROFILE_PATH, '?', 1), '/', -1) AS F
              FROM T_WC_WIKIDATA_PERSON_V1
              WHERE WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> ''
              ORDER BY TIM_UPDATED DESC LIMIT 5) d), 'aucune') AS CHAR),
       'informatif'

-- =====================================================================================
-- D. Signature du correctif B : le repli est mort, donc aucune image de portail fraiche.
--    Apollo 11 est le temoin : sans repli, il ne peut plus etre ecrit par le crawler.
-- =====================================================================================

UNION ALL SELECT 'D1',
       'Personnes ecrites dans la derniere heure portant Apollo 11 (temoin du repli)',
       '0',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1
             WHERE TIM_UPDATED >= @DERNIERPERSON - INTERVAL 60 MINUTE
               AND WIKIPEDIA_PROFILE_PATH LIKE '%Apollo\_11\_Crew%') AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1
                  WHERE TIM_UPDATED >= @DERNIERPERSON - INTERVAL 60 MINUTE
                    AND WIKIPEDIA_PROFILE_PATH LIKE '%Apollo\_11\_Crew%') = 0
            THEN 'OK, le repli ne tourne plus'
            ELSE '>>> LE REPLI TOURNE ENCORE : correctif B non charge' END

UNION ALL SELECT 'D2',
       'Images ecrites dans la derniere heure deja portees par 50+ autres personnes',
       '0 (une image de tete est propre a son sujet)',
       CAST((SELECT COUNT(*) FROM (
              SELECT WIKIPEDIA_PROFILE_PATH FROM T_WC_WIKIDATA_PERSON_V1
              WHERE TIM_UPDATED >= @DERNIERPERSON - INTERVAL 60 MINUTE
                AND WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> ''
                AND WIKIPEDIA_PROFILE_PATH IN (
                  SELECT WIKIPEDIA_PROFILE_PATH FROM T_WC_WIKIDATA_PERSON_V1
                  WHERE WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> ''
                  GROUP BY WIKIPEDIA_PROFILE_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50)
            ) x) AS CHAR),
       CASE WHEN (SELECT COUNT(*) FROM (
              SELECT WIKIPEDIA_PROFILE_PATH FROM T_WC_WIKIDATA_PERSON_V1
              WHERE TIM_UPDATED >= @DERNIERPERSON - INTERVAL 60 MINUTE
                AND WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> ''
                AND WIKIPEDIA_PROFILE_PATH IN (
                  SELECT WIKIPEDIA_PROFILE_PATH FROM T_WC_WIKIDATA_PERSON_V1
                  WHERE WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> ''
                  GROUP BY WIKIPEDIA_PROFILE_PATH HAVING COUNT(DISTINCT ID_WIKIDATA) >= 50)
            ) x) = 0
            THEN 'OK' ELSE '>>> une decoration vient d''etre ecrite : ancien code' END

-- =====================================================================================
-- E. Combien d'enregistrements douteux l'ancien code a-t-il produits hier ?
--    Informatif : donne le volume a reprendre une fois le bon code en place.
-- =====================================================================================

UNION ALL SELECT 'E1',
       'Personnes portant du chrome, ecrites AVANT la derniere heure (heritage a reprendre)',
       'informatif',
       CAST((SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1
             WHERE (TIM_UPDATED < @DERNIERPERSON - INTERVAL 60 MINUTE OR TIM_UPDATED IS NULL)
               AND WIKIPEDIA_PROFILE_PATH REGEXP @CHROME) AS CHAR),
       'informatif'

) AS RAPPORT ORDER BY ETAPE;
