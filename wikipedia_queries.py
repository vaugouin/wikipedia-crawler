"""Per-family row-selection SQL for the Wikipedia crawler.

Every entity family (movie, person, item, ... technical, season, sourcework) selects its rows
with a ``build_<family>_sql(strresumeid)`` builder: the family's source table, the
chain of exclusions that stops an entity already owned by an earlier family from
being crawled twice, and an optional ``>= resume id`` lower bound.

These builders used to live in [wikipedia_crawler.py](wikipedia_crawler.py). They
live here so the on-demand entry point ([wikipedia_functions.py](wikipedia_functions.py))
can run a whole family with the SAME query -- exclusion chain included -- WITHOUT
importing the crawler module, whose body launches a full crawl at import time.

The builders are pure string functions: no database, no network, no import-time work.
"""


def append_exclusion_tables(strsql, arrtables):
    for strtable in arrtables:
        strsql += "AND ID_WIKIDATA NOT IN (SELECT ID_WIKIDATA FROM " + strtable + ") "
    return strsql

def append_exclusion_queries(strsql, arrqueries):
    for strquery in arrqueries:
        strsql += "AND ID_WIKIDATA NOT IN (" + strquery + ") "
    return strsql

def normalize_resumeid(strresumeid):
    if strresumeid is None:
        return ""
    return str(strresumeid)

def build_movie_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_MOVIE.ID_MOVIE AS id, ID_WIKIDATA FROM T_WC_TMDB_MOVIE "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    if strresumeid != "":
        strsql += "AND ID_MOVIE >= " + strresumeid + " "
    strsql += "ORDER BY ID_MOVIE ASC "
    return strsql

def build_person_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_PERSON.ID_PERSON AS id, ID_WIKIDATA FROM T_WC_TMDB_PERSON "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    if strresumeid != "":
        strsql += "AND ID_PERSON >= " + strresumeid + " "
    strsql += "ORDER BY ID_PERSON ASC "
    return strsql

def build_item_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_WIKIDATA_ITEM_V1.ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_WIKIDATA_ITEM_V1 "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_character_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_CHARACTER.ID_CHARACTER AS id, ID_WIKIDATA FROM T_WC_TMDB_CHARACTER "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_NOMINATION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TOPIC WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TECHNICAL WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_CHARACTER >= " + strresumeid + " "
    strsql += "ORDER BY ID_CHARACTER ASC "
    return strsql

def build_tmdb_collection_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_COLLECTION.ID_COLLECTION AS id, ID_WIKIDATA FROM T_WC_TMDB_COLLECTION "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_NOMINATION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TOPIC WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TECHNICAL WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_CHARACTER WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_COLLECTION >= " + strresumeid + " "
    strsql += "ORDER BY ID_COLLECTION ASC "
    return strsql

def build_episode_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_EPISODE.ID_EPISODE AS id, ID_WIKIDATA FROM T_WC_TMDB_EPISODE "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_NOMINATION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TOPIC WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TECHNICAL WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_CHARACTER WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_EPISODE >= " + strresumeid + " "
    strsql += "ORDER BY ID_EPISODE ASC "
    return strsql

def build_keyword_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_KEYWORD.ID_KEYWORD AS id, ID_WIKIDATA FROM T_WC_TMDB_KEYWORD "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_NOMINATION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TOPIC WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TECHNICAL WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_CHARACTER WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_EPISODE WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_KEYWORD >= " + strresumeid + " "
    strsql += "ORDER BY ID_KEYWORD ASC "
    return strsql

def build_season_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_SEASON.ID_SEASON AS id, ID_WIKIDATA FROM T_WC_TMDB_SEASON "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_NOMINATION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TOPIC WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TECHNICAL WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_CHARACTER WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_EPISODE WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_KEYWORD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_SEASON >= " + strresumeid + " "
    strsql += "ORDER BY ID_SEASON ASC "
    return strsql

def build_serie_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_SERIE.ID_SERIE AS id, ID_WIKIDATA FROM T_WC_TMDB_SERIE "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
    ])
    if strresumeid != "":
        strsql += "AND ID_SERIE >= " + strresumeid + " "
    strsql += "ORDER BY ID_SERIE ASC "
    return strsql

def build_wikidata_character_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_WIKIDATA_CHARACTER_V1.ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_WIKIDATA_CHARACTER_V1 "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_other_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT 'Q1204187' AS id, 'Q1204187' AS ID_WIKIDATA FROM DUAL "
    strsql += "WHERE 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_MOVIE_V1) "
    strsql += "AND 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1) "
    strsql += "AND 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_ITEM_V1) "
    strsql += "AND 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_SERIE_V1) "
    strsql += "AND 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_CHARACTER_V1) "
    return strsql

def build_list_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_LIST "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_movement_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_MOVEMENT "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_collection_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_COLLECTION "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_group_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_GROUP "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_death_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_DEATH "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_award_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_AWARD "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_nomination_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_NOMINATION "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_topic_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_TOPIC "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_NOMINATION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_technical_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_TECHNICAL "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_NOMINATION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TOPIC WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql


# WIKIPEDIA-CRAWLER-023: the rows of T_WC_T2S_SOURCE_WORK (the "based on" sources,
# Wikidata P144, built by process 73 of tmdb-movie-preprocess) that deserve a crawl.
#
# - DELETED: process 73 keeps the sources that left the perimeter as DELETED = 1 so
#   their ID_SOURCE_WORK is never reused; they are not served, so not crawled.
# - 'screen' sources with a T2S sheet: when the source is itself a film or a series
#   known to T2S (ID_MOVIE / ID_SERIE set), its Wikipedia page is already that film's
#   or series' page, crawled by 201 movie / 204 serie, and the source-work fiche points
#   to that sheet. A 'screen' source WITHOUT a T2S sheet (a film TMDb does not have)
#   stays in: nothing else would crawl it.
#
# Kept as a constant because the `--no-exclusions` path of wikipedia_functions.py
# builds its own query from the source table and must apply the same row filter.
SOURCEWORK_ROW_FILTER = (
    "COALESCE(DELETED, 0) = 0 "
    "AND NOT (COALESCE(SOURCE_WORK_TYPE, '') = 'screen' "
    "AND (ID_MOVIE IS NOT NULL OR ID_SERIE IS NOT NULL)) "
)

def build_sourcework_sql(strresumeid):
    strresumeid = normalize_resumeid(strresumeid)
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_T2S_SOURCE_WORK.ID_SOURCE_WORK AS id, ID_WIKIDATA FROM T_WC_T2S_SOURCE_WORK "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    strsql += "AND " + SOURCEWORK_ROW_FILTER
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
        "T_WC_WIKIDATA_CHARACTER_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_LIST WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_MOVEMENT WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_GROUP WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_DEATH WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_AWARD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_NOMINATION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TOPIC WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_T2S_TECHNICAL WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_CHARACTER WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_COLLECTION WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_EPISODE WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_KEYWORD WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
        "SELECT DISTINCT ID_WIKIDATA FROM T_WC_TMDB_SEASON WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''",
    ])
    if strresumeid != "":
        strsql += "AND ID_SOURCE_WORK >= " + strresumeid + " "
    strsql += "ORDER BY ID_SOURCE_WORK ASC "
    return strsql


# content family -> row-selection builder. The keys are the CONTENT_CONFIG keys in
# wikipedia_page_writer.py (i.e. the ITEM_TYPE written to the database), which is what
# `--content-all` takes on the command line.
#
# NOTE on the two character families: `wikidatacharacter` (process 205, sourced from
# T_WC_WIKIDATA_CHARACTER_V1) maps to build_wikidata_character_sql and `character`
# (process 218, sourced from T_WC_TMDB_CHARACTER) to build_character_sql, matching
# CONTENT_CONFIG and the README. `arrprocesses` in wikipedia_crawler.py currently wires
# BOTH 205 and 218 to build_character_sql; that discrepancy is left untouched here.
CONTENT_SQL_BUILDERS = {
    "movie":             build_movie_sql,
    "person":            build_person_sql,
    "item":              build_item_sql,
    "serie":             build_serie_sql,
    "wikidatacharacter": build_wikidata_character_sql,
    "other":             build_other_sql,
    "list":              build_list_sql,
    "movement":          build_movement_sql,
    "collection":        build_collection_sql,
    "group":             build_group_sql,
    "death":             build_death_sql,
    "award":             build_award_sql,
    "nomination":        build_nomination_sql,
    "topic":             build_topic_sql,
    "technical":         build_technical_sql,
    "character":         build_character_sql,
    "tmdbcollection":    build_tmdb_collection_sql,
    "episode":           build_episode_sql,
    "keyword":           build_keyword_sql,
    "season":            build_season_sql,
    "sourcework":        build_sourcework_sql,
}


# Extra row filter a family applies to its source table on top of the ID_WIKIDATA
# guard, honoured by the `--no-exclusions` path too (wikipedia_functions._buildsourcesql),
# so bypassing the ownership chain never resurrects rows the family itself refuses.
CONTENT_SOURCE_FILTERS = {
    "sourcework": SOURCEWORK_ROW_FILTER,
}
