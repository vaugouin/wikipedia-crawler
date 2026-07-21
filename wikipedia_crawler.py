import requests
import json
import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import time
import pymysql.cursors
import citizenphil as cp
from datetime import datetime
from bs4 import BeautifulSoup
import wikipedia_images as wimg
from wikipedia_crawler_helpers import (
    WikidataTransientError,
    extract_titles_and_text,
    get_linked_pages,
    get_linked_pages_batch,
)
from wikipedia_http import get_session, get_worker_count, rate_limit
# Per-entity fetch + persist logic is shared with the single-Qid entry point
# (wikipedia_functions.py); it lives in wikipedia_page_writer.py so that module
# can be imported without launching a full crawl.
from wikipedia_page_writer import f_fetchlangpayload, f_writelangtodb
#import re

# Load .env file 
load_dotenv()

strwikipediauseragent = os.getenv("WIKIMEDIA_USER_AGENT")
print("strwikipediauseragent",strwikipediauseragent)
headers = {
    'User-Agent': strwikipediauseragent
}

cwd = os.getcwd()


# Quick-mode container: runs ONLY a fixed subset of processes and skips writes
# to shared resume-state server variables so the main wikipedia-crawler
# container can keep running 202 (person) in parallel without corruption.
intquickmode = True
#intquickmode = False
#arrquickprocessids = {205, 218, 223}
#arrquickprocessids = {220, 222}
# Ordered from least-recently-updated to most-recently-updated ITEM_TYPE in
# T_WC_WIKIPEDIA_PAGE_LANG_SECTION (MAX(TIM_UPDATED) ASC), so quick-mode
# refreshes the stalest content first.
arrquickprocessids = [
    212,  # collection      (never updated)
    213,  # group           (never updated)
    214,  # death           (never updated)
    219,  # tmdbcollection  (never updated)
    221,  # keyword         (never updated)
    209,  # other       (2026-04-14)
    203,  # item        (2026-05-08)
    204,  # serie       (2026-05-09)
    210,  # list        (2026-05-09)
    211,  # movement    (2026-05-09)
    215,  # award       (2026-05-09)
    216,  # nomination  (2026-05-09)
    217,  # topic       (2026-05-09)
    201,  # movie       (2026-05-18)
    205,  # character   (2026-05-20)
    218,  # character   (2026-05-20)
    223,  # technical   (2026-05-20)
    202,  # person      (2026-05-23)
    220,  # episode     (2026-05-24)
    222,  # season      (2026-05-24)
]

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

# wikidata_id = "Q24829" # Wikidata ID for Orson Welles

#strlanguage = 'en'
strlanguage = 'fr'
strprops = 'sitelinks'

strprocessesexecutedprevious = cp.f_getservervariable("strwikipediacrawlerprocessesexecuted",0)
strprocessesexecuteddesc = "List of processes executed in the Wikipedia crawler for Wikipedia pages retrieval"
if not intquickmode:
    cp.f_setservervariable("strwikipediacrawlerprocessesexecutedprevious",strprocessesexecutedprevious,strprocessesexecuteddesc + " (previous execution)",0)
strprocessesexecuted = ""
if not intquickmode:
    cp.f_setservervariable("strwikipediacrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)

# Connect to the database
#connection = pymysql.connect(host=cp.strdbhost, user=cp.strdbuser, password=cp.strdbpassword, database=cp.strdbname, cursorclass=pymysql.cursors.DictCursor)

try:
    conn = cp.f_getconnection()
    with conn:
        with conn.cursor() as cursor:
            cursor2 = conn.cursor()
            # Start timing the script execution
            start_time = time.time()
            strcurrentprocessdesc = "Current process in the Wikipedia crawler for Wikipedia pages retrieval"
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            if not intquickmode:
                cp.f_setservervariable("strwikipediacrawlerstartdatetime",strnow,"Date and time of the last start of the Wikipedia crawler",0)
            strtotalruntimedesc = "Total runtime of the Wikipedia crawler"
            strtotalruntimeprevious = cp.f_getservervariable("strwikipediacrawlertotalruntime",0)
            if not intquickmode:
                cp.f_setservervariable("strwikipediacrawlertotalruntimeprevious",strtotalruntimeprevious,strtotalruntimedesc + " (previous execution)",0)
            strtotalruntime = "RUNNING"
            if not intquickmode:
                cp.f_setservervariable("strwikipediacrawlertotalruntime",strtotalruntime,strtotalruntimedesc,0)
            
            strmovieidold = cp.f_getservervariable("strwikipediacrawlermovieid",0)
            strpersonidold = cp.f_getservervariable("strwikipediacrawlerpersonid",0)
            stritemidold = cp.f_getservervariable("strwikipediacrawleritemid",0)
            strotheridold = cp.f_getservervariable("strwikipediacrawlerotherid",0)
            strserieidold = cp.f_getservervariable("strwikipediacrawlerserieid",0)
            strcharacteridold = cp.f_getservervariable("strwikipediacrawlercharacterid",0)
            strlistidold = cp.f_getservervariable("strwikipediacrawlerlistid",0)
            strmovementidold = cp.f_getservervariable("strwikipediacrawlermovementid",0)
            strcollectionidold = cp.f_getservervariable("strwikipediacrawlercollectionid",0)
            strgroupidold = cp.f_getservervariable("strwikipediacrawlergroupid",0)
            strdeathidold = cp.f_getservervariable("strwikipediacrawlerdeathid",0)
            strawardidold = cp.f_getservervariable("strwikipediacrawlerawardid",0)
            strnominationidold = cp.f_getservervariable("strwikipediacrawlernominationid",0)
            strtopicidold = cp.f_getservervariable("strwikipediacrawlertopicid",0)
            strtechnicalidold = cp.f_getservervariable("strwikipediacrawlertechnicalid",0)
            strcharacteridold = cp.f_getservervariable("strwikipediacrawlercharacterid",0)
            strtmdbcollectionidold = cp.f_getservervariable("strwikipediacrawlertmdbcollectionid",0)
            strepisodeidold = cp.f_getservervariable("strwikipediacrawlerepisodeid",0)
            strkeywordidold = cp.f_getservervariable("strwikipediacrawlerkeywordid",0)
            strseasonidold = cp.f_getservervariable("strwikipediacrawlerseasonid",0)
            strcurrentcontent = cp.f_getservervariable("strwikipediacrawlercurrentcontent",0)
            arrprocesses = [
                {
                    "id": 201,
                    "content": "movie",
                    "resumeid": strmovieidold,
                    "sqlbuilder": build_movie_sql,
                    "imagetable": "T_WC_WIKIDATA_MOVIE_V1",
                    "imagecolumn": "WIKIPEDIA_POSTER_PATH",
                },
                {
                    "id": 202,
                    "content": "person",
                    "resumeid": strpersonidold,
                    "sqlbuilder": build_person_sql,
                    "imagetable": "T_WC_WIKIDATA_PERSON_V1",
                    "imagecolumn": "WIKIPEDIA_PROFILE_PATH",
                },
                {
                    "id": 203,
                    "content": "item",
                    "resumeid": stritemidold,
                    "sqlbuilder": build_item_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 204,
                    "content": "serie",
                    "resumeid": strserieidold,
                    "sqlbuilder": build_serie_sql,
                    "imagetable": "T_WC_WIKIDATA_SERIE_V1",
                    "imagecolumn": "WIKIPEDIA_POSTER_PATH",
                },
                {
                    "id": 205,
                    "content": "character",
                    "resumeid": strcharacteridold,
                    "sqlbuilder": build_character_sql,
                    "imagetable": "T_WC_WIKIDATA_CHARACTER_V1",
                    "imagecolumn": "WIKIPEDIA_PROFILE_PATH",
                },
                {
                    "id": 209,
                    "content": "other",
                    "resumeid": strotheridold,
                    "sqlbuilder": build_other_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 210,
                    "content": "list",
                    "resumeid": strlistidold,
                    "sqlbuilder": build_list_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 211,
                    "content": "movement",
                    "resumeid": strmovementidold,
                    "sqlbuilder": build_movement_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 212,
                    "content": "collection",
                    "resumeid": strcollectionidold,
                    "sqlbuilder": build_collection_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 213,
                    "content": "group",
                    "resumeid": strgroupidold,
                    "sqlbuilder": build_group_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 214,
                    "content": "death",
                    "resumeid": strdeathidold,
                    "sqlbuilder": build_death_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 215,
                    "content": "award",
                    "resumeid": strawardidold,
                    "sqlbuilder": build_award_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 216,
                    "content": "nomination",
                    "resumeid": strnominationidold,
                    "sqlbuilder": build_nomination_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 217,
                    "content": "topic",
                    "resumeid": strtopicidold,
                    "sqlbuilder": build_topic_sql,
                    "imagetable": "T_WC_WIKIDATA_ITEM_V1",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 223,
                    "content": "technical",
                    "resumeid": strtechnicalidold,
                    "sqlbuilder": build_technical_sql,
                    "imagetable": "T_WC_T2S_TECHNICAL",
                    "imagecolumn": "WIKIPEDIA_IMAGE_PATH",
                },
                {
                    "id": 218,
                    "content": "character",
                    "resumeid": strcharacteridold,
                    "sqlbuilder": build_character_sql,
                    "imagetable": "T_WC_WIKIDATA_CHARACTER_V1",
                    "imagecolumn": "WIKIPEDIA_PROFILE_PATH",
                },
                {
                    "id": 219,
                    "content": "tmdbcollection",
                    "resumeid": strtmdbcollectionidold,
                    "sqlbuilder": build_tmdb_collection_sql,
                    "imagetable": "",
                    "imagecolumn": "",
                },
                {
                    "id": 220,
                    "content": "episode",
                    "resumeid": strepisodeidold,
                    "sqlbuilder": build_episode_sql,
                    "imagetable": "",
                    "imagecolumn": "",
                },
                {
                    "id": 221,
                    "content": "keyword",
                    "resumeid": strkeywordidold,
                    "sqlbuilder": build_keyword_sql,
                    "imagetable": "",
                    "imagecolumn": "",
                },
                {
                    "id": 222,
                    "content": "season",
                    "resumeid": strseasonidold,
                    "sqlbuilder": build_season_sql,
                    "imagetable": "",
                    "imagecolumn": "",
                },
            ]
            resume_index = 0
            if strcurrentcontent != "":
                for index, processconfig in enumerate(arrprocesses):
                    if processconfig["content"] == strcurrentcontent:
                        resume_index = index
                        break
            if intquickmode:
                arrprocessesbyid = {p["id"]: p for p in arrprocesses}
                arrprocessscope = [arrprocessesbyid[intid] for intid in arrquickprocessids if intid in arrprocessesbyid]
            else:
                arrprocessscope = arrprocesses[resume_index:]
            for processconfig in arrprocessscope:
                intindex = processconfig["id"]
                strcontent = processconfig["content"]
                strsql = processconfig["sqlbuilder"](processconfig["resumeid"])
                strimagetable = processconfig["imagetable"]
                strimagecolumn = processconfig["imagecolumn"]
                strcurrentprocess = f"{intindex}: processing Wikipedia English and French " + strcontent + " content"
                strprocessesexecuted += str(intindex) + ", "
                if not intquickmode:
                    cp.f_setservervariable("strwikipediacrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                if strsql != "":
                    print(strcurrentprocess)
                    if not intquickmode:
                        cp.f_setservervariable("strwikipediacrawlercurrentprocess",strcurrentprocess,"Current process in the Wikipedia crawler",0)
                        cp.f_setservervariable("strwikipediacrawlercurrentcontent",strcontent,"Current content processed in the Wikipedia crawler",0)
                    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                    cp.f_setservervariable("strwikipediacrawler"+strcontent+"startdatetime",strnow,"Date and time of the last start of the Wikipedia crawler for "+strcontent,0)
                    print(strsql)
                    #cp.f_setservervariable("strwikipediacrawlercurrentsql",strsql,"Current SQL query in the Wikipedia crawler",0)
                    cursor.execute(strsql)
                    lngrowcount = cursor.rowcount
                    print(f"{lngrowcount} lines for "+strcontent)
                    lngfrcount = 0
                    lngencount = 0
                    # Fetching all rows from the last executed statement
                    results = cursor.fetchall()
                    arrrows = list(results)
                    arrlang = {1: 'en', 2: 'fr'}
                    needs_image = (strimagetable != "" and strimagecolumn != "")
                    session = get_session()
                    intworkers = get_worker_count()
                    # Phase 1c/1d: resolve up to 50 titles (en|fr) per Wikidata call,
                    # fetch each (entity, language) page concurrently, then write every
                    # result to the DB here on the main thread (single shared connection).
                    with ThreadPoolExecutor(max_workers=intworkers) as executor:
                        for intchunkstart in range(0, len(arrrows), 50):
                            arrchunk = arrrows[intchunkstart:intchunkstart + 50]
                            arrids = [row['ID_WIKIDATA'] for row in arrchunk]
                            batch = get_linked_pages_batch(arrids, strprops, 'en|fr')
                            arrentities = batch.get('entities', {}) if isinstance(batch, dict) else {}
                            # Submit a fetch task per (row, language) with a resolvable title.
                            arrrowtasks = []
                            for row in arrchunk:
                                wikidata_id = row['ID_WIKIDATA']
                                entity = arrentities.get(wikidata_id) or {}
                                sitelinks = (entity.get(strprops) or {}) if strprops else {}
                                arrtasks = []
                                for intlang, strlanguage in arrlang.items():
                                    strkey = strlanguage + 'wiki'
                                    blnpresent = strkey in sitelinks
                                    page_title = (sitelinks.get(strkey) or {}).get('title') if blnpresent else None
                                    if page_title:
                                        strpageurl = (
                                            f"https://{strlanguage}.wikipedia.org/wiki/"
                                            f"{urllib.parse.quote(page_title.replace(' ', '_'))}"
                                        )
                                        future = executor.submit(
                                            f_fetchlangpayload, session, wikidata_id,
                                            page_title, strkey, strlanguage, needs_image,
                                        )
                                    else:
                                        strpageurl = None
                                        future = None
                                    arrtasks.append((strlanguage, blnpresent, future, strpageurl))
                                arrrowtasks.append((row, arrtasks))
                            # Drain in submission order; all DB writes stay on this thread.
                            for row, arrtasks in arrrowtasks:
                                lngid = row['id']
                                wikidata_id = row['ID_WIKIDATA']
                                print("-" * 80)
                                print(f"TMDb {strcontent} id {lngid} Wikidata id: {wikidata_id} ")
                                for strlanguage, blnpresent, future, strpageurl in arrtasks:
                                    if strpageurl:
                                        print(f"  {strlanguage}: {strpageurl}")
                                for strlanguage, blnpresent, future, strpageurl in arrtasks:
                                    if future is None:
                                        if blnpresent:
                                            print(f'No Wikipedia page found for {strcontent} id {wikidata_id} and language code {strlanguage}')
                                        continue
                                    payload = future.result()
                                    lngencount, lngfrcount = f_writelangtodb(
                                        payload, lngid, wikidata_id, strlanguage, strcontent,
                                        intindex, strimagetable, strimagecolumn, cursor2,
                                        lngencount, lngfrcount,
                                    )
                                #cp.f_tmdbmoviesetwikipediacompleted(lngid)
                                cp.f_setservervariable("strwikipediacrawler"+strcontent+"wikidataid",wikidata_id,"Current wikidata id in the Wikipedia crawler for "+strcontent,0)
                                cp.f_setservervariable("strwikipediacrawler"+strcontent+"id",str(lngid),"Current id in the Wikipedia crawler for "+strcontent,0)
                        
                    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                    cp.f_setservervariable("strwikipediacrawler"+strcontent+"enddatetime",strnow,"Date and time of the last end of the Wikipedia crawler for "+strcontent,0)
                    cp.f_setservervariable("strwikipediacrawler"+strcontent+"id","","Current id in the Wikipedia crawler for "+strcontent,0)
                    # Define what is the next content to process
                    strnextcontent = ""
                    for index, nextprocessconfig in enumerate(arrprocesses):
                        if nextprocessconfig["content"] == strcontent:
                            if index + 1 < len(arrprocesses):
                                strnextcontent = arrprocesses[index + 1]["content"]
                            break
                    if not intquickmode:
                        cp.f_setservervariable("strwikipediacrawlercurrentcontent",strnextcontent,"Current content processed in the Wikipedia crawler",0)
            strcurrentprocess = ""
            if not intquickmode:
                cp.f_setservervariable("strwikipediacrawlercurrentprocess",strcurrentprocess,strcurrentprocessdesc,0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            if not intquickmode:
                cp.f_setservervariable("strwikipediacrawlerenddatetime",strnow,"Date and time of the Wikipedia crawler ending",0)
            # Calculate total runtime and convert to readable format
            end_time = time.time()
            strtotalruntime = int(end_time - start_time)  # Total runtime in seconds
            if not intquickmode:
                cp.f_setservervariable("strwikipediacrawlertotalruntimesecond",str(strtotalruntime),strtotalruntimedesc,0)
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            if not intquickmode:
                cp.f_setservervariable("strwikipediacrawlertotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
    print("Process completed")
except WikidataTransientError as e:
    # WIKIPEDIA-CRAWLER-017: a Wikidata maxlag / transient outage persisted through all
    # retries. Stop cleanly: the per-entity resume server-vars still point at the last
    # SUCCESSFULLY processed entity, so the un-checkpointed chunk is re-crawled on the next
    # run. Never let this be swallowed as "no sitelinks" (silent content loss).
    print(f"⏸️  Wikidata transient outage, stopping to resume next run: {e}")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    conn = getattr(cp, "connectioncp", None)
    if conn is not None and getattr(conn, "open", False):
        conn.rollback()
