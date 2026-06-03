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
    extract_titles_and_text,
    get_linked_pages,
    get_linked_pages_batch,
)
from wikipedia_http import get_session, get_worker_count, rate_limit
#import re

# Load .env file 
load_dotenv()

strwikipediauseragent = os.getenv("WIKIMEDIA_USER_AGENT")
print("strwikipediauseragent",strwikipediauseragent)
headers = {
    'User-Agent': strwikipediauseragent
}

cwd = os.getcwd()


def f_fetchlangpayload(session, wikidata_id, page_title, strkey, strlanguage, needs_image):
    """Fetch all network/parse data for one (entity, language) Wikipedia page.

    Runs inside a worker thread (Phase 1d): performs ONLY network requests and HTML
    parsing, then returns a plain dict. It does NO database work — PyMySQL's single
    shared connection is not thread-safe — so the caller writes the returned payload
    to the database on the main thread via ``f_writelangtodb``.

    The rendered page HTML is fetched once and reused for both section extraction and
    image captions (Phase 1b).
    """
    strwikipediapageurl = (
        f"https://{strlanguage}.wikipedia.org/wiki/"
        f"{urllib.parse.quote(page_title.replace(' ', '_'))}"
    )
    payload = {
        "site_key": strkey,
        "page_title": page_title,
        "page_url": strwikipediapageurl,
        "main_image_url": "",
        "page_images": [],
        "http_status": None,
        "success": False,
        "has_content": False,
        "sections": [],
    }

    # Lead/main image — only for content types that store one.
    if needs_image:
        try:
            strmainimageurl = wimg.get_wikipedia_main_image_url(page_title, strlanguage)
            if strmainimageurl:
                payload["main_image_url"] = strmainimageurl
        except Exception as err:
            print(f"Main image retrieval error for {wikidata_id} ({strlanguage}): {err}")

    # Rendered HTML, fetched once and reused below (sections + image captions).
    url = f'https://{strlanguage}.wikipedia.org/w/api.php'
    params = {
        'action': 'parse',
        'page': page_title,
        'prop': 'text',
        'formatversion': 2,
        'format': 'json',
        'maxlag': 5,
    }
    intsuccess = False
    inthttpstatus = None
    response = None
    try:
        rate_limit()
        response = session.get(url, params=params, timeout=30)
        inthttpstatus = response.status_code
        intsuccess = (inthttpstatus == 200)
        if not intsuccess:
            print(f'parse API HTTP {inthttpstatus} for {page_title} ({strlanguage})')
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  # Handle specific HTTP errors
    except requests.exceptions.ConnectionError as conn_err:
        print(f'Connection error occurred: {conn_err}')  # Handle connection errors
    except requests.exceptions.Timeout as timeout_err:
        print(f'Timeout error occurred: {timeout_err}')  # Handle timeout errors
    except requests.exceptions.RequestException as req_err:
        print(f'Request error occurred: {req_err}')  # Handle other request-related errors
    except Exception as err:
        print(f'An error occurred: {err}')  # Handle any other exceptions
    payload["http_status"] = inthttpstatus
    payload["success"] = intsuccess

    data = None
    if intsuccess:
        try:
            data = response.json()
        except ValueError as json_err:
            print(f'parse API JSON decode error for {page_title} ({strlanguage}): {json_err}')
            data = None

    soup = None
    if intsuccess and data is not None:
        wikipedia_page_content = data['parse']['text']
        if wikipedia_page_content:
            payload["has_content"] = True
            wikipedia_page_content = "<body>" + wikipedia_page_content + "</body>"
            soup = BeautifulSoup(wikipedia_page_content, 'html.parser')
            payload["sections"] = extract_titles_and_text(soup=soup)

    # All page images. The image list comes from the query API; captions reuse the
    # soup above when available, otherwise fall back to the function's own fetch.
    try:
        if soup is not None:
            payload["page_images"] = wimg.get_wikipedia_page_images(page_title, strlanguage, soup=soup)
        else:
            payload["page_images"] = wimg.get_wikipedia_page_images(page_title, strlanguage)
    except Exception as err:
        print(f"All page images retrieval error for {wikidata_id} ({strlanguage}): {err}")
        payload["page_images"] = []

    return payload


def f_writelangtodb(payload, lngid, wikidata_id, strlanguage, strcontent, intindex,
                    strimagetable, strimagecolumn, cursor2, lngencount, lngfrcount):
    """Persist one (entity, language) payload produced by ``f_fetchlangpayload``.

    Runs on the main thread only (single shared PyMySQL connection). Mirrors the
    original per-row database writes; returns the updated (lngencount, lngfrcount)
    English/French page counters.
    """
    strkey = payload["site_key"]
    page_title = payload["page_title"]
    strwikipediapageurl = payload["page_url"]
    strmainimageurl = payload["main_image_url"]

    arrcouples = {}
    arrcouples["ID_WIKIDATA"] = wikidata_id
    arrcouples["LANG"] = strlanguage
    arrcouples["ITEM_TYPE"] = strcontent
    arrcouples["WIKIPEDIA_SITE_KEY"] = strkey
    arrcouples["WIKIPEDIA_PAGE_TITLE"] = page_title
    arrcouples["WIKIPEDIA_PAGE_URL"] = strwikipediapageurl
    arrcouples["PAGE_EXISTS"] = 1
    strsqltablename = "T_WC_WIKIPEDIA_PAGE_LANG"
    strsqlupdatecondition = f"ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}'"
    cp.f_sqlupdatearray(strsqltablename, arrcouples, strsqlupdatecondition, 1)

    if not page_title:
        return lngencount, lngfrcount

    # Main image row (when this content type stores one).
    if strimagetable != "" and strimagecolumn != "":
        if strmainimageurl:
            print("Found an image:", strmainimageurl)
            arrcouples = {}
            arrcouples["ID_WIKIDATA"] = wikidata_id
            arrcouples[strimagecolumn] = strmainimageurl
            strsqlupdatecondition = f"ID_WIKIDATA = '{wikidata_id}'"
            cp.f_sqlupdatearray(strimagetable, arrcouples, strsqlupdatecondition, 1)

    try:
        arrpageimages = payload["page_images"]
        if not strmainimageurl and strimagetable != "" and strimagecolumn != "" and len(arrpageimages) > 0:
            strmainimageurl = arrpageimages[0].get("image_url") or ""
            if strmainimageurl:
                print("Main image fallback (first page image):", strmainimageurl)
                arrcouples = {}
                arrcouples["ID_WIKIDATA"] = wikidata_id
                arrcouples[strimagecolumn] = strmainimageurl
                strsqlupdatecondition = f"ID_WIKIDATA = '{wikidata_id}'"
                cp.f_sqlupdatearray(strimagetable, arrcouples, strsqlupdatecondition, 1)
        # In-place bulk upsert keyed by the (ID_WIKIDATA, LANG, DISPLAY_ORDER)
        # unique index (added by migrations/add_unique_section_image_keys.py),
        # then prune any stale tail rows beyond the current image count. The
        # upsert refreshes existing rows in place and PRESERVES their DAT_CREAT
        # (creation fields are insert-only -- see citizenphil module docs).
        arrimagerows = []
        for imageitem in arrpageimages:
            arrimagerows.append({
                "ID_WIKIDATA": wikidata_id,
                "LANG": strlanguage,
                "ITEM_TYPE": strcontent,
                "DISPLAY_ORDER": imageitem.get("display_order"),
                "IMAGE_URL": imageitem.get("image_url"),
                "IMAGE_URL_NORMALIZED": imageitem.get("image_url_normalized"),
                "THUMBNAIL_URL": imageitem.get("thumbnail_url"),
                "MEDIA_TYPE": imageitem.get("media_type"),
                "FILE_NAME": imageitem.get("file_name"),
                "COMMONS_TITLE": imageitem.get("commons_title"),
                "CAPTION": imageitem.get("caption"),
                "IS_MAIN_IMAGE": 1 if imageitem.get("image_url") == strmainimageurl else 0,
            })
        if arrimagerows:
            cp.f_sqlbulkupsert("T_WC_WIKIPEDIA_PAGE_LANG_IMAGE", arrimagerows, ["ID_WIKIDATA", "LANG", "DISPLAY_ORDER"], 1)
            cursor2.execute(
                f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}' AND DISPLAY_ORDER > {len(arrimagerows)}"
            )
        else:
            cursor2.execute(
                f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}'"
            )
        cp.connectioncp.commit()
    except Exception as err:
        print(f"All page images persistence error for {wikidata_id} ({strlanguage}): {err}")

    # Crawl-status columns (recorded whether or not the parse succeeded).
    arrcouples = {}
    arrcouples["LAST_CRAWLED_AT"] = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    arrcouples["HTTP_STATUS"] = payload["http_status"]
    if payload["success"]:
        arrcouples["LAST_SUCCESS_AT"] = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    strsqltablename = "T_WC_WIKIPEDIA_PAGE_LANG"
    strsqlupdatecondition = f"ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}'"
    cp.f_sqlupdatearray(strsqltablename, arrcouples, strsqlupdatecondition, 1)

    if payload["success"] and payload["has_content"]:
        if strlanguage == "en":
            lngencount += 1
            cp.f_setservervariable("strwikipediacrawler"+strcontent+"englishcount",str(lngencount),"Count of Wikipedia English pages retrieved for "+strcontent,0)
        if strlanguage == "fr":
            lngfrcount += 1
            cp.f_setservervariable("strwikipediacrawler"+strcontent+"frenchcount",str(lngfrcount),"Count of Wikipedia French pages retrieved for "+strcontent,0)
        arrcontent = payload["sections"]
        # In-place bulk upsert keyed by the (ID_WIKIDATA, LANG, DISPLAY_ORDER)
        # unique index (added by migrations/add_unique_section_image_keys.py),
        # then prune any stale tail rows beyond the current section count. The
        # upsert refreshes existing rows in place and PRESERVES their DAT_CREAT
        # (creation fields are insert-only -- see citizenphil module docs).
        arrsectionrows = []
        lngdisplayorder = 0
        for i, (strsectiontitle, strsectioncontent) in enumerate(arrcontent):
            print(f"{i}. Title: {strsectiontitle}")
            strsectioncontent = strsectioncontent.replace("[edit]","")
            if len(strsectiontitle) > 300:
                strsectiontitle = strsectiontitle[:300]
            lngdisplayorder += 1
            arrsectionrows.append({
                "ID_WIKIDATA": wikidata_id,
                "LANG": strlanguage,
                "ITEM_TYPE": strcontent,
                "DISPLAY_ORDER": lngdisplayorder,
                "TITLE": strsectiontitle,
                "CONTENT": strsectioncontent,
            })

            # Extract Format data from movie, fr, Fiche Technique section
            if intindex == 201:
                # This is a movie, so we have extra processing because the French Wikipedia page holds technical data about the movie
                if strlanguage == "fr":
                    # In French
                    if strsectiontitle == "Fiche technique":
                        # Fiche technique
                        strstringbegin = "\n- Format"
                        strstringend = "\n- "
                        strformatline = ""
                        lngbeginindex = strsectioncontent.find(strstringbegin)
                        if lngbeginindex == -1:
                            strstringbegin = "- Format"
                            lngbeginindex = strsectioncontent.find(strstringbegin)
                        if lngbeginindex != -1:
                            # Begin string found
                            lngbeginindex += len(strstringbegin)
                            lngendindex = strsectioncontent.find(strstringend, lngbeginindex)
                            if lngendindex != -1:
                                strformatline = strsectioncontent[lngbeginindex:lngendindex].strip()
                            else:
                                strformatline = strsectioncontent[lngbeginindex:].strip()
                        if strformatline != "":
                            if strformatline[0:2] == ": ":
                                strformatline = strformatline[2:]
                        print("Format :",strformatline)
                        arrcouples = {}
                        arrcouples["WIKIPEDIA_FORMAT_LINE"] = strformatline
                        arrcouples["DAT_WIKIPEDIA_FORMAT_LINE"] = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                        strsqltablename = "T_WC_TMDB_MOVIE"
                        strsqlupdatecondition = f"ID_MOVIE = {lngid}"
                        cp.f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)

        if arrsectionrows:
            cp.f_sqlbulkupsert("T_WC_WIKIPEDIA_PAGE_LANG_SECTION", arrsectionrows, ["ID_WIKIDATA", "LANG", "DISPLAY_ORDER"], 1)
            cursor2.execute(
                f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}' AND DISPLAY_ORDER > {lngdisplayorder}"
            )
        else:
            cursor2.execute(
                f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}'"
            )
        cp.connectioncp.commit()

    return lngencount, lngfrcount


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
                                        future = executor.submit(
                                            f_fetchlangpayload, session, wikidata_id,
                                            page_title, strkey, strlanguage, needs_image,
                                        )
                                    else:
                                        future = None
                                    arrtasks.append((strlanguage, blnpresent, future))
                                arrrowtasks.append((row, arrtasks))
                            # Drain in submission order; all DB writes stay on this thread.
                            for row, arrtasks in arrrowtasks:
                                lngid = row['id']
                                wikidata_id = row['ID_WIKIDATA']
                                print(f"TMDb {strcontent} id {lngid} Wikidata id: {wikidata_id} ")
                                for strlanguage, blnpresent, future in arrtasks:
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
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    conn = getattr(cp, "connectioncp", None)
    if conn is not None and getattr(conn, "open", False):
        conn.rollback()
