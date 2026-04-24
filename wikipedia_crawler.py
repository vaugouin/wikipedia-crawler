import requests
import json
import os
from dotenv import load_dotenv
import time
import pymysql.cursors
import citizenphil as cp
from datetime import datetime
import wikipedia_images as wimg
from wikipedia_crawler_helpers import extract_titles_and_text, get_linked_pages
#import re

# Load .env file 
load_dotenv()

strwikipediauseragent = os.getenv("WIKIMEDIA_USER_AGENT")
print("strwikipediauseragent",strwikipediauseragent)
headers = {
    'User-Agent': strwikipediauseragent
}

cwd = os.getcwd()

def append_exclusion_tables(strsql, arrtables):
    for strtable in arrtables:
        strsql += "AND ID_WIKIDATA NOT IN (SELECT ID_WIKIDATA FROM " + strtable + ") "
    return strsql

def append_exclusion_queries(strsql, arrqueries):
    for strquery in arrqueries:
        strsql += "AND ID_WIKIDATA NOT IN (" + strquery + ") "
    return strsql

def build_movie_sql(strresumeid):
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_MOVIE.ID_MOVIE AS id, ID_WIKIDATA FROM T_WC_TMDB_MOVIE "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    if strresumeid != "":
        strsql += "AND ID_MOVIE >= " + strresumeid + " "
    strsql += "ORDER BY ID_MOVIE ASC "
    return strsql

def build_person_sql(strresumeid):
    strsql = ""
    strsql += "SELECT DISTINCT T_WC_TMDB_PERSON.ID_PERSON AS id, ID_WIKIDATA FROM T_WC_TMDB_PERSON "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql += "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    if strresumeid != "":
        strsql += "AND ID_PERSON >= " + strresumeid + " "
    strsql += "ORDER BY ID_PERSON ASC "
    return strsql

def build_item_sql(strresumeid):
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

def build_serie_sql(strresumeid):
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

def build_other_sql(strresumeid):
    strsql = ""
    strsql += "SELECT DISTINCT 'Q1204187' AS id, 'Q1204187' AS ID_WIKIDATA FROM DUAL "
    strsql += "WHERE 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_MOVIE_V1) "
    strsql += "AND 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1) "
    strsql += "AND 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_ITEM_V1) "
    strsql += "AND 'Q1204187' NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_SERIE_V1) "
    return strsql

def build_list_sql(strresumeid):
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_LIST "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
    ])
    strsql = append_exclusion_queries(strsql, [
        "SELECT 'Q1204187' AS ID_WIKIDATA FROM DUAL",
    ])
    if strresumeid != "":
        strsql += "AND ID_WIKIDATA >= '" + strresumeid + "' "
    strsql += "ORDER BY ID_WIKIDATA ASC "
    return strsql

def build_movement_sql(strresumeid):
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_MOVEMENT "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
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
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_COLLECTION "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
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
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_GROUP "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
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
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_DEATH "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
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
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_AWARD "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
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
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_NOMINATION "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
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
    strsql = ""
    strsql += "SELECT DISTINCT ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_T2S_TOPIC "
    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
    strsql = append_exclusion_tables(strsql, [
        "T_WC_WIKIDATA_MOVIE_V1",
        "T_WC_WIKIDATA_PERSON_V1",
        "T_WC_WIKIDATA_ITEM_V1",
        "T_WC_WIKIDATA_SERIE_V1",
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

# wikidata_id = "Q24829" # Wikidata ID for Orson Welles

#strlanguage = 'en'
strlanguage = 'fr'
strprops = 'sitelinks'

strprocessesexecutedprevious = cp.f_getservervariable("strwikipediacrawlerprocessesexecuted",0)
strprocessesexecuteddesc = "List of processes executed in the Wikipedia crawler for Wikipedia pages retrieval"
cp.f_setservervariable("strwikipediacrawlerprocessesexecutedprevious",strprocessesexecutedprevious,strprocessesexecuteddesc + " (previous execution)",0)
strprocessesexecuted = ""
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
            cp.f_setservervariable("strwikipediacrawlerstartdatetime",strnow,"Date and time of the last start of the Wikipedia crawler",0)
            strtotalruntimedesc = "Total runtime of the Wikipedia crawler"
            strtotalruntimeprevious = cp.f_getservervariable("strwikipediacrawlertotalruntime",0)
            cp.f_setservervariable("strwikipediacrawlertotalruntimeprevious",strtotalruntimeprevious,strtotalruntimedesc + " (previous execution)",0)
            strtotalruntime = "RUNNING"
            cp.f_setservervariable("strwikipediacrawlertotalruntime",strtotalruntime,strtotalruntimedesc,0)
            
            strmovieidold = cp.f_getservervariable("strwikipediacrawlermovieid",0)
            strpersonidold = cp.f_getservervariable("strwikipediacrawlerpersonid",0)
            stritemidold = cp.f_getservervariable("strwikipediacrawleritemid",0)
            strotheridold = cp.f_getservervariable("strwikipediacrawlerotherid",0)
            strserieidold = cp.f_getservervariable("strwikipediacrawlerserieid",0)
            strlistidold = cp.f_getservervariable("strwikipediacrawlerlistid",0)
            strmovementidold = cp.f_getservervariable("strwikipediacrawlermovementid",0)
            strcollectionidold = cp.f_getservervariable("strwikipediacrawlercollectionid",0)
            strgroupidold = cp.f_getservervariable("strwikipediacrawlergroupid",0)
            strdeathidold = cp.f_getservervariable("strwikipediacrawlerdeathid",0)
            strawardidold = cp.f_getservervariable("strwikipediacrawlerawardid",0)
            strnominationidold = cp.f_getservervariable("strwikipediacrawlernominationid",0)
            strtopicidold = cp.f_getservervariable("strwikipediacrawlertopicid",0)
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
            ]
            resume_index = 0
            if strcurrentcontent != "":
                for index, processconfig in enumerate(arrprocesses):
                    if processconfig["content"] == strcurrentcontent:
                        resume_index = index
                        break
            arrprocessscope = arrprocesses[resume_index:]
            for processconfig in arrprocessscope:
                intindex = processconfig["id"]
                strcontent = processconfig["content"]
                strsql = processconfig["sqlbuilder"](processconfig["resumeid"])
                strimagetable = processconfig["imagetable"]
                strimagecolumn = processconfig["imagecolumn"]
                strcurrentprocess = f"{intindex}: processing Wikipedia English and French " + strcontent + " content"
                strprocessesexecuted += str(intindex) + ", "
                cp.f_setservervariable("strwikipediacrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                if strsql != "":
                    print(strcurrentprocess)
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
                    # Iterating through the results and printing
                    for row in results:
                        # print("------------------------------------------")
                        lngid = row['id']
                        wikidata_id = row['ID_WIKIDATA']
                        print(f"TMDb {strcontent} id {lngid} Wikidata id: {wikidata_id} ")
                        arrlang = {1: 'en', 2:'fr'}
                        for intlang, strlanguage in arrlang.items():
                            page_content = get_linked_pages(wikidata_id, strprops, strlanguage)
                            if page_content:
                                print(page_content)
                                if 'entities' in page_content:
                                    if wikidata_id in page_content['entities']:
                                        if strprops in page_content['entities'][wikidata_id]:
                                            strkey = strlanguage + 'wiki'
                                            if strkey in page_content['entities'][wikidata_id][strprops]:
                                                page_title = page_content['entities'][wikidata_id][strprops][strkey]['title']
                                                print(f"{strlanguage}: {page_title}")
                                                #print("Now retrieving the image for this content")
                                                if page_title:
                                                    if strimagetable != "" and strimagecolumn != "":
                                                        try:
                                                            strmainimageurl = wimg.get_wikipedia_main_image_url(page_title, strlanguage)
                                                            if strmainimageurl:
                                                                print("Found an image:", strmainimageurl)
                                                                arrcouples = {}
                                                                arrcouples[strimagecolumn] = strmainimageurl
                                                                strsqlupdatecondition = f"ID_WIKIDATA = '{wikidata_id}'"
                                                                cp.f_sqlupdatearray(strimagetable, arrcouples, strsqlupdatecondition, 1)
                                                            #else:
                                                            #    print("No image found")
                                                        except Exception as err:
                                                            print(f"Main image retrieval error for {wikidata_id} ({strlanguage}): {err}")
                                                    url = f'https://{strlanguage}.wikipedia.org/w/api.php'
                                                    params = {
                                                        'action': 'parse',
                                                        'page': page_title,
                                                        'prop': 'text',
                                                        'formatversion': 2,
                                                        'format': 'json'
                                                    }
                                                    # By default we assume no success
                                                    intsuccess = False
                                                    #print(url)
                                                    try:
                                                        response = requests.get(url, params=params, headers=headers)
                                                        intsuccess = True
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
                                                    if intsuccess:
                                                        data = response.json()
                                                        """
                                                        strjsonfilename = wikidata_id + '-' + strlanguage + '-data.json'
                                                        file_path = os.path.join(cwd, strjsonfilename)
                                                        with open(file_path, 'w') as file:
                                                            file.write(json.dumps(data, ensure_ascii=False))
                                                        print(f'File created: {file_path}')
                                                        """
                                                        wikipedia_page_content = data['parse']['text']
                                                        if wikipedia_page_content:
                                                            # print(wikipedia_page_content)
                                                            """
                                                            strpagetitle = page_title.replace(" ","_")
                                                            strfilename = strcontent + "/" + strlanguage + "/" + wikidata_id + '.html'
                                                            file_path = os.path.join(cwd, strfilename)
                                                            with open(file_path, 'w') as file:
                                                                file.write(wikipedia_page_content)
                                                            #print(f'File created: {file_path}')
                                                            """
                                                            if strlanguage == "en":
                                                                lngencount += 1
                                                                cp.f_setservervariable("strwikipediacrawler"+strcontent+"englishcount",str(lngencount),"Count of Wikipedia English pages retrieved for "+strcontent,0)
                                                            if strlanguage == "fr":
                                                                lngfrcount += 1
                                                                cp.f_setservervariable("strwikipediacrawler"+strcontent+"frenchcount",str(lngfrcount),"Count of Wikipedia French pages retrieved for "+strcontent,0)
                                                            wikipedia_page_content = "<body>" + wikipedia_page_content + "</body>"
                                                            arrcontent = extract_titles_and_text(wikipedia_page_content)
                                                            #print(arrcontent)
                                                            lngdisplayorder = 0
                                                            for i, (strsectiontitle, strsectioncontent) in enumerate(arrcontent):
                                                                print(f"{i}. Title: {strsectiontitle}")
                                                                strsectioncontent = strsectioncontent.replace("[edit]","")
                                                                if len(strsectiontitle) > 300:
                                                                    strsectiontitle = strsectiontitle[:300]
                                                                #print(f"   Text: {strsectioncontent}\n")
                                                                lngdisplayorder += 1
                                                                arrcouples = {}
                                                                arrcouples["ID_WIKIDATA"] = wikidata_id
                                                                arrcouples["LANG"] = strlanguage
                                                                arrcouples["ITEM_TYPE"] = strcontent
                                                                arrcouples["DISPLAY_ORDER"] = lngdisplayorder
                                                                arrcouples["TITLE"] = strsectiontitle
                                                                arrcouples["CONTENT"] = strsectioncontent
                                                                strsqltablename = "T_WC_WIKIPEDIA_PAGE_LANG_SECTION"
                                                                strsqlupdatecondition = f"ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}' AND DISPLAY_ORDER = {lngdisplayorder}"
                                                                cp.f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)
                                                                strsqldelete = f"DELETE FROM {strsqltablename} WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}' AND DISPLAY_ORDER > {lngdisplayorder}"
                                                                # print(f"{strsqldelete}")
                                                                cursor2.execute(strsqldelete)
                                                                cp.connectioncp.commit()
                                                                
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
                                                        # print(wikipedia_page_content)
                                                else:
                                                    print(f'No Wikipedia page found for {strcontent} id {wikidata_id} and language code {strlanguage}')
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
                    cp.f_setservervariable("strwikipediacrawlercurrentcontent",strnextcontent,"Current content processed in the Wikipedia crawler",0)
            strcurrentprocess = ""
            cp.f_setservervariable("strwikipediacrawlercurrentprocess",strcurrentprocess,strcurrentprocessdesc,0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strwikipediacrawlerenddatetime",strnow,"Date and time of the Wikipedia crawler ending",0)
            # Calculate total runtime and convert to readable format
            end_time = time.time()
            strtotalruntime = int(end_time - start_time)  # Total runtime in seconds
            cp.f_setservervariable("strwikipediacrawlertotalruntimesecond",str(strtotalruntime),strtotalruntimedesc,0)
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            cp.f_setservervariable("strwikipediacrawlertotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
    print("Process completed")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    conn = getattr(cp, "connectioncp", None)
    if conn is not None and getattr(conn, "open", False):
        conn.rollback()
