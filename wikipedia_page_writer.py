"""Shared per-entity Wikipedia fetch + persist helpers.

Both the full-batch orchestrator ([wikipedia_crawler.py](wikipedia_crawler.py))
and the single-Qid entry point ([wikipedia_functions.py](wikipedia_functions.py))
crawl one ``(entity, language)`` Wikipedia page exactly the same way. That logic
lives here so the single-Qid path can reuse it WITHOUT importing
``wikipedia_crawler.py`` (importing that module would launch a full crawl, since
its body runs at import time).

Two functions make up the per-entity pipeline:

- ``f_fetchlangpayload`` — network + HTML parsing only (thread-safe, no DB work).
- ``f_writelangtodb`` — persists one payload on the main thread (single shared
  PyMySQL connection).

``CONTENT_CONFIG`` mirrors the per-process ``imagetable`` / ``imagecolumn`` /
``id`` fields declared in ``arrprocesses`` inside ``wikipedia_crawler.py`` and
adds the source table / id column used to resolve an entity's own primary key
(needed for the French movie ``Fiche technique`` writeback). Keep the two in
sync when a process's image destination changes.
"""

import time
import urllib.parse
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import citizenphil as cp
import wikipedia_images as wimg
from wikipedia_crawler_helpers import extract_titles_and_text
from wikipedia_http import rate_limit


# content -> per-process configuration. Mirrors arrprocesses in
# wikipedia_crawler.py (id / imagetable / imagecolumn) and adds sourcetable /
# idcolumn so a single Qid can be resolved back to its own primary key. An empty
# imagetable/imagecolumn means the family stores full page rows but no main-image
# writeback (tmdbcollection, episode, keyword, season).
CONTENT_CONFIG = {
    "movie":             {"id": 201, "imagetable": "T_WC_WIKIDATA_MOVIE_V1",     "imagecolumn": "WIKIPEDIA_POSTER_PATH",  "sourcetable": "T_WC_TMDB_MOVIE",          "idcolumn": "ID_MOVIE"},
    "person":            {"id": 202, "imagetable": "T_WC_WIKIDATA_PERSON_V1",    "imagecolumn": "WIKIPEDIA_PROFILE_PATH", "sourcetable": "T_WC_TMDB_PERSON",         "idcolumn": "ID_PERSON"},
    "item":              {"id": 203, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_WIKIDATA_ITEM_V1",    "idcolumn": "ID_WIKIDATA"},
    "serie":             {"id": 204, "imagetable": "T_WC_WIKIDATA_SERIE_V1",     "imagecolumn": "WIKIPEDIA_POSTER_PATH",  "sourcetable": "T_WC_TMDB_SERIE",          "idcolumn": "ID_SERIE"},
    "wikidatacharacter": {"id": 205, "imagetable": "T_WC_WIKIDATA_CHARACTER_V1", "imagecolumn": "WIKIPEDIA_PROFILE_PATH", "sourcetable": "T_WC_WIKIDATA_CHARACTER_V1", "idcolumn": "ID_WIKIDATA"},
    "other":             {"id": 209, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": None,                       "idcolumn": None},
    "list":              {"id": 210, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_LIST",            "idcolumn": "ID_WIKIDATA"},
    "movement":          {"id": 211, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_MOVEMENT",        "idcolumn": "ID_WIKIDATA"},
    "collection":        {"id": 212, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_COLLECTION",      "idcolumn": "ID_WIKIDATA"},
    "group":             {"id": 213, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_GROUP",          "idcolumn": "ID_WIKIDATA"},
    "death":             {"id": 214, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_DEATH",          "idcolumn": "ID_WIKIDATA"},
    "award":             {"id": 215, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_AWARD",          "idcolumn": "ID_WIKIDATA"},
    "nomination":        {"id": 216, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_NOMINATION",     "idcolumn": "ID_WIKIDATA"},
    "topic":             {"id": 217, "imagetable": "T_WC_WIKIDATA_ITEM_V1",      "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_TOPIC",          "idcolumn": "ID_WIKIDATA"},
    "technical":         {"id": 223, "imagetable": "T_WC_T2S_TECHNICAL",         "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_TECHNICAL",      "idcolumn": "ID_WIKIDATA"},
    "character":         {"id": 218, "imagetable": "T_WC_WIKIDATA_CHARACTER_V1", "imagecolumn": "WIKIPEDIA_PROFILE_PATH", "sourcetable": "T_WC_TMDB_CHARACTER",     "idcolumn": "ID_CHARACTER"},
    "tmdbcollection":    {"id": 219, "imagetable": "",                           "imagecolumn": "",                       "sourcetable": "T_WC_TMDB_COLLECTION",    "idcolumn": "ID_COLLECTION"},
    "episode":           {"id": 220, "imagetable": "",                           "imagecolumn": "",                       "sourcetable": "T_WC_TMDB_EPISODE",       "idcolumn": "ID_EPISODE"},
    "keyword":           {"id": 221, "imagetable": "",                           "imagecolumn": "",                       "sourcetable": "T_WC_TMDB_KEYWORD",       "idcolumn": "ID_KEYWORD"},
    "season":            {"id": 222, "imagetable": "",                           "imagecolumn": "",                       "sourcetable": "T_WC_TMDB_SEASON",        "idcolumn": "ID_SEASON"},
}


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
            # WIKIPEDIA-CRAWLER-019: the summary endpoint occasionally returns a
            # maintenance banner or an edit icon as the "lead" image. Storing it
            # states something false about the subject, so drop it and let the
            # fallback below look for a real picture.
            if strmainimageurl and not wimg.is_acceptable_main_image_url(strmainimageurl):
                print(f"Main image rejected (UI chrome) for {wikidata_id} ({strlanguage}): {strmainimageurl}")
                strmainimageurl = ""
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
    data = None
    # The API reports maxlag throttling and bad/missing titles as an HTTP 200
    # ``error`` envelope, so the session-level urllib3 retry (429/5xx only)
    # never sees them; maxlag is retried here, other API errors are terminal.
    for _ in range(3):
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

        data = None
        if intsuccess:
            try:
                data = response.json()
            except ValueError as json_err:
                print(f'parse API JSON decode error for {page_title} ({strlanguage}): {json_err}')

        arrapierror = data.get('error') if isinstance(data, dict) else None
        if arrapierror is None:
            break
        strerrorcode = arrapierror.get('code', '')
        strerrorinfo = arrapierror.get('info', '')
        print(f'parse API error "{strerrorcode}" for {page_title} ({strlanguage}): {strerrorinfo}')
        intsuccess = False
        data = None
        if strerrorcode != 'maxlag':
            break  # missingtitle, invalidtitle, ... -- retrying will not help
        dblretryafter = 5.0
        try:
            dblretryafter = float(response.headers.get('Retry-After', dblretryafter))
        except (TypeError, ValueError):
            pass
        time.sleep(dblretryafter)
    payload["http_status"] = inthttpstatus
    payload["success"] = intsuccess

    soup = None
    if intsuccess and data is not None:
        wikipedia_page_content = data.get('parse', {}).get('text')
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
                    strimagetable, strimagecolumn, cursor2, lngencount, lngfrcount,
                    blnwritecounters=True):
    """Persist one (entity, language) payload produced by ``f_fetchlangpayload``.

    Runs on the main thread only (single shared PyMySQL connection). Mirrors the
    original per-row database writes; returns the updated (lngencount, lngfrcount)
    English/French page counters.

    ``blnwritecounters`` guards the per-content ``...englishcount`` /
    ``...frenchcount`` server-variable writes. The full crawler leaves it ``True``;
    the single-Qid entry point passes ``False`` so a parallel container never
    clobbers the main crawler's monitoring counters.
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
            # WIKIPEDIA-CRAWLER-019: take the first image that can plausibly BE the
            # subject, not merely the first image on the page. What sits at the top
            # of an article is often template decoration, which is exactly how edit
            # pencils and maintenance banners became stored "illustrations". SVG is
            # excluded here (but not on the summary path): this deep in an article a
            # vector file is decoration far more often than it is the subject.
            # Storing nothing is the honest outcome when nothing qualifies.
            strmainimageurl = next(
                (img.get("image_url") or "" for img in arrpageimages
                 if wimg.is_acceptable_main_image_url(img.get("image_url") or "", allow_svg=False)),
                "",
            )
            if strmainimageurl:
                print("Main image fallback (first usable page image):", strmainimageurl)
                # A fallback must never overwrite a value already in place. This
                # function runs once PER language, in ("en", "fr") order, while the
                # main-image column is shared across languages. Collection 4845 is the
                # case that exposed it: the English page yields a real lead image (the
                # Man-with-No-Name blu-ray cover), then the French page has no lead
                # image, falls back to the first picture on the article, and overwrote
                # the cover with Apollo_11_Crew.jpg, a decade-portal icon that sits at
                # the top of frwiki articles. A guess must not displace a real lead
                # image, so the write is conditional on the column being empty.
                #
                # Deliberately a plain UPDATE and NOT f_sqlupdatearray: that helper runs
                # `SELECT ... WHERE <condition>` and INSERTs when the condition matches
                # nothing, so folding the emptiness test into its condition would create
                # a duplicate row on every entity that already has an image.
                try:
                    cursor2.execute(
                        f"UPDATE {strimagetable} "
                        f"SET {strimagecolumn} = %s, TIM_UPDATED = %s "
                        f"WHERE ID_WIKIDATA = %s "
                        f"  AND ({strimagecolumn} IS NULL OR {strimagecolumn} = '')",
                        (strmainimageurl,
                         datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"),
                         wikidata_id),
                    )
                    cp.connectioncp.commit()
                    if cursor2.rowcount == 0:
                        print(f"  (kept the existing main image for {wikidata_id}: "
                              f"a fallback never overwrites one)")
                except Exception as err:
                    print(f"Main image fallback write error for {wikidata_id} "
                          f"({strlanguage}): {err}")
            else:
                print(f"No usable main image for {wikidata_id} ({strlanguage}): "
                      f"{len(arrpageimages)} page image(s), none qualified. "
                      f"Leaving the column untouched.")
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
            if blnwritecounters:
                cp.f_setservervariable("strwikipediacrawler"+strcontent+"englishcount",str(lngencount),"Count of Wikipedia English pages retrieved for "+strcontent,0)
        if strlanguage == "fr":
            lngfrcount += 1
            if blnwritecounters:
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
