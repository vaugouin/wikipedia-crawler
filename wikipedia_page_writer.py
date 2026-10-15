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
import wikipedia_content_md as wcmd
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
    # WIKIPEDIA-CRAWLER-023. The image column is the legacy per-entity copy, like
    # technical's; the one tmdb-movie-preprocess process 71 reads is
    # T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL (per language), written for every family.
    "sourcework":        {"id": 224, "imagetable": "T_WC_T2S_SOURCE_WORK",       "imagecolumn": "WIKIPEDIA_IMAGE_PATH",   "sourcetable": "T_WC_T2S_SOURCE_WORK",    "idcolumn": "ID_SOURCE_WORK"},
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
        # True once the summary endpoint has answered, with or without an image.
        "main_image_checked": False,
        "page_images": [],
        "http_status": None,
        "success": False,
        "has_content": False,
        # (display_order, title, CONTENT, CONTENT_MD); CONTENT_MD is None on the legacy fallback.
        "sections": [],
        "links": [],
        "refs": [],
        # {title: (qid, final_title, via_redirect)} from the API, resolved here in the worker (1.1.1).
        "api_qids": {},
    }

    # Lead/main image — only for content types that store one.
    if needs_image:
        try:
            strmainimageurl = wimg.get_wikipedia_main_image_url(page_title, strlanguage)
            # WIKIPEDIA-CRAWLER-019: the summary endpoint occasionally returns a
            # maintenance banner or an edit icon as the "lead" image. Storing it
            # states something false about the subject, so drop it: no image.
            if strmainimageurl and not wimg.is_acceptable_main_image_url(strmainimageurl):
                print(f"Main image rejected (UI chrome) for {wikidata_id} ({strlanguage}): {strmainimageurl}")
                strmainimageurl = ""
            payload["main_image_url"] = strmainimageurl
            payload["main_image_checked"] = True
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
            # 1.1.0 (topic wikipedia-rich-sections): CONTENT is the plain text repaired from
            # the Markdown, CONTENT_MD the Markdown, plus the page's links and references.
            # Should the converter ever fail on a page, the page keeps the legacy plain-text
            # extraction, with no Markdown, links or references, rather than losing its sections.
            try:
                converted = wcmd.convert_page(soup, strlanguage)
                payload["sections"] = converted["sections"]
                payload["links"] = converted["links"]
                payload["refs"] = converted["refs"]
                # 1.1.1: the API lookup of the link targets runs here, in the fetch worker, for
                # every live title. The worker cannot see which titles the local title join on
                # the main thread will answer, so it asks about all of them: more API calls, but
                # in parallel, instead of serialised on the thread that does every database write.
                try:
                    payload["api_qids"] = wcmd.resolve_by_api(strlanguage, wcmd.api_titles(converted["links"]))
                except Exception as err:
                    # Never a reason to lose the Markdown: the links stay, unresolved by the API.
                    print(f"Link resolution error for {wikidata_id} ({strlanguage}): {err}")
            except Exception as err:
                print(f"CONTENT_MD conversion error for {wikidata_id} ({strlanguage}), legacy text kept: {err}")
                payload["sections"] = [
                    (i, t, c.replace("[edit]", ""), None)
                    for i, (t, c) in enumerate(extract_titles_and_text(soup=soup), start=1)
                ]

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
        # WIKIPEDIA-CRAWLER-019 (correctif B): there is deliberately NO first-page-image
        # fallback any more. Only the lead image resolved by the REST summary endpoint
        # above is ever stored as an entity's main image; when there is none, the column
        # is left exactly as it was.
        #
        # The fallback took "the first image on the article" as the subject, and on a
        # Wikipedia article the top of the page belongs to the templates, not to the
        # subject. It produced the great majority of the 1.25M UI-chrome images cleared
        # by migrations/clear_ui_chrome_images.py, Blue_pencil.svg alone accounting for
        # 33734 of the 38136 wrong movie posters.
        #
        # Filtering the chrome out was not enough, and collections 4840/4845 are why:
        # on frwiki the decade-portal banner carries Apollo_11_Crew.jpg, a genuine
        # photograph correctly named, which no filename filter can tell from a real
        # illustration. Filtering only turned visible garbage (an edit pencil) into
        # plausible garbage (astronauts on a Sergio Leone trilogy), which is worse:
        # a wrong image that looks right is read as fact.
        #
        # Consequence, accepted: entities whose Wikipedia page has no lead image now
        # keep no main image at all. An empty illustration is honest; a guess is not.
        if not strmainimageurl and strimagetable != "" and strimagecolumn != "":
            print(f"No lead image for {wikidata_id} ({strlanguage}): "
                  f"{len(arrpageimages)} page image(s) available, none used as main "
                  f"image (no fallback by design). Column left untouched.")
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
    # WIKIPEDIA-CRAWLER-020: the page's lead image now also lands HERE, next to the
    # page it belongs to, and not only in the entity's `T_WC_WIKIDATA_*_V1` column.
    #
    # It closes the one gap that blocked the V1 decommission (WIKIDATA-CRAWLER-015):
    # V2 carries no image column at all and should not, because a Wikipedia lead image
    # is Wikipedia data, not a Wikidata statement. This table is its natural home, it
    # already holds the title, the URL, the HTTP status and the crawl dates of that
    # very page.
    #
    # Bonus the V1 column could never offer: this row is keyed on (ID_WIKIDATA, LANG),
    # so the main image is stored PER LANGUAGE. V1 had a single column shared across
    # languages, which is precisely how the French pass overwrote the English lead
    # image of collection 4845 with a portal banner.
    #
    # Written whenever the summary endpoint ANSWERED, image or not: an empty string
    # means "Wikipedia says this page has no lead image", NULL means "never checked".
    # A failed request leaves the column untouched, so a network error never costs
    # an image found earlier. Blanking on a real answer is what lets a re-crawl undo
    # the values the removed fallback wrote: 5107 French pages still carried the
    # "Portail:Années 1960" photo (Apollo_11_Crew.jpg) in 2026-10, Dollars Trilogy
    # and Once Upon a Time trilogy among them, because the old rule kept every
    # stored value forever (WIKIPEDIA-CRAWLER-019).
    #
    # The legacy per-entity column above keeps the "never blank" rule on purpose: it
    # is shared by both languages, so blanking it on the French pass would erase the
    # image the English pass had just found.
    if payload.get("main_image_checked"):
        arrcouples["MAIN_IMAGE_URL"] = strmainimageurl
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
        for lngdisplayorder, strsectiontitle, strsectioncontent, strsectionmd in arrcontent:
            print(f"{lngdisplayorder}. Title: {strsectiontitle}")
            if len(strsectiontitle) > 300:
                strsectiontitle = strsectiontitle[:300]
            arrsectionrows.append({
                "ID_WIKIDATA": wikidata_id,
                "LANG": strlanguage,
                "ITEM_TYPE": strcontent,
                "DISPLAY_ORDER": lngdisplayorder,
                "TITLE": strsectiontitle,
                "CONTENT": strsectioncontent,
                "CONTENT_MD": strsectionmd,
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
            # 100 rows per statement: with CONTENT_MD beside CONTENT a row is about 2.4 times
            # what it was, and a statement must stay under max_allowed_packet.
            cp.f_sqlbulkupsert("T_WC_WIKIPEDIA_PAGE_LANG_SECTION", arrsectionrows, ["ID_WIKIDATA", "LANG", "DISPLAY_ORDER"], 1, intchunksize=100)
            cursor2.execute(
                f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}' AND DISPLAY_ORDER > {lngdisplayorder}"
            )
        else:
            cursor2.execute(
                f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}'"
            )
        cp.connectioncp.commit()

        try:
            f_writelinksandrefs(payload, wikidata_id, strlanguage, strcontent, lngdisplayorder, cursor2)
        except Exception as err:
            print(f"Links/references persistence error for {wikidata_id} ({strlanguage}): {err}")

    return lngencount, lngfrcount


def _prunecondition(lngsectioncount, arrcounts):
    """WHERE clause tail matching the rows of a page this crawl no longer produced:
    sections past the last one, and in each section the rows past its last link/reference."""
    arrconds = [f"SECTION_DISPLAY_ORDER > {int(lngsectioncount)}"]
    for lngsection in range(1, int(lngsectioncount) + 1):
        arrconds.append(f"(SECTION_DISPLAY_ORDER = {lngsection} AND DISPLAY_ORDER > {int(arrcounts.get(lngsection, 0))})")
    return "(" + " OR ".join(arrconds) + ")"


def f_buildlinkrows(arrlinks, wikidata_id, strlanguage, strcontent):
    """Rows for T_WC_WIKIPEDIA_PAGE_LANG_LINK from resolved links, plus the per-section counts."""
    arrrows = []
    arrcounts = {}
    for link in arrlinks:
        arrcounts[link["section"]] = max(arrcounts.get(link["section"], 0), link["display_order"])
        arrrows.append({
            "ID_WIKIDATA": wikidata_id,
            "LANG": strlanguage,
            "ITEM_TYPE": strcontent,
            "SECTION_DISPLAY_ORDER": link["section"],
            "DISPLAY_ORDER": link["display_order"],
            "ANCHOR": link["anchor"][:wcmd.MAX_ANCHOR],
            "TARGET_TITLE": link["target"][:wcmd.MAX_TITLE],
            "FRAGMENT": link["fragment"][:wcmd.MAX_TITLE] if link["fragment"] else None,
            "TARGET_FINAL_TITLE": link["final_title"][:wcmd.MAX_TITLE] if link["final_title"] else None,
            "VIA_REDIRECT": link["via_redirect"],
            "IS_REDLINK": link["redlink"],
            "IS_DATE": link["is_date"],
            "TARGET_ID_WIKIDATA": link["target_qid"],
            "RESOLVED_BY": link["resolved_by"],
        })
    return arrrows, arrcounts


def f_buildrefrows(arrrefs, wikidata_id, strlanguage, strcontent):
    """Rows for T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE, plus the per-section counts."""
    arrrows = []
    arrcounts = {}
    for ref in arrrefs:
        arrcounts[ref["section"]] = max(arrcounts.get(ref["section"], 0), ref["display_order"])
        arrrows.append({
            "ID_WIKIDATA": wikidata_id,
            "LANG": strlanguage,
            "ITEM_TYPE": strcontent,
            "SECTION_DISPLAY_ORDER": ref["section"],
            "DISPLAY_ORDER": ref["display_order"],
            "NOTE_KEY": ref["note"][:wcmd.MAX_NOTE_KEY],
            "LABEL": ref["label"][:wcmd.MAX_LABEL],
            "URL": ref["url"] or None,
            "TITLE": ref["title"][:wcmd.MAX_REF_TITLE] if ref["title"] else None,
            "REFERENCE_TEXT": ref["text"] or None,
        })
    return arrrows, arrcounts


def f_writelinksandrefs(payload, wikidata_id, strlanguage, strcontent, lngsectioncount, cursor2):
    """Persist the links and references of one page (1.1.0), keyed like its sections.

    Same pattern as sections and images: an in-place bulk upsert on
    (ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER), then a prune of the rows this
    crawl no longer produced. Links are resolved to a Qid first: the local title join on
    T_WC_WIKIPEDIA_PAGE_LANG, then the API answers the fetch worker already got (no network
    call here; date and red links aside).
    On the legacy fallback (no Markdown) both lists are empty, so the page's old rows go:
    they would describe sections that no longer line up.
    """
    arrlinks = payload.get("links") or []
    arrrefs = payload.get("refs") or []
    strwhere = f"ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}'"
    lnglinkkeys = ["ID_WIKIDATA", "LANG", "SECTION_DISPLAY_ORDER", "DISPLAY_ORDER"]

    if arrlinks:
        wcmd.resolve_link_qids(cp.f_getconnection().cursor(), strlanguage, arrlinks,
                               api_results=payload.get("api_qids") or {})
    arrrows, arrcounts = f_buildlinkrows(arrlinks, wikidata_id, strlanguage, strcontent)
    if arrrows:
        cp.f_sqlbulkupsert("T_WC_WIKIPEDIA_PAGE_LANG_LINK", arrrows, lnglinkkeys, 1)
        cursor2.execute(f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK WHERE {strwhere} AND {_prunecondition(lngsectioncount, arrcounts)}")
    else:
        cursor2.execute(f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK WHERE {strwhere}")

    arrrows, arrcounts = f_buildrefrows(arrrefs, wikidata_id, strlanguage, strcontent)
    if arrrows:
        cp.f_sqlbulkupsert("T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE", arrrows, lnglinkkeys, 1)
        cursor2.execute(f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE WHERE {strwhere} AND {_prunecondition(lngsectioncount, arrcounts)}")
    else:
        cursor2.execute(f"DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE WHERE {strwhere}")
    cp.connectioncp.commit()
