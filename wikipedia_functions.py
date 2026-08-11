"""On-demand Wikipedia refresh entry points (one Qid, or one whole family).

Companion to the full-run orchestrator ([wikipedia_crawler.py](wikipedia_crawler.py)).
Where that script sweeps every entity family in order and checkpoints its progress
in server variables, this module refreshes exactly what you ask for and checkpoints
nothing — the Wikipedia equivalent of the ``tmdb_functions`` one-shot helpers
(e.g. ``tf.f_tmdbmovietosqleverything(13860)``).

Two entry points:

- ``f_wikipediaqidtosqleverything`` — one Wikidata entity.
- ``f_wikipediacontenttosqleverything`` — every entity of one family (``list``,
  ``technical``, ...), using the family's own row-selection query from
  [wikipedia_queries.py](wikipedia_queries.py), exclusion chain included, with the
  same concurrent fetch fan-out as the crawler.

It is designed to run in a **second, throwaway container** alongside the always-on
``wikipedia-crawler`` container:

    docker run -it --rm --network="host" \\
      --env-file /home/debian/docker/wikipedia-crawler/.env \\
      -v $(pwd):/home/debian/docker/wikipedia-crawler \\
      --name wikipedia-crawler-manual wikipedia-crawler-python-app python

    >>> import citizenphil as cp
    >>> import wikipedia_functions as wf
    >>> wf.f_wikipediaqidtosqleverything("Q24815")                     # item (default)
    >>> wf.f_wikipediaqidtosqleverything("Q25188", strcontent="movie")  # a movie
    >>> wf.f_wikipediaqidtosqleverything("Q42", strcontent="person")    # a person
    >>> wf.f_wikipediacontenttosqleverything("technical")               # a whole family

Parallel-safe by construction: it reuses the exact same per-entity fetch/persist
code as the main crawler (``wikipedia_page_writer``) but writes **no** resume-state
or monitoring server variables, so it can never corrupt the main container's
checkpoints. All page/section/image writes are keyed upserts on
``(ID_WIKIDATA, LANG, DISPLAY_ORDER)``, so refreshing a Qid the main crawler is not
currently touching is idempotent.
"""

import argparse
import time
from concurrent.futures import ThreadPoolExecutor

import citizenphil as cp
from wikipedia_crawler_helpers import WikidataTransientError, get_linked_pages_batch
from wikipedia_http import get_session, get_worker_count
from wikipedia_page_writer import (
    CONTENT_CONFIG,
    f_fetchlangpayload,
    f_writelangtodb,
)
from wikipedia_queries import CONTENT_SQL_BUILDERS

STRPROPS = "sitelinks"


def _resolveentityid(strwikidataid, strcontent):
    """Return the entity's own primary key for ``strwikidataid``.

    Only the ``movie`` family actually uses this id (to write the French
    ``Fiche technique`` Format line back into ``T_WC_TMDB_MOVIE`` keyed by
    ``ID_MOVIE``). For every other family the id is informational, so a missing
    lookup falls back to the Qid itself.
    """
    config = CONTENT_CONFIG[strcontent]
    strsourcetable = config.get("sourcetable")
    stridcolumn = config.get("idcolumn")
    if not strsourcetable or not stridcolumn:
        return strwikidataid
    if stridcolumn == "ID_WIKIDATA":
        return strwikidataid
    try:
        cursor = cp.f_getconnection().cursor()
        cursor.execute(
            f"SELECT {stridcolumn} AS id FROM {strsourcetable} "
            f"WHERE ID_WIKIDATA = '{strwikidataid}' LIMIT 1"
        )
        row = cursor.fetchone()
    except Exception as err:
        print(f"Could not resolve {stridcolumn} for {strwikidataid} in {strsourcetable}: {err}")
        return strwikidataid
    if not row:
        print(f"No {strsourcetable} row for {strwikidataid}; using the Qid as id.")
        return strwikidataid
    return row["id"] if isinstance(row, dict) else row[0]


def f_wikipediaqidtosqleverything(strwikidataid, strcontent="item", arrlanguages=("en", "fr")):
    """Fully refresh the Wikipedia data for one Wikidata entity.

    Mirrors the per-row work of ``wikipedia_crawler.py`` (page metadata, every page
    image, and structured sections into the shared ``T_WC_WIKIPEDIA_PAGE_LANG*``
    tables), plus the content-specific main-image and movie-Format writebacks, for a
    single Qid. Writes no resume/monitoring server variables, so it is safe to run in
    a parallel container while the main crawler keeps going.

    Args:
        strwikidataid: The Wikidata Q-id to refresh (e.g. ``"Q24815"``).
        strcontent: Entity family, one of the keys of ``CONTENT_CONFIG`` (default
            ``"item"``). This selects the main-image destination table/column and,
            for ``"movie"``, enables the French ``Fiche technique`` extraction.
        arrlanguages: Wikipedia languages to refresh (default English + French).

    Returns:
        A summary dict: ``{"wikidata_id", "content", "id", "languages": {lang: bool}}``
        where each language flag reports whether a page was found and persisted.
    """
    if strcontent not in CONTENT_CONFIG:
        raise ValueError(
            f"Unknown content type '{strcontent}'. "
            f"Expected one of: {', '.join(sorted(CONTENT_CONFIG))}"
        )
    config = CONTENT_CONFIG[strcontent]
    intindex = config["id"]
    strimagetable = config["imagetable"]
    strimagecolumn = config["imagecolumn"]
    needs_image = (strimagetable != "" and strimagecolumn != "")

    lngid = _resolveentityid(strwikidataid, strcontent)
    print(f"Refreshing Wikipedia {strcontent} (process {intindex}) "
          f"Wikidata id {strwikidataid} (entity id {lngid})")

    # One batched wbgetentities call resolves en+fr sitelinks at once.
    batch = get_linked_pages_batch([strwikidataid], STRPROPS, "|".join(arrlanguages))
    arrentities = batch.get("entities", {}) if isinstance(batch, dict) else {}
    entity = arrentities.get(strwikidataid) or {}
    sitelinks = entity.get(STRPROPS) or {}

    session = get_session()
    cursor2 = cp.f_getconnection().cursor()
    arrresults = {}
    for strlanguage in arrlanguages:
        strkey = strlanguage + "wiki"
        page_title = (sitelinks.get(strkey) or {}).get("title")
        if not page_title:
            print(f"No {strkey} Wikipedia page for {strwikidataid} "
                  f"(available: {sorted(sitelinks.keys())})")
            arrresults[strlanguage] = False
            continue
        print(f"  {strlanguage}: {page_title}")
        payload = f_fetchlangpayload(
            session, strwikidataid, page_title, strkey, strlanguage, needs_image,
        )
        # blnwritecounters=False: never touch the main crawler's monitoring counters.
        f_writelangtodb(
            payload, lngid, strwikidataid, strlanguage, strcontent, intindex,
            strimagetable, strimagecolumn, cursor2, 0, 0, blnwritecounters=False,
        )
        arrresults[strlanguage] = bool(payload["success"] and payload["has_content"])

    print(f"Done {strwikidataid}: " + ", ".join(f"{k}={'ok' if v else 'skip'}" for k, v in arrresults.items()))
    return {
        "wikidata_id": strwikidataid,
        "content": strcontent,
        "id": lngid,
        "languages": arrresults,
    }


def _buildsourcesql(strcontent, strresumeid):
    """Return the family's rows WITHOUT the exclusion chain: the whole source table.

    The exclusion chain answers "which family OWNS this entity in a full crawl", and it
    empties most ``T_WC_T2S_*`` families: process 203 ``item`` crawls the whole of
    ``T_WC_WIKIDATA_ITEM_V1`` and runs first, so an award that is also a Wikidata item is
    crawled as an ``item`` and ``215 award`` selects nothing. That is right for a full
    crawl and wrong for "refresh the awards now", which is what this query serves.

    Built from ``CONTENT_CONFIG``'s ``sourcetable`` / ``idcolumn``. The ``^Q[0-9]+$``
    guard is applied to every family, including the ones whose own builder omits it, so a
    malformed id never reaches the Wikidata API. Returns ``None`` for a family with no
    source table (``other``, a single hard-coded Qid), leaving the caller to fall back to
    the family's own builder.
    """
    config = CONTENT_CONFIG[strcontent]
    strsourcetable = config.get("sourcetable")
    stridcolumn = config.get("idcolumn")
    if not strsourcetable or not stridcolumn:
        return None
    strsql = (
        f"SELECT DISTINCT {stridcolumn} AS id, ID_WIKIDATA FROM {strsourcetable} "
        "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
        "AND ID_WIKIDATA REGEXP '^Q[0-9]+$' "
    )
    if strresumeid != "":
        # Same quoting rule as the builders in wikipedia_queries: the Qid-keyed families
        # compare strings, the TMDB-keyed ones compare integers.
        if stridcolumn == "ID_WIKIDATA":
            strsql += f"AND {stridcolumn} >= '{strresumeid}' "
        else:
            strsql += f"AND {stridcolumn} >= {strresumeid} "
    strsql += f"ORDER BY {stridcolumn} ASC "
    return strsql


def f_wikipediacontenttosqleverything(strcontent, arrlanguages=("en", "fr"),
                                      strresumeid="", lnglimit=0, blnnoexclusions=False):
    """Refresh EVERY entity of one content family, on demand.

    Runs the family's own row-selection query from ``wikipedia_queries`` — same source
    table, same exclusion chain that keeps an entity owned by an earlier family from
    being crawled twice — and then the same per-entity pipeline as the main crawler:
    titles resolved 50 Qids per ``wbgetentities`` call, ``(entity, language)`` pages
    fetched concurrently by a thread pool, every database write performed here on the
    calling thread (PyMySQL's shared connection is not thread-safe).

    With ``blnnoexclusions=True`` the exclusion chain is dropped and the whole source
    table is crawled instead. This is how the absorbed families are refreshed on demand:
    ``215 award`` selects 0 rows in a full crawl because process 203 ``item`` already owns
    those entities, so asking for "the awards" only ever works by ignoring ownership. The
    rows are then written with this family's ``ITEM_TYPE`` (``award`` rather than
    ``item``); no consumer filters on that column, and the next full crawl of 203 sets it
    back.

    Like the single-Qid path, it writes **no** resume-state or monitoring server
    variables, so it is safe to run in a second container while the main crawler keeps
    going. The trade-off of writing no checkpoint is that resuming is manual: the last
    processed id is printed on exit (and on Ctrl-C), to be passed back as
    ``strresumeid``.

    Args:
        strcontent: Entity family, one of the keys of ``CONTENT_SQL_BUILDERS``.
        arrlanguages: Wikipedia languages to refresh (default English + French).
        strresumeid: Optional lower bound on the family's own id column (``ID_MOVIE``,
            ``ID_WIKIDATA``, ...), inclusive. Empty means start at the first row.
        lnglimit: Optional cap on the number of entities processed (0 = no cap), handy
            for a trial run before committing to the whole family.
        blnnoexclusions: When True, crawl the family's whole source table instead of the
            rows it owns. Required to refresh a family the exclusion chain has absorbed
            (``award``, ``death``, ``group``, ``collection``, ...).

    Returns:
        A summary dict: ``{"content", "selected", "processed", "en", "fr", "lastid",
        "stopped"}`` where ``en``/``fr`` count the pages actually persisted and
        ``stopped`` reports why the run ended (``"complete"``, ``"limit"``,
        ``"interrupted"``, ``"wikidata-outage"``).
    """
    if strcontent not in CONTENT_SQL_BUILDERS:
        raise ValueError(
            f"Unknown content family '{strcontent}'. "
            f"Expected one of: {', '.join(sorted(CONTENT_SQL_BUILDERS))}"
        )
    config = CONTENT_CONFIG[strcontent]
    intindex = config["id"]
    strimagetable = config["imagetable"]
    strimagecolumn = config["imagecolumn"]
    needs_image = (strimagetable != "" and strimagecolumn != "")

    strsql = ""
    strmode = "rows owned by this family (exclusion chain applied)"
    if blnnoexclusions:
        strsql = _buildsourcesql(strcontent, strresumeid)
        strmode = f"WHOLE source table {CONTENT_CONFIG[strcontent].get('sourcetable')}, ownership ignored"
        if strsql is None:
            # `other` is a single hard-coded Qid with no source table: nothing to widen.
            print(f"No source table for '{strcontent}'; --no-exclusions has no effect here.")
            strsql = ""
            strmode = "rows owned by this family (exclusion chain applied)"
    if strsql == "":
        strsql = CONTENT_SQL_BUILDERS[strcontent](strresumeid)
    print(f"Refreshing the whole Wikipedia {strcontent} family (process {intindex})")
    print(f"Selection: {strmode}")
    print(strsql)

    conn = cp.f_getconnection()
    cursor = conn.cursor()
    cursor2 = conn.cursor()
    cursor.execute(strsql)
    arrrows = list(cursor.fetchall())
    if lnglimit > 0:
        arrrows = arrrows[:lnglimit]
    lngtotal = len(arrrows)
    print(f"{lngtotal} row(s) to process for {strcontent}")

    dblstart = time.time()
    lngencount = 0
    lngfrcount = 0
    lngprocessed = 0
    strlastid = ""
    strstopped = "complete"
    session = get_session()
    intworkers = get_worker_count()
    strlanguagesparam = "|".join(arrlanguages)
    # The executor is managed by hand rather than with a `with` block: on Ctrl-C the
    # block form would still wait for every queued task (up to 100 page fetches), so
    # the run would take a long minute to die. `cancel_futures=True` drops what has
    # not started and waits only for the in-flight fetches.
    executor = ThreadPoolExecutor(max_workers=intworkers)
    try:
        for intchunkstart in range(0, lngtotal, 50):
            arrchunk = arrrows[intchunkstart:intchunkstart + 50]
            arrids = [row["ID_WIKIDATA"] for row in arrchunk]
            batch = get_linked_pages_batch(arrids, STRPROPS, strlanguagesparam)
            arrentities = batch.get("entities", {}) if isinstance(batch, dict) else {}
            # Submit one fetch task per (row, language) with a resolvable title.
            arrrowtasks = []
            for row in arrchunk:
                wikidata_id = row["ID_WIKIDATA"]
                entity = arrentities.get(wikidata_id) or {}
                sitelinks = entity.get(STRPROPS) or {}
                arrtasks = []
                for strlanguage in arrlanguages:
                    strkey = strlanguage + "wiki"
                    page_title = (sitelinks.get(strkey) or {}).get("title")
                    if not page_title:
                        arrtasks.append((strlanguage, None, None))
                        continue
                    future = executor.submit(
                        f_fetchlangpayload, session, wikidata_id,
                        page_title, strkey, strlanguage, needs_image,
                    )
                    arrtasks.append((strlanguage, page_title, future))
                arrrowtasks.append((row, arrtasks))
            # Drain in submission order; every database write stays on this thread.
            for row, arrtasks in arrrowtasks:
                lngid = row["id"]
                wikidata_id = row["ID_WIKIDATA"]
                lngprocessed += 1
                print("-" * 80)
                print(f"[{lngprocessed}/{lngtotal}] {strcontent} id {lngid} "
                      f"Wikidata id {wikidata_id}")
                for strlanguage, page_title, future in arrtasks:
                    if future is None:
                        print(f"  {strlanguage}: no Wikipedia page")
                        continue
                    print(f"  {strlanguage}: {page_title}")
                    # blnwritecounters=False: never touch the main crawler's counters.
                    lngencount, lngfrcount = f_writelangtodb(
                        future.result(), lngid, wikidata_id, strlanguage, strcontent,
                        intindex, strimagetable, strimagecolumn, cursor2,
                        lngencount, lngfrcount, blnwritecounters=False,
                    )
                strlastid = str(lngid)
    except KeyboardInterrupt:
        strstopped = "interrupted"
        print("\n⏹️  Interrupted.")
    except WikidataTransientError as err:
        # Same rule as the crawler (WIKIPEDIA-CRAWLER-017): a persistent Wikidata
        # maxlag / outage must stop the run, never be swallowed as "no sitelinks",
        # which would record empty Wikipedia content for real pages.
        strstopped = "wikidata-outage"
        print(f"\n⏸️  Wikidata transient outage, stopping: {err}")
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    if strstopped == "complete" and lnglimit > 0 and lngprocessed >= lnglimit:
        strstopped = "limit"

    lngruntime = int(time.time() - dblstart)
    print("=" * 80)
    print(f"{strcontent}: {lngprocessed}/{lngtotal} entities, "
          f"{lngencount} en + {lngfrcount} fr pages written in {lngruntime} seconds "
          f"({cp.convert_seconds_to_duration(lngruntime)}) [{strstopped}]")
    if strstopped != "complete" and strlastid != "":
        strflag = " --no-exclusions" if blnnoexclusions else ""
        print(f"Resume with: --content-all {strcontent}{strflag} --resume-from {strlastid}")
    return {
        "content": strcontent,
        "selected": lngtotal,
        "processed": lngprocessed,
        "en": lngencount,
        "fr": lngfrcount,
        "lastid": strlastid,
        "stopped": strstopped,
    }


def main():
    ap = argparse.ArgumentParser(
        description="Fully refresh the Wikipedia data for a single Wikidata Q-id, or "
                    "for a whole entity family (parallel-safe with a running "
                    "wikipedia-crawler container).",
        epilog="Examples:\n"
               "  python wikipedia_functions.py Q25188 --item-type movie\n"
               "  python wikipedia_functions.py --content-all technical\n"
               "  python wikipedia_functions.py --content-all list --limit 20\n"
               "  python wikipedia_functions.py --content-all list --resume-from Q123456\n"
               "  python wikipedia_functions.py --content-all award --no-exclusions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("wikidata_id", nargs="?", help="Wikidata Q-id, e.g. Q24815")
    ap.add_argument(
        "--content-all", dest="contentall", choices=sorted(CONTENT_SQL_BUILDERS),
        help="Refresh EVERY entity of this family instead of a single Q-id, using the "
             "family's own row-selection query (exclusion chain included)",
    )
    ap.add_argument(
        "--item-type", "--content", dest="content", default="item",
        choices=sorted(CONTENT_CONFIG),
        help="Entity family / ITEM_TYPE of the single Q-id (default: item). Ignored "
             "with --content-all",
    )
    ap.add_argument(
        "--lang", action="append", choices=["en", "fr"],
        help="Language to refresh; pass twice for both. Default: en and fr",
    )
    ap.add_argument(
        "--resume-from", dest="resumeid", default="",
        help="--content-all only: start at this id of the family's own id column "
             "(ID_MOVIE, ID_WIKIDATA, ...), inclusive",
    )
    ap.add_argument(
        "--limit", dest="limit", type=int, default=0,
        help="--content-all only: process at most N entities (0 = no limit)",
    )
    ap.add_argument(
        "--no-exclusions", dest="noexclusions", action="store_true",
        help="--content-all only: crawl the family's WHOLE source table instead of the "
             "rows it owns. Needed for the families the exclusion chain absorbs (award, "
             "death, group, collection, ...), which otherwise select 0 rows",
    )
    args = ap.parse_args()
    if bool(args.wikidata_id) == bool(args.contentall):
        ap.error("give either a Wikidata Q-id or --content-all <family>, not both/neither")
    if args.noexclusions and not args.contentall:
        ap.error("--no-exclusions only applies to --content-all")
    arrlanguages = tuple(args.lang) if args.lang else ("en", "fr")
    if args.contentall:
        f_wikipediacontenttosqleverything(
            args.contentall, arrlanguages=arrlanguages,
            strresumeid=args.resumeid, lnglimit=args.limit,
            blnnoexclusions=args.noexclusions,
        )
    else:
        f_wikipediaqidtosqleverything(args.wikidata_id, strcontent=args.content, arrlanguages=arrlanguages)


if __name__ == "__main__":
    main()
