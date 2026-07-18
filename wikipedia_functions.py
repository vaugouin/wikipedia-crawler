"""Single-Qid Wikipedia refresh entry point.

Companion to the full-run orchestrator ([wikipedia_crawler.py](wikipedia_crawler.py)).
Where that script sweeps every entity family in order and checkpoints its progress
in server variables, this module exposes ONE function that refreshes exactly one
Wikidata entity — the Wikipedia equivalent of the ``tmdb_functions`` one-shot
helpers (e.g. ``tf.f_tmdbmovietosqleverything(13860)``).

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

Parallel-safe by construction: it reuses the exact same per-entity fetch/persist
code as the main crawler (``wikipedia_page_writer``) but writes **no** resume-state
or monitoring server variables, so it can never corrupt the main container's
checkpoints. All page/section/image writes are keyed upserts on
``(ID_WIKIDATA, LANG, DISPLAY_ORDER)``, so refreshing a Qid the main crawler is not
currently touching is idempotent.
"""

import argparse

import citizenphil as cp
from wikipedia_crawler_helpers import get_linked_pages_batch
from wikipedia_http import get_session
from wikipedia_page_writer import (
    CONTENT_CONFIG,
    f_fetchlangpayload,
    f_writelangtodb,
)

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


def main():
    ap = argparse.ArgumentParser(
        description="Fully refresh the Wikipedia data for a single Wikidata Q-id "
                    "(parallel-safe with a running wikipedia-crawler container)."
    )
    ap.add_argument("wikidata_id", help="Wikidata Q-id, e.g. Q24815")
    ap.add_argument(
        "--item-type", "--content", dest="content", default="item",
        choices=sorted(CONTENT_CONFIG),
        help="Entity family / ITEM_TYPE (default: item)",
    )
    ap.add_argument(
        "--lang", action="append", choices=["en", "fr"],
        help="Language to refresh; pass twice for both. Default: en and fr",
    )
    args = ap.parse_args()
    arrlanguages = tuple(args.lang) if args.lang else ("en", "fr")
    f_wikipediaqidtosqleverything(args.wikidata_id, strcontent=args.content, arrlanguages=arrlanguages)


if __name__ == "__main__":
    main()
