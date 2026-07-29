"""
Migration: clear stored "illustrations" that are actually MediaWiki UI chrome.

    T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH
    T_WC_WIKIDATA_MOVIE_V1.WIKIPEDIA_POSTER_PATH
    T_WC_WIKIDATA_SERIE_V1.WIKIPEDIA_POSTER_PATH
    T_WC_WIKIDATA_PERSON_V1.WIKIPEDIA_PROFILE_PATH
    T_WC_WIKIDATA_CHARACTER_V1.WIKIPEDIA_PROFILE_PATH
    T_WC_T2S_TECHNICAL.WIKIPEDIA_IMAGE_PATH
    T_WC_WIKIPEDIA_PAGE_LANG_IMAGE.IMAGE_URL   (whole rows, they are gallery noise)

Why this migration is needed (WIKIPEDIA-CRAWLER-019)
----------------------------------------------------
`wikipedia_images._is_ui_chrome_file` filtered nothing for a long time: every
pattern is written in underscore form (`^OOjs_UI_icon_`) but the MediaWiki Action
API returns titles in display form, with spaces ("File:OOjs UI icon
edit-ltr-progressive.svg"). The filter therefore never matched on that path, and
the crawler's "first image on the page" fallback happily stored edit pencils,
maintenance banners and project logos as if they were pictures of the subject.

Audit of 2026-07-27 (46 entities sampled through the API): 10 carried UI chrome
in the served column, 3 more in the V2 image table.

The code fix stops NEW bad values and repairs an entity the next time it is
crawled, but it cannot repair an entity whose page has no usable image at all
(the fallback then writes nothing and the old value survives). This migration
removes those leftovers.

Ordering note
-------------
Run this AFTER deploying the code fix, otherwise a crawler still running the old
code will write the chrome values straight back.

Usage
-----
    python migrations/clear_ui_chrome_images.py                      # dry run
    python migrations/clear_ui_chrome_images.py --top 400            # inspect the long tail
    python migrations/clear_ui_chrome_images.py --dump before.csv --apply

Read the per-filename counts before --apply. Real chrome repeats thousands of
times; a wrongly-matched real image appears once or twice, down in the tail that
the default --top 12 hides. `--top 400` is the check that would have caught the
Crystal false positives on their own.

Soft-delete is NOT an option on the gallery table, and this is counter-intuitive
enough to write down: `DELETED` is insert-only in citizenphil.f_sqlbulkupsert
(see `insertonlystd`), so a row marked DELETED=1 would keep that flag when a
later crawl writes a legitimate image at the same (ID_WIKIDATA, LANG,
DISPLAY_ORDER). The good image would stay invisible for ever. A hard DELETE is
re-inserted cleanly with DELETED=0 on the next crawl, so it is the safer of the
two despite looking more brutal. Use --dump to keep a trace.
"""

import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import citizenphil as cp  # noqa: E402
import wikipedia_images as wimg  # noqa: E402

# (table, id column, image column). The id column is only used to report examples.
MAIN_IMAGE_TARGETS = [
    ("T_WC_WIKIDATA_ITEM_V1", "ID_WIKIDATA", "WIKIPEDIA_IMAGE_PATH"),
    ("T_WC_WIKIDATA_MOVIE_V1", "ID_WIKIDATA", "WIKIPEDIA_POSTER_PATH"),
    ("T_WC_WIKIDATA_SERIE_V1", "ID_WIKIDATA", "WIKIPEDIA_POSTER_PATH"),
    ("T_WC_WIKIDATA_PERSON_V1", "ID_WIKIDATA", "WIKIPEDIA_PROFILE_PATH"),
    ("T_WC_WIKIDATA_CHARACTER_V1", "ID_WIKIDATA", "WIKIPEDIA_PROFILE_PATH"),
    ("T_WC_T2S_TECHNICAL", "ID_TECHNICAL", "WIKIPEDIA_IMAGE_PATH"),
]

GALLERY_TABLE = ("T_WC_WIKIPEDIA_PAGE_LANG_IMAGE", "ID_ROW", "IMAGE_URL")


def _scan_column(cursor, strtable, stridcol, strimagecol, lnglimit):
    """Return (total_non_empty, [(id, url), ...]) for rows whose image is UI chrome.

    The chrome test is a Python regex list, not SQL, so the read is deliberately
    dumb (pull candidates, filter in Python). Keeps one source of truth for what
    counts as chrome: `wikipedia_images._UI_CHROME_PATTERNS`.
    """
    strsql = (
        f"SELECT {stridcol} AS ROW_ID, {strimagecol} AS IMAGE_URL FROM {strtable} "
        f"WHERE {strimagecol} IS NOT NULL AND {strimagecol} <> ''"
    )
    if lnglimit:
        strsql += f" LIMIT {int(lnglimit)}"
    cursor.execute(strsql)
    arrrows = cursor.fetchall()
    arrchrome = [(r["ROW_ID"], r["IMAGE_URL"]) for r in arrrows
                 if wimg.is_ui_chrome_url(r["IMAGE_URL"])]
    return len(arrrows), arrchrome


def _print_breakdown(arrchrome, lngtop=12):
    """Print the matched filenames by descending count, not three arbitrary rows.

    This exists because the first production dry run reported three examples, two
    of which were false positives (portraits of Crystal Allen and Crystal Pite,
    caught by a pattern meant for the Crystal icon set). A count per distinct
    filename makes a bad pattern obvious: real chrome repeats thousands of times,
    a wrongly-matched person's photo appears once. Read this list before --apply.
    """
    arrcounts = Counter(url.rsplit("/", 1)[-1] for _rowid, url in arrchrome)
    for strname, lngcount in arrcounts.most_common(lngtop):
        strflag = "  <-- appears once, check it is really chrome" if lngcount == 1 else ""
        print(f"    {lngcount:>8}  {strname}{strflag}")
    lngrest = len(arrcounts) - min(lngtop, len(arrcounts))
    if lngrest > 0:
        print(f"    {'':>8}  ... and {lngrest} other distinct filename(s)")


def _dump_rows(filedump, strtable, arrchrome):
    """Append the matched rows to the CSV trace, when --dump was given."""
    if filedump is None:
        return
    for rowid, url in arrchrome:
        filedump.write('%s,%s,"%s"\n' % (strtable, rowid, url))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="actually clear the values (default: report only)")
    parser.add_argument("--limit", type=int, default=0,
                        help="scan at most N rows per table (0 = all)")
    parser.add_argument("--top", type=int, default=12,
                        help="how many distinct filenames to list per table "
                             "(use a large value to inspect the long tail, where a "
                             "wrongly-matched real image would hide)")
    parser.add_argument("--dump", metavar="FILE",
                        help="write every matched row to a CSV (table,id,url) before "
                             "touching anything. Strongly advised with --apply: the "
                             "clearing is not reversible and the rows number in the millions")
    args = parser.parse_args()

    filedump = None
    if args.dump:
        filedump = open(args.dump, "w", encoding="utf-8", newline="")
        filedump.write("table,id,url\n")

    connection = cp.f_getconnection()
    lngtotalchrome = 0

    with connection.cursor() as cursor:
        for strtable, stridcol, strimagecol in MAIN_IMAGE_TARGETS:
            try:
                lngscanned, arrchrome = _scan_column(cursor, strtable, stridcol, strimagecol, args.limit)
            except Exception as err:
                print(f"{strtable}.{strimagecol}: skipped ({err})")
                continue
            lngtotalchrome += len(arrchrome)
            strpct = f"{100 * len(arrchrome) / lngscanned:.1f}%" if lngscanned else "n/a"
            print(f"{strtable}.{strimagecol}: {len(arrchrome)} chrome / {lngscanned} non-empty ({strpct})")
            _print_breakdown(arrchrome, args.top)
            _dump_rows(filedump, strtable, arrchrome)
            if args.apply and arrchrome:
                for rowid, _url in arrchrome:
                    cursor.execute(
                        f"UPDATE {strtable} SET {strimagecol} = '' WHERE {stridcol} = %s",
                        (rowid,),
                    )
                connection.commit()
                print(f"    -> cleared {len(arrchrome)} value(s)")

        # Gallery rows: the whole row is noise, not just its URL, so delete it.
        strtable, stridcol, strimagecol = GALLERY_TABLE
        try:
            lngscanned, arrchrome = _scan_column(cursor, strtable, stridcol, strimagecol, args.limit)
            lngtotalchrome += len(arrchrome)
            print(f"{strtable}.{strimagecol}: {len(arrchrome)} chrome / {lngscanned} non-empty")
            _print_breakdown(arrchrome, args.top)
            _dump_rows(filedump, strtable, arrchrome)
            if args.apply and arrchrome:
                for rowid, _url in arrchrome:
                    cursor.execute(f"DELETE FROM {strtable} WHERE {stridcol} = %s", (rowid,))
                connection.commit()
                print(f"    -> deleted {len(arrchrome)} row(s)")
        except Exception as err:
            print(f"{strtable}: skipped ({err})")

    print(f"\nTotal UI-chrome images found: {lngtotalchrome}")
    if not args.apply:
        print("Dry run. Re-run with --apply to clear them.")
    else:
        print("Done. Downstream T_WC_T2S_* copies refresh on the next "
              "tmdb-movie-preprocess run (TMDB-MOVIE-PREPROCESS-035).")


if __name__ == "__main__":
    main()
