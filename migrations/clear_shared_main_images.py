"""
Migration: clear main images that are shared by too many unrelated entities.

Why frequency and not a filename list (WIKIPEDIA-CRAWLER-019)
-------------------------------------------------------------
The removed first-page-image fallback stored "the first picture on the article"
as an entity's main image. On frwiki the top of an article belongs to the portal
banners, and some of those banners are GENUINE PHOTOGRAPHS with perfectly ordinary
names. `Apollo_11_Crew.jpg` is the icon of the "Portail:Années 1960": it appears on
4500 French articles, from "Mai 68" to "Musique soul", and it landed on 5493 rows
of our database (4552 movie posters, 925 items, 16 person profiles).

No filename filter can catch that, and enumerating the offenders one by one only
ever finds the ones already noticed. Each decade portal has its own photo.

The reliable signal is frequency, not the name. The main image of a SUBJECT is
close to unique; a portal decoration repeats thousands of times. This migration
therefore reports every image URL used as the main image of at least `--min`
distinct entities, which catches Apollo *and* the ones nobody has spotted yet.

What it will NOT catch, on purpose: a decoration used only a handful of times.
That is the accepted cost of a threshold, and the report shows counts so the
threshold can be judged rather than trusted.

Ordering note
-------------
Run AFTER deploying the code fix (the fallback is gone since WIKIPEDIA-CRAWLER-019),
otherwise a crawler still running the old code writes the values straight back.

Usage
-----
    python migrations/clear_shared_main_images.py                    # dry run, --min 25
    python migrations/clear_shared_main_images.py --min 10           # widen the net
    python migrations/clear_shared_main_images.py --dump before.csv --apply

Read the counts before --apply. A legitimate image at the top of this list would be
a real surprise, but it is exactly the kind of surprise worth catching by eye: the
column is cleared, not archived, and the crawler only refills it when the page has
a real lead image.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import citizenphil as cp  # noqa: E402

# (table, id column, image column) -- the columns a main image can land in.
MAIN_IMAGE_TARGETS = [
    ("T_WC_WIKIDATA_ITEM_V1", "ID_WIKIDATA", "WIKIPEDIA_IMAGE_PATH"),
    ("T_WC_WIKIDATA_MOVIE_V1", "ID_WIKIDATA", "WIKIPEDIA_POSTER_PATH"),
    ("T_WC_WIKIDATA_SERIE_V1", "ID_WIKIDATA", "WIKIPEDIA_POSTER_PATH"),
    ("T_WC_WIKIDATA_PERSON_V1", "ID_WIKIDATA", "WIKIPEDIA_PROFILE_PATH"),
    ("T_WC_WIKIDATA_CHARACTER_V1", "ID_WIKIDATA", "WIKIPEDIA_PROFILE_PATH"),
    ("T_WC_T2S_TECHNICAL", "ID_TECHNICAL", "WIKIPEDIA_IMAGE_PATH"),
]


# NOT a target, and this is a design point rather than an oversight.
#
# T_WC_WIKIPEDIA_PAGE_LANG_IMAGE was added here for one commit and removed after the
# first dry run: at --min 50 it wanted to delete 3729264 of the gallery's 7470521 rows,
# half the table, including Flag_of_the_United_States.svg (90452), Dorothea Lange's
# Migrant Mother (5541), a Battle of Britain photograph (3350) and the UN headquarters
# (6410).
#
# Frequency is a sound signal for a MAIN image: claiming one picture represents 4000
# different subjects is absurd, so it is decoration. The gallery is not a main-image
# table, it is the list of every image ON a page. A French flag legitimately appears on
# 62945 pages because it really is on them. There, sharing is the norm, not the anomaly.
#
# The gallery is cleaned by name instead, through clear_ui_chrome_images.py.
GALLERY_NOT_A_TARGET = "T_WC_WIKIPEDIA_PAGE_LANG_IMAGE"


def _scan(cursor, strtable, stridcol, strimagecol, lngmin):
    """Return [(url, distinct_entity_count), ...] above the threshold, most shared first.

    Counting DISTINCT ids, not rows: a table with several rows per entity would
    otherwise inflate a perfectly legitimate image into a suspect one.
    """
    cursor.execute(
        f"SELECT {strimagecol} AS IMAGE_URL, COUNT(DISTINCT {stridcol}) AS ENTITY_COUNT "
        f"FROM {strtable} "
        f"WHERE {strimagecol} IS NOT NULL AND {strimagecol} <> '' "
        f"GROUP BY {strimagecol} "
        f"HAVING ENTITY_COUNT >= %s "
        f"ORDER BY ENTITY_COUNT DESC",
        (lngmin,),
    )
    return [(r["IMAGE_URL"], r["ENTITY_COUNT"]) for r in cursor.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="actually clear the values (default: report only)")
    parser.add_argument("--min", type=int, default=25,
                        help="report an image shared by at least N distinct entities "
                             "(default 25; lower it to widen the net, but read the counts)")
    parser.add_argument("--top", type=int, default=30,
                        help="how many images to list per table (default 30)")
    parser.add_argument("--dump", metavar="FILE",
                        help="write every cleared (table, url, count) to a CSV first. "
                             "Advised with --apply: clearing is not reversible")
    args = parser.parse_args()

    filedump = None
    if args.dump:
        filedump = open(args.dump, "w", encoding="utf-8", newline="")
        filedump.write("table,column,url,entity_count\n")

    connection = cp.f_getconnection()
    lngtotal = 0

    with connection.cursor() as cursor:
        for strtable, stridcol, strimagecol in MAIN_IMAGE_TARGETS:
            try:
                arrshared = _scan(cursor, strtable, stridcol, strimagecol, args.min)
            except Exception as err:
                print(f"{strtable}.{strimagecol}: skipped ({err})")
                continue
            lngrows = sum(c for _u, c in arrshared)
            lngtotal += lngrows
            print(f"{strtable}.{strimagecol}: {len(arrshared)} image(s) shared by "
                  f"{args.min}+ entities, {lngrows} row(s) concerned")
            for url, count in arrshared[:args.top]:
                print(f"    {count:>7}  {url.rsplit('/', 1)[-1]}")
            if len(arrshared) > args.top:
                print(f"    {'':>7}  ... and {len(arrshared) - args.top} more")
            if filedump is not None:
                for url, count in arrshared:
                    filedump.write('%s,%s,"%s",%s\n' % (strtable, strimagecol, url, count))
            if args.apply and arrshared:
                for url, _count in arrshared:
                    cursor.execute(
                        f"UPDATE {strtable} SET {strimagecol} = '' WHERE {strimagecol} = %s",
                        (url,),
                    )
                connection.commit()
                print(f"    -> cleared {lngrows} value(s)")

    if filedump is not None:
        filedump.close()
        print(f"\nTrace written to {args.dump}")

    print(f"Total rows carrying an over-shared main image: {lngtotal}")
    if not args.apply:
        print("Dry run. Re-run with --apply to clear them.")
    else:
        print("Done. The crawler refills a column only when the page has a real lead "
              "image, so entities without one stay empty by design "
              "(WIKIPEDIA-CRAWLER-019 correctif B).")


if __name__ == "__main__":
    main()
