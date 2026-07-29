"""
Migration: add T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL.

Why (WIKIPEDIA-CRAWLER-020, unblocks WIKIDATA-CRAWLER-015)
----------------------------------------------------------
The Wikipedia lead image of an entity is stored today in the entity's V1 row:
`T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`, `_MOVIE_V1.WIKIPEDIA_POSTER_PATH`,
`_PERSON_V1.WIKIPEDIA_PROFILE_PATH`, ... The V2 model carries **no image column at
all**, and it should not: a Wikipedia lead image is Wikipedia data, not a Wikidata
statement. So as long as the image lives only in V1, V1 cannot be decommissioned.
That was the single blocking gap in the V1 -> V2 inventory.

This table is the natural home. It already holds, for one (entity, language) pair,
the page title, the page URL, PAGE_EXISTS, the HTTP status and the crawl dates. The
lead image of that same page belongs beside them.

What it also fixes, for free
----------------------------
V1 had ONE image column per entity while the crawler runs once per language, so the
second language silently overwrote the first. That is exactly how collection 4845
lost its English lead image (the Man-with-No-Name blu-ray cover) to a French
decade-portal banner. Keyed on (ID_WIKIDATA, LANG), this column cannot do that, and
consumers gain a localized main image they never had.

Safe to run on a live database: adding a nullable column takes a metadata-only
ALTER on MariaDB 10.3+ (ALGORITHM=INSTANT), and the crawler simply starts filling
it on the next pass. Nothing reads it yet.

Backfill
--------
Existing rows stay NULL. `--backfill` copies what can be trusted: the image already
flagged IS_MAIN_IMAGE = 1 in T_WC_WIKIPEDIA_PAGE_LANG_IMAGE for the same
(ID_WIKIDATA, LANG). That flag is set only when the gallery image equalled the
resolved lead image, so it never invents one. Rows whose page has no flagged image
stay NULL, which is the honest state.

`--backfill` is now self-repairing, in three steps:

  1. RESET  values already stored that are UI chrome are blanked back to NULL. The
            first backfill ran while the gallery still held thumbnail URLs of chrome
            (WIKIPEDIA-CRAWLER-021) and copied 8504 of them in. Since the fill only
            touches NULL rows, without this reset a re-run would leave every one of
            them in place.
  2. FILL   the backfill itself, unchanged.
  3. VERIFY the column is re-read afterwards. If chrome came back, the gallery is
            still dirty and the script says so, naming the command to run first.

Order matters: clear_ui_chrome_images.py must be re-run BEFORE this script,
otherwise step 3 will simply report that step 1 was undone by step 2.

Usage
-----
    python migrations/add_main_image_url_to_page_lang.py             # report only
    python migrations/add_main_image_url_to_page_lang.py --apply     # add the column
    python migrations/add_main_image_url_to_page_lang.py --backfill          # dry run of all 3 steps
    python migrations/add_main_image_url_to_page_lang.py --apply --backfill
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import citizenphil as cp  # noqa: E402
import wikipedia_images as wimg  # noqa: E402

TABLE = "T_WC_WIKIPEDIA_PAGE_LANG"
COLUMN = "MAIN_IMAGE_URL"
GALLERY = "T_WC_WIKIPEDIA_PAGE_LANG_IMAGE"


def _column_exists(cursor) -> bool:
    cursor.execute(
        "SELECT COUNT(*) AS N FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        (TABLE, COLUMN),
    )
    return (cursor.fetchone() or {}).get("N", 0) > 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="actually alter the table")
    parser.add_argument("--backfill", action="store_true",
                        help="also fill existing rows from the gallery's IS_MAIN_IMAGE = 1")
    args = parser.parse_args()

    connection = cp.f_getconnection()
    with connection.cursor() as cursor:
        blnexists = _column_exists(cursor)
        print(f"{TABLE}.{COLUMN}: {'already present' if blnexists else 'missing'}")

        if not blnexists:
            if not args.apply:
                print("Dry run. Re-run with --apply to add it.")
                return
            cursor.execute(
                f"ALTER TABLE {TABLE} "
                f"ADD COLUMN {COLUMN} varchar(1000) DEFAULT NULL AFTER WIKIPEDIA_PAGE_URL"
            )
            connection.commit()
            print("  -> column added")
        else:
            # First cut of this migration created the column as varchar(500), copying the
            # V1 size. That was too narrow: V1 only ever stored lead-image URLs, which
            # get_wikipedia_main_image_url strips of their query string, while the gallery
            # keeps the ?utm_source=...&utm_campaign=imageinfo tracking parameters the
            # imageinfo API appends (about 70 extra characters). The backfill reads the
            # gallery, so it hit "Data too long for column". Widen in place if needed.
            cursor.execute(
                "SELECT CHARACTER_MAXIMUM_LENGTH AS L FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s",
                (TABLE, COLUMN),
            )
            lnglen = (cursor.fetchone() or {}).get("L") or 0
            if lnglen and lnglen < 1000:
                print(f"  column is varchar({lnglen}), too narrow for gallery URLs")
                if not args.apply:
                    print("  Dry run. Re-run with --apply to widen it to varchar(1000).")
                    return
                cursor.execute(f"ALTER TABLE {TABLE} MODIFY {COLUMN} varchar(1000) DEFAULT NULL")
                connection.commit()
                print("  -> widened to varchar(1000)")

        if not args.backfill:
            print("No backfill requested (existing rows stay NULL). "
                  "The crawler fills them on its next pass.")
            return

        # ---------------------------------------------------------------------------
        # Reset first: a value this migration wrote wrongly is this migration's job to
        # undo. The first backfill ran while the gallery still held thumbnail URLs of
        # UI chrome (WIKIPEDIA-CRAWLER-021: the ^-anchored filter matched
        # "Blue_pencil.svg" but not "langfr-960px-Blue_pencil.svg.png"), and copied
        # 8504 of them in. Since the backfill only fills rows that are NULL, re-running
        # it would leave every one of those in place. So blank them back to NULL first.
        #
        # Read DISTINCT urls, not rows: 790k rows collapse to a few thousand values, and
        # the chrome test is the Python one, so there is a single source of truth for
        # what counts as chrome (wikipedia_images._UI_CHROME_PATTERNS).
        # ---------------------------------------------------------------------------
        cursor.execute(
            f"SELECT DISTINCT {COLUMN} AS U FROM {TABLE} "
            f"WHERE {COLUMN} IS NOT NULL AND {COLUMN} <> ''"
        )
        arrurls = [r["U"] for r in cursor.fetchall()]
        arrchrome = [u for u in arrurls if wimg.is_ui_chrome_url(u)]
        print(f"stored values: {len(arrurls)} distinct, of which {len(arrchrome)} are UI chrome")
        for u in arrchrome[:8]:
            print(f"    {u.rsplit('/', 1)[-1][:78]}")
        if len(arrchrome) > 8:
            print(f"    ... and {len(arrchrome) - 8} more distinct value(s)")

        if arrchrome and args.apply:
            lngreset = 0
            for u in arrchrome:
                cursor.execute(
                    f"UPDATE {TABLE} SET {COLUMN} = NULL WHERE {COLUMN} = %s", (u,)
                )
                lngreset += cursor.rowcount
            connection.commit()
            print(f"  -> reset {lngreset} row(s) to NULL so the backfill below can redo them")
        elif arrchrome:
            print("  (dry run: they would be reset to NULL before the backfill)")

        cursor.execute(
            f"SELECT COUNT(*) AS N FROM {TABLE} p "
            f"WHERE p.{COLUMN} IS NULL AND EXISTS ("
            f"  SELECT 1 FROM {GALLERY} g "
            f"  WHERE g.ID_WIKIDATA = p.ID_WIKIDATA AND g.LANG = p.LANG "
            f"    AND g.IS_MAIN_IMAGE = 1 AND g.DELETED = 0)"
        )
        lngcandidates = (cursor.fetchone() or {}).get("N", 0)
        print(f"backfill candidates (page has a gallery image flagged IS_MAIN_IMAGE): {lngcandidates}")

        if not args.apply:
            print("Dry run. Re-run with --apply --backfill to fill them.")
            return

        # SUBSTRING_INDEX(..., '?', 1) drops the imageinfo tracking parameters, exactly
        # what get_wikipedia_main_image_url does on the lead-image path. They are noise,
        # they break URL equality between the two stores, and they are what overflowed
        # the column on the first run.
        cursor.execute(
            f"UPDATE {TABLE} p "
            f"JOIN ("
            f"  SELECT ID_WIKIDATA, LANG, "
            f"         MIN(SUBSTRING_INDEX(IMAGE_URL, '?', 1)) AS IMAGE_URL "
            f"  FROM {GALLERY} WHERE IS_MAIN_IMAGE = 1 AND DELETED = 0 "
            f"  GROUP BY ID_WIKIDATA, LANG"
            f") g ON g.ID_WIKIDATA = p.ID_WIKIDATA AND g.LANG = p.LANG "
            f"SET p.{COLUMN} = g.IMAGE_URL "
            f"WHERE p.{COLUMN} IS NULL AND CHAR_LENGTH(g.IMAGE_URL) <= 1000"
        )
        connection.commit()
        print(f"  -> backfilled {cursor.rowcount} row(s). Rows whose page has no flagged "
              f"main image stay NULL, which is the honest state.")

        # Verify rather than assume: if chrome came back, the gallery is still dirty and
        # clear_ui_chrome_images.py has not been re-run since WIKIPEDIA-CRAWLER-021.
        cursor.execute(
            f"SELECT DISTINCT {COLUMN} AS U FROM {TABLE} "
            f"WHERE {COLUMN} IS NOT NULL AND {COLUMN} <> ''"
        )
        arrapres = [r["U"] for r in cursor.fetchall() if wimg.is_ui_chrome_url(r["U"])]
        if arrapres:
            print(f"\n  !! {len(arrapres)} chrome value(s) came straight back from the gallery.")
            print( "     The gallery still holds them, so run this first, then this script again:")
            print( "       python migrations/clear_ui_chrome_images.py --apply")
            for u in arrapres[:5]:
                print(f"       {u.rsplit('/', 1)[-1][:74]}")
        else:
            print("  verified: no UI chrome in the column after the backfill.")


if __name__ == "__main__":
    main()
