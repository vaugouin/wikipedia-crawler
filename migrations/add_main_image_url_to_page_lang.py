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

Usage
-----
    python migrations/add_main_image_url_to_page_lang.py             # report only
    python migrations/add_main_image_url_to_page_lang.py --apply     # add the column
    python migrations/add_main_image_url_to_page_lang.py --apply --backfill
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import citizenphil as cp  # noqa: E402

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
                f"ADD COLUMN {COLUMN} varchar(500) DEFAULT NULL AFTER WIKIPEDIA_PAGE_URL"
            )
            connection.commit()
            print(f"  -> column added")

        if not args.backfill:
            print("No backfill requested (existing rows stay NULL). "
                  "The crawler fills them on its next pass.")
            return

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

        cursor.execute(
            f"UPDATE {TABLE} p "
            f"JOIN ("
            f"  SELECT ID_WIKIDATA, LANG, MIN(IMAGE_URL) AS IMAGE_URL "
            f"  FROM {GALLERY} WHERE IS_MAIN_IMAGE = 1 AND DELETED = 0 "
            f"  GROUP BY ID_WIKIDATA, LANG"
            f") g ON g.ID_WIKIDATA = p.ID_WIKIDATA AND g.LANG = p.LANG "
            f"SET p.{COLUMN} = g.IMAGE_URL "
            f"WHERE p.{COLUMN} IS NULL"
        )
        connection.commit()
        print(f"  -> backfilled {cursor.rowcount} row(s). Rows whose page has no flagged "
              f"main image stay NULL, which is the honest state.")


if __name__ == "__main__":
    main()
