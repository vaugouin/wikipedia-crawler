"""
Migration: add a UNIQUE key on (ID_WIKIDATA, LANG, DISPLAY_ORDER) to the two
high-volume Wikipedia tables so the crawler can switch from delete-then-insert
back to a TRUE in-place upsert (which preserves DAT_CREAT).

    T_WC_WIKIPEDIA_PAGE_LANG_SECTION
    T_WC_WIKIPEDIA_PAGE_LANG_IMAGE

Why a migration is needed
-------------------------
Both tables currently have only PRIMARY KEY (ID_ROW). `INSERT ... ON DUPLICATE
KEY UPDATE` (f_sqlbulkupsert) only updates in place when a UNIQUE/PRIMARY key
covers the conflict columns; without it, the crawler must DELETE the set first
and re-insert -- which resets DAT_CREAT every crawl. Adding the unique key lets
us keep DAT_CREAT.

Why it must check for duplicates first
--------------------------------------
`ALTER TABLE ... ADD UNIQUE KEY` FAILS if duplicate key tuples already exist.
This script reports duplicates, and (with --apply) removes the extras -- keeping
the freshest row per key (greatest ID_ROW, i.e. the most recently inserted row)
-- before adding the index.

NULL handling
-------------
A UNIQUE index treats NULLs as distinct (multiple NULLs are allowed), so rows
with NULL LANG or NULL DISPLAY_ORDER never conflict and are intentionally left
untouched. The live crawler always writes non-null LANG and DISPLAY_ORDER, so
new rows upsert correctly.

Safety / operations
--------------------
- DRY RUN by default: only reports. Pass --apply to actually dedupe + add index.
- Idempotent: re-running after success is a no-op (index already present; no
  duplicates left).
- Deletes are batched and committed in chunks (no single giant transaction).
- HEAVY on large tables: the duplicate scan and the ALTER each rewrite/scan the
  table. Run in a low-traffic window. For an online change on a live primary,
  prefer pt-online-schema-change / gh-ost to add the index instead of the
  blocking ALTER this script issues (see --skip-alter).

Usage
-----
    python migrations/add_unique_section_image_keys.py            # dry run
    python migrations/add_unique_section_image_keys.py --apply    # dedupe + ALTER
    python migrations/add_unique_section_image_keys.py --apply --skip-alter
                                                                  # dedupe only
"""
import argparse
import os
import sys
import time

# citizenphil.py lives in the repo root (one level up); make it importable no
# matter where this script is launched from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import citizenphil as cp

INDEX_NAME = "UQ_ID_WIKIDATA_LANG_DISPLAY_ORDER"
KEY_COLUMNS = ["ID_WIKIDATA", "LANG", "DISPLAY_ORDER"]
TABLES = [
    "T_WC_WIKIPEDIA_PAGE_LANG_SECTION",
    "T_WC_WIKIPEDIA_PAGE_LANG_IMAGE",
]
# Rows whose LANG or DISPLAY_ORDER is NULL are excluded from dedup: a UNIQUE
# index allows repeated NULLs, so they neither block the ALTER nor need removing.
NON_NULL_FILTER = "ID_WIKIDATA IS NOT NULL AND LANG IS NOT NULL AND DISPLAY_ORDER IS NOT NULL"
DELETE_CHUNK = 5000


def f_index_exists(conn, strtable):
    """Return True if a UNIQUE index named INDEX_NAME already exists on strtable."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) AS n
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND INDEX_NAME = %s
        """,
        (strtable, INDEX_NAME),
    )
    row = cursor.fetchone()
    return bool(row) and (row["n"] if isinstance(row, dict) else row[0]) > 0


def f_count_duplicates(conn, strtable):
    """Return (duplicate_group_count, extra_row_count) for the key tuple.

    extra_row_count is how many rows would be deleted (each group keeps one).
    """
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT COUNT(*) AS groups, COALESCE(SUM(c - 1), 0) AS extras
        FROM (
            SELECT COUNT(*) AS c
            FROM {strtable}
            WHERE {NON_NULL_FILTER}
            GROUP BY ID_WIKIDATA, LANG, DISPLAY_ORDER
            HAVING COUNT(*) > 1
        ) d
        """
    )
    row = cursor.fetchone()
    if not row:
        return 0, 0
    groups = row["groups"] if isinstance(row, dict) else row[0]
    extras = row["extras"] if isinstance(row, dict) else row[1]
    return int(groups or 0), int(extras or 0)


def f_collect_doomed_ids(conn, strtable):
    """Return the list of ID_ROW values to delete (all but the freshest per key).

    "Freshest" = greatest ID_ROW, i.e. the most recently inserted row. Given the
    crawler's replace-set history (rows are rewritten each crawl), the highest
    ID_ROW in a duplicate group holds the latest content. Only duplicate groups
    (COUNT > 1) with non-null key columns are considered.
    """
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT t.ID_ROW AS doomed
        FROM {strtable} t
        JOIN (
            SELECT ID_WIKIDATA, LANG, DISPLAY_ORDER, MAX(ID_ROW) AS max_id
            FROM {strtable}
            WHERE {NON_NULL_FILTER}
            GROUP BY ID_WIKIDATA, LANG, DISPLAY_ORDER
            HAVING COUNT(*) > 1
        ) keep
          ON t.ID_WIKIDATA = keep.ID_WIKIDATA
         AND t.LANG = keep.LANG
         AND t.DISPLAY_ORDER = keep.DISPLAY_ORDER
         AND t.ID_ROW <> keep.max_id
        """
    )
    return [r["doomed"] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]


def f_delete_ids(conn, strtable, arrids):
    """Delete the given ID_ROW values in committed chunks. Returns rows deleted."""
    lngdeleted = 0
    for lngstart in range(0, len(arrids), DELETE_CHUNK):
        chunk = arrids[lngstart:lngstart + DELETE_CHUNK]
        placeholders = ", ".join(["%s"] * len(chunk))
        cursor = conn.cursor()
        cursor.execute(
            f"DELETE FROM {strtable} WHERE ID_ROW IN ({placeholders})",
            chunk,
        )
        conn.commit()
        lngdeleted += cursor.rowcount
        print(f"    deleted {lngdeleted}/{len(arrids)} extra rows", flush=True)
    return lngdeleted


def f_add_unique_index(conn, strtable):
    """Add the composite UNIQUE index (blocking ALTER)."""
    cursor = conn.cursor()
    strcols = ", ".join(KEY_COLUMNS)
    print(f"    ALTER TABLE {strtable} ADD UNIQUE KEY {INDEX_NAME} ({strcols}) ...", flush=True)
    cursor.execute(
        f"ALTER TABLE {strtable} ADD UNIQUE KEY {INDEX_NAME} ({strcols})"
    )
    conn.commit()
    print(f"    index {INDEX_NAME} added on {strtable}", flush=True)


def f_process_table(conn, strtable, apply_changes, skip_alter):
    print(f"\n=== {strtable} ===", flush=True)

    if f_index_exists(conn, strtable):
        print(f"  index {INDEX_NAME} already present -> nothing to do", flush=True)
        return

    print("  scanning for duplicate (ID_WIKIDATA, LANG, DISPLAY_ORDER) tuples ...", flush=True)
    groups, extras = f_count_duplicates(conn, strtable)
    print(f"  duplicate groups: {groups}   extra rows to remove: {extras}", flush=True)

    if not apply_changes:
        print("  DRY RUN -> no changes made. Re-run with --apply to dedupe + add index.", flush=True)
        return

    if extras > 0:
        print("  collecting ID_ROW values to delete (keeping freshest per key) ...", flush=True)
        arrids = f_collect_doomed_ids(conn, strtable)
        print(f"  {len(arrids)} rows flagged for deletion", flush=True)
        if len(arrids) != extras:
            print(
                f"  WARNING: flagged ({len(arrids)}) != counted extras ({extras}); "
                "aborting this table to stay safe.",
                flush=True,
            )
            return
        f_delete_ids(conn, strtable, arrids)
        # Re-verify there are no duplicates left before the ALTER.
        groups2, extras2 = f_count_duplicates(conn, strtable)
        if extras2 > 0:
            print(
                f"  ERROR: {extras2} duplicates remain after dedupe; skipping ALTER.",
                flush=True,
            )
            return

    if skip_alter:
        print("  --skip-alter set -> dedupe done, add the index yourself "
              "(e.g. via pt-online-schema-change).", flush=True)
        return

    f_add_unique_index(conn, strtable)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="actually dedupe and add the index (default: dry run)")
    parser.add_argument("--skip-alter", action="store_true",
                        help="dedupe only; do not run the blocking ALTER")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"Migration add_unique_section_image_keys -- mode: {mode}", flush=True)
    tstart = time.time()

    conn = cp.f_getconnection()
    for strtable in TABLES:
        try:
            f_process_table(conn, strtable, args.apply, args.skip_alter)
        except Exception as err:  # noqa: BLE001 - surface, roll back, continue
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"  FAILED on {strtable}: {err}", flush=True)

    print(f"\nDone in {time.time() - tstart:.1f}s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
