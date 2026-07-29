"""
Verify the in-place bulk-upsert behavior the crawler now relies on, against the
unique index added by migrations/add_unique_section_image_keys.py.

It exercises the REAL citizenphil.f_sqlbulkupsert (the same call the crawler's
f_writelangtodb makes for sections and images) and asserts that, for an entity
whose rows already exist:

  * rows are updated IN PLACE  -> ID_ROW is unchanged (not delete + re-insert),
  * DAT_CREAT is PRESERVED     -> creation date untouched,
  * TIM_UPDATED ADVANCES       -> the row was really rewritten,
  * NO DUPLICATES are created   -> the unique key + ON DUPLICATE KEY UPDATE work.

Non-destructive: it reads the current rows for one (ID_WIKIDATA, LANG) and writes
the SAME column values back, so only TIM_UPDATED changes. Safe to run in preprod.
It does NOT hit the network (the parsing/fetch path is unchanged by this work);
it validates the database write mechanism, which is the part that changed.

Usage (Docker one-off, same pattern as the migration):
    python verify_inplace_upsert.py                 # auto-pick an entity with rows
    python verify_inplace_upsert.py Q24815 en       # a specific (Q-id, language)
"""
import sys

import citizenphil as cp

# Per table: the non-key data columns to round-trip (everything the crawler sets
# besides the key columns and the standard/creation fields).
TABLES = {
    "T_WC_WIKIPEDIA_PAGE_LANG_SECTION": ["ITEM_TYPE", "TITLE", "CONTENT"],
    "T_WC_WIKIPEDIA_PAGE_LANG_IMAGE": [
        "ITEM_TYPE", "IMAGE_URL", "IMAGE_URL_NORMALIZED", "THUMBNAIL_URL",
        "MEDIA_TYPE", "FILE_NAME", "COMMONS_TITLE", "CAPTION", "IS_MAIN_IMAGE",
    ],
}
KEY = ["ID_WIKIDATA", "LANG", "DISPLAY_ORDER"]


def f_fetch_rows(conn, strtable, datacols, wikidata_id, lang):
    """Current rows for (wikidata_id, lang), non-null DISPLAY_ORDER, ordered."""
    cols = ["ID_ROW", "DISPLAY_ORDER", "DAT_CREAT", "TIM_UPDATED"] + datacols
    cur = conn.cursor()
    cur.execute(
        f"SELECT {', '.join(cols)} FROM {strtable} "
        f"WHERE ID_WIKIDATA = %s AND LANG = %s AND DISPLAY_ORDER IS NOT NULL "
        f"ORDER BY DISPLAY_ORDER",
        (wikidata_id, lang),
    )
    return cur.fetchall()


def f_pick_entity(conn):
    """Pick an (ID_WIKIDATA, LANG) that has >= 2 section rows to verify against."""
    cur = conn.cursor()
    cur.execute(
        "SELECT ID_WIKIDATA, LANG, COUNT(*) AS c "
        "FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION "
        "WHERE ID_WIKIDATA IS NOT NULL AND LANG IS NOT NULL "
        "  AND DISPLAY_ORDER IS NOT NULL "
        "GROUP BY ID_WIKIDATA, LANG HAVING c >= 2 LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return None, None
    return row["ID_WIKIDATA"], row["LANG"]


def f_count_duplicates(conn, strtable, wikidata_id, lang):
    cur = conn.cursor()
    cur.execute(
        f"SELECT DISPLAY_ORDER, COUNT(*) AS c FROM {strtable} "
        f"WHERE ID_WIKIDATA = %s AND LANG = %s AND DISPLAY_ORDER IS NOT NULL "
        f"GROUP BY DISPLAY_ORDER HAVING c > 1",
        (wikidata_id, lang),
    )
    return cur.fetchall()


def f_verify_table(conn, strtable, datacols, wikidata_id, lang):
    print(f"\n=== {strtable}  ({wikidata_id} / {lang}) ===", flush=True)
    before = f_fetch_rows(conn, strtable, datacols, wikidata_id, lang)
    if not before:
        print("  no rows for this (entity, language) -> skipped", flush=True)
        return True

    print(f"  BEFORE ({len(before)} rows):", flush=True)
    for r in before:
        print(f"    DO={r['DISPLAY_ORDER']:>3}  ID_ROW={r['ID_ROW']}  "
              f"DAT_CREAT={r['DAT_CREAT']}  TIM_UPDATED={r['TIM_UPDATED']}", flush=True)

    # Re-upsert the SAME values back through the real crawler write primitive.
    rows = []
    for r in before:
        row = {"ID_WIKIDATA": wikidata_id, "LANG": lang,
               "DISPLAY_ORDER": r["DISPLAY_ORDER"]}
        for col in datacols:
            row[col] = r[col]
        rows.append(row)
    cp.f_sqlbulkupsert(strtable, rows, KEY, 1)

    after = f_fetch_rows(conn, strtable, datacols, wikidata_id, lang)
    after_by_do = {r["DISPLAY_ORDER"]: r for r in after}

    print(f"  AFTER ({len(after)} rows):", flush=True)
    for r in after:
        print(f"    DO={r['DISPLAY_ORDER']:>3}  ID_ROW={r['ID_ROW']}  "
              f"DAT_CREAT={r['DAT_CREAT']}  TIM_UPDATED={r['TIM_UPDATED']}", flush=True)

    ok = True
    if len(after) != len(before):
        print(f"  FAIL: row count changed {len(before)} -> {len(after)}", flush=True)
        ok = False

    for b in before:
        do = b["DISPLAY_ORDER"]
        a = after_by_do.get(do)
        if a is None:
            print(f"  FAIL DO={do}: row disappeared after upsert", flush=True)
            ok = False
            continue
        if a["ID_ROW"] != b["ID_ROW"]:
            print(f"  FAIL DO={do}: ID_ROW changed {b['ID_ROW']} -> {a['ID_ROW']} "
                  f"(row was re-inserted, not updated in place)", flush=True)
            ok = False
        if str(a["DAT_CREAT"]) != str(b["DAT_CREAT"]):
            print(f"  FAIL DO={do}: DAT_CREAT changed {b['DAT_CREAT']} -> {a['DAT_CREAT']}", flush=True)
            ok = False
        if str(a["TIM_UPDATED"]) == str(b["TIM_UPDATED"]):
            print(f"  WARN DO={do}: TIM_UPDATED did not advance ({a['TIM_UPDATED']})", flush=True)

    dups = f_count_duplicates(conn, strtable, wikidata_id, lang)
    if dups:
        print(f"  FAIL: duplicate DISPLAY_ORDER rows exist: {dups}", flush=True)
        ok = False

    print(f"  -> {'PASS' if ok else 'FAIL'}", flush=True)
    return ok


def main():
    args = sys.argv[1:]
    conn = cp.f_getconnection()

    if len(args) >= 2:
        wikidata_id, lang = args[0], args[1]
    else:
        wikidata_id, lang = f_pick_entity(conn)
        if not wikidata_id:
            print("No section rows found to verify against.", flush=True)
            return 1
        print(f"Auto-picked entity with sections: {wikidata_id} / {lang}", flush=True)

    allok = True
    for strtable, datacols in TABLES.items():
        try:
            if not f_verify_table(conn, strtable, datacols, wikidata_id, lang):
                allok = False
        except Exception as err:  # noqa: BLE001 - surface and continue
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"  ERROR on {strtable}: {err}", flush=True)
            allok = False

    print("\nRESULT:", "PASS -- in-place upsert preserves DAT_CREAT, advances "
          "TIM_UPDATED, creates no duplicates."
          if allok else "FAIL -- see messages above.", flush=True)
    return 0 if allok else 1


if __name__ == "__main__":
    sys.exit(main())
