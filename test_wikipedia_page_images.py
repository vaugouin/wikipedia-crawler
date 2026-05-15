"""Test script: crawl every Wikipedia image for a single Wikidata ID and write
the results to ``T_WC_WIKIPEDIA_PAGE_LANG`` and ``T_WC_WIKIPEDIA_PAGE_LANG_IMAGE``.

Mirrors the per-row logic of ``wikipedia_crawler.py`` (steps 4-6 of the data flow)
but for one entity at a time, so the new Action-API-based image enumeration can
be validated end-to-end without launching a full crawl.

Usage:
    python test_wikipedia_page_images.py                  # defaults to Q24815
    python test_wikipedia_page_images.py Q3481393
    python test_wikipedia_page_images.py Q24815 --item-type person --lang en
"""

import argparse
import urllib.parse
from datetime import datetime

import citizenphil as cp
import wikipedia_images as wimg
from wikipedia_crawler_helpers import get_linked_pages


def crawl_one(wikidata_id: str, item_type: str, languages: tuple[str, ...]) -> None:
    for lang in languages:
        print(f"\n=== {wikidata_id} / {lang} ===")
        site_key = f"{lang}wiki"

        page_content = get_linked_pages(wikidata_id, "sitelinks", lang)
        if not page_content or "entities" not in page_content:
            print(f"  no Wikidata response")
            continue
        entity = page_content["entities"].get(wikidata_id) or {}
        sitelinks = entity.get("sitelinks") or {}
        if site_key not in sitelinks:
            print(f"  no {site_key} sitelink (available: {sorted(sitelinks.keys())})")
            continue

        page_title = sitelinks[site_key].get("title") or ""
        if not page_title:
            print(f"  empty page title for {site_key}")
            continue

        page_url = (
            f"https://{lang}.wikipedia.org/wiki/"
            f"{urllib.parse.quote(page_title.replace(' ', '_'))}"
        )
        print(f"  title: {page_title}")
        print(f"  url:   {page_url}")

        meta = {
            "ID_WIKIDATA": wikidata_id,
            "LANG": lang,
            "ITEM_TYPE": item_type,
            "WIKIPEDIA_SITE_KEY": site_key,
            "WIKIPEDIA_PAGE_TITLE": page_title,
            "WIKIPEDIA_PAGE_URL": page_url,
            "PAGE_EXISTS": 1,
            "LAST_CRAWLED_AT": datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"),
            "LAST_SUCCESS_AT": datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"),
            "HTTP_STATUS": 200,
        }
        cp.f_sqlupdatearray(
            "T_WC_WIKIPEDIA_PAGE_LANG",
            meta,
            f"ID_WIKIDATA = '{wikidata_id}' AND LANG = '{lang}'",
            1,
        )

        try:
            main_image_url = wimg.get_wikipedia_main_image_url(page_title, lang)
            print(f"  main image: {main_image_url}")
        except Exception as err:
            main_image_url = ""
            print(f"  no main image ({err})")

        try:
            page_images = wimg.get_wikipedia_page_images(page_title, lang)
        except Exception as err:
            print(f"  get_wikipedia_page_images failed: {err}")
            continue

        print(f"  page images: {len(page_images)}")
        for img in page_images:
            display_order = img.get("display_order")
            row = {
                "ID_WIKIDATA": wikidata_id,
                "LANG": lang,
                "ITEM_TYPE": item_type,
                "DISPLAY_ORDER": display_order,
                "IMAGE_URL": img.get("image_url"),
                "IMAGE_URL_NORMALIZED": img.get("image_url_normalized"),
                "THUMBNAIL_URL": img.get("thumbnail_url"),
                "MEDIA_TYPE": img.get("media_type"),
                "FILE_NAME": img.get("file_name"),
                "COMMONS_TITLE": img.get("commons_title"),
                "CAPTION": img.get("caption"),
                "IS_MAIN_IMAGE": 1 if img.get("image_url") == main_image_url else 0,
            }
            cp.f_sqlupdatearray(
                "T_WC_WIKIPEDIA_PAGE_LANG_IMAGE",
                row,
                f"ID_WIKIDATA = '{wikidata_id}' AND LANG = '{lang}' "
                f"AND DISPLAY_ORDER = {display_order}",
                1,
            )
            cap_preview = (img.get("caption") or "")[:60]
            print(
                f"    {display_order:>3}. {img.get('file_name','')[:40]:<40} "
                f"main={'Y' if row['IS_MAIN_IMAGE'] else ' '} "
                f"caption={cap_preview!r}"
            )

        cursor = cp.f_getconnection().cursor()
        if page_images:
            cursor.execute(
                "DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE "
                f"WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{lang}' "
                f"AND DISPLAY_ORDER > {len(page_images)}"
            )
        else:
            cursor.execute(
                "DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE "
                f"WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{lang}'"
            )
        deleted = cursor.rowcount
        cp.connectioncp.commit()
        if deleted:
            print(f"  pruned {deleted} stale image row(s)")


def summarize(wikidata_id: str) -> None:
    cursor = cp.f_getconnection().cursor()
    cursor.execute(
        "SELECT LANG, COUNT(*) AS n, SUM(IS_MAIN_IMAGE) AS mains "
        "FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE "
        f"WHERE ID_WIKIDATA = '{wikidata_id}' GROUP BY LANG"
    )
    rows = cursor.fetchall()
    print(f"\n--- T_WC_WIKIPEDIA_PAGE_LANG_IMAGE summary for {wikidata_id} ---")
    if not rows:
        print("  (no rows)")
        return
    for r in rows:
        # f_getconnection() returns a DictCursor-enabled connection in citizenphil,
        # but the bare connectioncp cursor may yield tuples. Handle both.
        if isinstance(r, dict):
            print(f"  lang={r['LANG']}  rows={r['n']}  is_main_image={r['mains']}")
        else:
            print(f"  lang={r[0]}  rows={r[1]}  is_main_image={r[2]}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Crawl every Wikipedia image for one Wikidata ID into the target tables."
    )
    ap.add_argument(
        "wikidata_id",
        nargs="?",
        default="Q24815",
        help="Wikidata Q-id (default: Q24815)",
    )
    ap.add_argument(
        "--item-type",
        default="item",
        help="ITEM_TYPE value stored in the target tables (default: item)",
    )
    ap.add_argument(
        "--lang",
        action="append",
        choices=["en", "fr"],
        help="Language to crawl; pass twice for both. Default: en and fr",
    )
    args = ap.parse_args()

    languages = tuple(args.lang) if args.lang else ("en", "fr")
    crawl_one(args.wikidata_id, item_type=args.item_type, languages=languages)
    summarize(args.wikidata_id)


if __name__ == "__main__":
    main()
