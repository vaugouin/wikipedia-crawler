"""Backfill Wikipedia pages from the Wikimedia Enterprise monthly snapshots (1.1.6, prototype).

WIKIPEDIA-CRAWLER-024 (Nestor t2s-backlog). The live crawl reads one page per ``action=parse``
call and is held back by Wikipedia's rate limits; the free Enterprise account gives the whole
of ``frwiki`` / ``enwiki`` namespace 0 once a month, as ``.tar.gz`` chunks of NDJSON records
(150 chunks for frwiki, 435 for enwiki, 1,500 chunk downloads a month on the free tier).

Each record carries the page's Wikidata Qid (``main_entity.identifier``), its title, its
redirects and its Parsoid HTML. This script streams a chunk (never written to disk: ~300 MB
compressed, 2.1 GB of NDJSON), keeps the records whose Qid one of the crawler's families
selects (the same ``wikipedia_queries`` builders, exclusion chains included), converts them
with the same ``wikipedia_content_md.convert_page`` and, with ``--write``, persists them with
the same ``wikipedia_page_writer.f_writelangtodb``: sections, CONTENT_MD, links, references,
the French movie "Fiche technique" format line. Images are left as they are (``keep_images``):
a record carries no image list.

Links are resolved by the local title join only (``T_WC_WIKIPEDIA_PAGE_LANG``), never by the
Wikipedia API: the rest stays unresolved for a later pass fed by the snapshot's own title to
Qid map. ``--title-map DIR`` writes that map, one ``<snapshot>-<chunk>.tsv.gz`` per chunk
(title, Qid, 1 for a redirect), from every record of the chunk, target or not.

Without ``--write`` nothing is written to the database (dry run): the script reports what a
write would do and compares the converted pages with what the live crawl stored.

Usage (on the VPS, through wikipedia-dump-ingest.sh):
    python wikipedia_dump_ingest.py --lang fr --chunks 0                 # dry run, one chunk
    python wikipedia_dump_ingest.py --lang fr --chunks 0-4 --title-map dumpdata
    python wikipedia_dump_ingest.py --lang fr --chunks 0 --write         # crawler stopped first

Credentials: WME_USERNAME / WME_PASSWORD from the environment (.env); never printed.
"""

import argparse
import contextlib
import gzip
import io
import json
import os
import re
import sys
import tarfile
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import citizenphil as cp
import wikipedia_content_md as wcmd
from wikipedia_page_writer import CONTENT_CONFIG, f_writelangtodb
from wikipedia_queries import CONTENT_SQL_BUILDERS

load_dotenv()

AUTH_URL = "https://auth.enterprise.wikimedia.com/v1/login"
API = "https://api.enterprise.wikimedia.com"
TOKEN_MAX_AGE = 12 * 3600          # the access token lasts 24 h: renew well before
# 1.1.9: f_writelangtodb prints every section title and the format line (about 20,000 lines a
# chunk); only its error lines reach the report. 1.1.10: "error" as a whole word, and never in a
# section title line ("12. Title: ..."): a bare "error" matched "terrorisme" and "terroriste".
WRITER_ERROR = re.compile(r"\berror\b|⚠️|❌", re.IGNORECASE)
WRITER_TITLE = re.compile(r"^\s*\d+\. Title: ")


class Enterprise:
    """Login and chunk streaming. The token is kept in memory only."""

    def __init__(self):
        self.user = (os.getenv("WME_USERNAME") or "").lower()
        self.password = os.getenv("WME_PASSWORD") or ""
        if not self.user or not self.password:
            sys.exit("WME_USERNAME / WME_PASSWORD missing from the environment.")
        self.token = None
        self.token_at = 0.0

    def headers(self):
        if self.token is None or time.time() - self.token_at > TOKEN_MAX_AGE:
            r = requests.post(AUTH_URL, json={"username": self.user, "password": self.password}, timeout=60)
            if not r.ok:
                sys.exit(f"Enterprise login failed: HTTP {r.status_code}")
            self.token, self.token_at = r.json()["access_token"], time.time()
        return {"Authorization": "Bearer " + self.token}

    def snapshot(self, ident):
        r = requests.get(f"{API}/v2/snapshots/{ident}", headers=self.headers(), timeout=60)
        r.raise_for_status()
        return r.json()

    def records(self, ident, chunk):
        """Yield the raw NDJSON lines of one chunk, streamed from the presigned URL."""
        r = requests.get(f"{API}/v2/snapshots/{ident}/chunks/{chunk}/download",
                         headers=self.headers(), allow_redirects=False, timeout=60)
        if r.status_code not in (301, 302, 303, 307, 308):
            raise RuntimeError(f"chunk {chunk}: download answered HTTP {r.status_code}")
        # The presigned URL is valid 60 s and must not receive the bearer token.
        s = requests.get(r.headers["Location"], stream=True, timeout=300)
        s.raise_for_status()
        s.raw.decode_content = False
        try:
            tf = tarfile.open(fileobj=s.raw, mode="r|gz")
            for member in tf:
                f = tf.extractfile(member)
                if f is None:
                    continue
                while True:
                    line = f.readline()
                    if not line:
                        break
                    yield line
        finally:
            s.close()


def load_targets(families):
    """{qid: (content, id)} from the crawler's family queries, first family to claim a Qid wins."""
    targets = {}
    cursor = cp.f_getconnection().cursor()
    for content, builder in CONTENT_SQL_BUILDERS.items():
        if families and content not in families:
            continue
        t0 = time.time()
        cursor.execute(builder(""))
        n = 0
        for row in cursor.fetchall():
            if row["ID_WIKIDATA"] not in targets:
                targets[row["ID_WIKIDATA"]] = (content, row["id"])
                n += 1
        print(f"targets {content}: {n} new Qids ({time.time() - t0:.0f}s)")
    return targets


def parse_chunks(spec, available):
    out = []
    for part in spec.split(","):
        a, _, b = part.partition("-")
        out += list(range(int(a), int(b or a) + 1))
    bad = [i for i in out if i >= len(available)]
    if bad:
        sys.exit(f"chunk index out of range (0..{len(available) - 1}): {bad}")
    return out


def ws(text):
    return " ".join((text or "").split())


class Stats(dict):
    def add(self, key, n=1):
        self[key] = self.get(key, 0) + n


def db_is_fresher(cursor, qid, lang, rec):
    """1.1.7: True when the live crawl stored this page in Markdown after the record's revision.

    The snapshot is a monthly cut: a page the crawler read since then is at least as recent,
    and overwriting it would put an older revision back. Pages crawled before 1.1.0 (text
    only) are always taken, the snapshot's Markdown is what they lack.
    """
    rev = (rec.get("date_modified") or "")[:19]
    if not rev:
        return False
    cursor.execute("SELECT P.LAST_SUCCESS_AT, EXISTS(SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION S "
                   "WHERE S.ID_WIKIDATA = P.ID_WIKIDATA AND S.LANG = P.LANG AND S.CONTENT_MD <> '') AS HAS_MD "
                   "FROM T_WC_WIKIPEDIA_PAGE_LANG P WHERE P.ID_WIKIDATA = %s AND P.LANG = %s", (qid, lang))
    row = cursor.fetchone()
    if not row or not row["LAST_SUCCESS_AT"] or not row["HAS_MD"]:
        return False
    crawled = cp.paris_tz.localize(row["LAST_SUCCESS_AT"]).astimezone(timezone.utc)
    return crawled >= datetime.strptime(rev, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def compare_with_db(cursor, qid, lang, conv, stats):
    """Dry run: how the converted page differs from what the live crawl stored."""
    cursor.execute("SELECT DISPLAY_ORDER, TITLE, CONTENT_MD FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION "
                   "WHERE ID_WIKIDATA = %s AND LANG = %s", (qid, lang))
    db = {r["DISPLAY_ORDER"]: (r["TITLE"], r["CONTENT_MD"]) for r in cursor.fetchall()}
    if not db:
        stats.add("pages_new")
        return
    if not any(md for _, md in db.values()):
        stats.add("pages_db_text_only")         # crawled before 1.1.0: no Markdown to compare
        return
    stats.add("pages_compared")
    snap = {o: (t, md) for o, t, _x, md in conv["sections"]}
    stats.add("cmp_sections_db", len(db))
    stats.add("cmp_sections_snap", len(snap))
    same = sum(1 for o in db if snap.get(o) == db[o])
    same_ws = sum(1 for o in db if o in snap and snap[o][0] == db[o][0] and ws(snap[o][1]) == ws(db[o][1]))
    stats.add("cmp_sections_identical", same)
    stats.add("cmp_sections_identical_but_whitespace", same_ws - same)
    if [db[o][0] for o in sorted(db)] == [snap[o][0] for o in sorted(snap)]:
        stats.add("cmp_pages_same_titles")
    cursor.execute("SELECT COUNT(*) AS N FROM T_WC_WIKIPEDIA_PAGE_LANG_LINK WHERE ID_WIKIDATA = %s AND LANG = %s",
                   (qid, lang))
    stats.add("cmp_links_db", cursor.fetchone()["N"])
    stats.add("cmp_links_snap", len(conv["links"]))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--lang", required=True, choices=["en", "fr"])
    ap.add_argument("--chunks", required=True, help="chunk indexes: 0, 0-4, 0,7,12-14")
    ap.add_argument("--write", action="store_true", help="persist the pages (default: dry run)")
    ap.add_argument("--families", default="", help="comma-separated families (default: all)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N target pages (0: no cap)")
    ap.add_argument("--title-map", default="", help="directory for the per-chunk title to Qid maps")
    args = ap.parse_args()

    lang = args.lang
    ident = f"{lang}wiki_namespace_0"
    families = {f for f in args.families.split(",") if f}
    started = time.time()
    print(f"wikipedia_dump_ingest {ident} chunks {args.chunks} "
          f"{'WRITE' if args.write else 'dry run (nothing written)'} "
          f"{datetime.now(cp.paris_tz).strftime('%Y-%m-%d %H:%M:%S')}")

    wme = Enterprise()
    meta = wme.snapshot(ident)
    available = [c if isinstance(c, str) else c.get("identifier") for c in (meta.get("chunks") or [])]
    print(f"snapshot {ident}: modified {meta.get('date_modified')}, {len(available)} chunks")
    chunks = parse_chunks(args.chunks, available)

    targets = load_targets(families)
    print(f"targets: {len(targets)} Qids")

    conn = cp.f_getconnection()
    cursor2 = conn.cursor()
    total = Stats()
    stop = False
    for idx in chunks:
        chunk = available[idx]
        stats = Stats()
        t0 = time.time()
        tmap = None
        if args.title_map:
            os.makedirs(args.title_map, exist_ok=True)
            tpath = os.path.join(args.title_map, f"{chunk}.tsv.gz")
            tmap = gzip.open(tpath + ".part", "wt", encoding="utf-8")
        try:
            for line in wme.records(ident, chunk):
                stats.add("records")
                stats.add("bytes", len(line))
                rec = json.loads(line)
                qid = (rec.get("main_entity") or {}).get("identifier")
                title = rec.get("name") or ""
                if tmap is not None and qid and title:
                    tmap.write(f"{title}\t{qid}\t0\n")
                    for red in rec.get("redirects") or []:
                        if red.get("name"):
                            tmap.write(f"{red['name']}\t{qid}\t1\n")
                if not qid:
                    stats.add("records_without_qid")
                    continue
                target = targets.get(qid)
                if target is None:
                    continue
                content, lngid = target
                stats.add("targets")
                stats.add("targets_" + content)
                fresher = db_is_fresher(cursor2, qid, lang, rec)
                if fresher:
                    stats.add("pages_db_fresher")     # skipped by --write, still compared in a dry run
                    if args.write:
                        continue
                html = (rec.get("article_body") or {}).get("html") or ""
                try:
                    conv = wcmd.convert_page(BeautifulSoup(html, "html.parser"), lang, title)
                except Exception as err:
                    stats.add("convert_errors")
                    print(f"  convert error {qid} {lang} {title}: {err}")
                    continue
                stats.add("sections", len(conv["sections"]))
                stats.add("links", len(conv["links"]))
                stats.add("refs", len(conv["refs"]))
                if conv["links"]:
                    stats.add("pages_with_links")
                if not args.write:
                    compare_with_db(cursor2, qid, lang, conv, stats)
                else:
                    config = CONTENT_CONFIG[content]
                    payload = {
                        "site_key": f"{lang}wiki",
                        "page_title": title,
                        "page_url": rec.get("url") or "",
                        "main_image_url": "",
                        "main_image_checked": False,      # MAIN_IMAGE_URL left as it is
                        "page_images": [],
                        "keep_images": True,
                        "http_status": 200,
                        "success": True,
                        "has_content": bool(conv["sections"]),
                        "sections": conv["sections"],
                        "links": conv["links"],
                        "refs": conv["refs"],
                        "api_qids": {},                   # title join only, no API call
                    }
                    out = io.StringIO()
                    try:
                        with contextlib.redirect_stdout(out):
                            f_writelangtodb(payload, lngid, qid, lang, content, config["id"],
                                            config["imagetable"], config["imagecolumn"], cursor2,
                                            0, 0, blnwritecounters=False)
                        stats.add("pages_written")
                    except Exception as err:
                        stats.add("write_errors")
                        print(f"  write error {qid} {lang} {title}: {err}")
                    for msg in out.getvalue().splitlines():
                        if WRITER_ERROR.search(msg) and not WRITER_TITLE.match(msg):
                            stats.add("writer_error_lines")
                            print(f"  {qid} {lang} {title}: {msg.strip()}")
                if args.limit and total.get("targets", 0) + stats.get("targets", 0) >= args.limit:
                    stop = True
                    break
        except Exception as err:
            stats.add("chunk_errors")
            print(f"chunk {chunk}: stream error after {stats.get('records', 0)} records: {err}")
        finally:
            if tmap is not None:
                tmap.close()
                if not stats.get("chunk_errors") and not stop:
                    os.replace(tpath + ".part", tpath)
        elapsed = time.time() - t0
        print(f"chunk {chunk} ({elapsed:.0f}s): " + ", ".join(f"{k} {v}" for k, v in sorted(stats.items())))
        for k, v in stats.items():
            total.add(k, v)
        if stop:
            print(f"--limit {args.limit} reached")
            break

    print("-" * 80)
    print(f"TOTAL ({time.time() - started:.0f}s, {len(chunks)} chunk(s) asked)")
    for k, v in sorted(total.items()):
        print(f"  {k}: {v}")
    if total.get("cmp_sections_db"):
        print(f"  => sections identical {100 * total['cmp_sections_identical'] / total['cmp_sections_db']:.1f} %, "
              f"identical once whitespace is ignored "
              f"{100 * (total['cmp_sections_identical'] + total.get('cmp_sections_identical_but_whitespace', 0)) / total['cmp_sections_db']:.1f} %")
    if total.get("targets"):
        print(f"  => pages with links {100 * total.get('pages_with_links', 0) / total['targets']:.1f} % of target pages")


if __name__ == "__main__":
    main()
