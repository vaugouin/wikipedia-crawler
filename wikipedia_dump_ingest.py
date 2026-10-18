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
    python wikipedia_dump_ingest.py --check            # 0 a snapshot waits, 1 nothing to do, 2 cannot tell
    python wikipedia_dump_ingest.py --auto             # every language, resumed (run-if-new-snapshot.sh)
    python wikipedia_dump_ingest.py --mark-complete fr # record a snapshot ingested by hand-run batches

Credentials: WME_USERNAME / WME_PASSWORD from the environment (.env); never printed.
"""

import argparse
import contextlib
import gzip
import io
import json
import multiprocessing as mp
import os
import re
import sys
import tarfile
import time
from datetime import datetime, timezone

try:
    import resource                       # Linux only: the chunk process's peak memory
except ImportError:
    resource = None

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


def chunk_ranges(done):
    """{0, 1, 2, 5} -> "0-2,5": the done set as a server variable (VAR_VALUE is 255 chars)."""
    out, run = [], []
    for i in sorted(done):
        if run and i == run[-1] + 1:
            run.append(i)
        else:
            if run:
                out.append(f"{run[0]}-{run[-1]}" if len(run) > 1 else str(run[0]))
            run = [i]
    if run:
        out.append(f"{run[0]}-{run[-1]}" if len(run) > 1 else str(run[0]))
    return ",".join(out)


def parse_ranges(text):
    out = set()
    for part in (text or "").split(","):
        if part.strip():
            a, _, b = part.partition("-")
            out.update(range(int(a), int(b or a) + 1))
    return out


def snapshot_chunks(meta):
    return [c if isinstance(c, str) else c.get("identifier") for c in (meta.get("chunks") or [])]


def run_one_chunk(args, wme, ident, chunk, targets, limit_left, ctx):
    """One chunk in a forked child (1.1.11), or inline where fork does not exist. Prints its line."""
    t0 = time.time()
    if ctx is None:
        stats, stop = ingest_chunk(args, wme, ident, chunk, targets, limit_left)
    else:
        wme.headers()                         # renew the token here, not once in each child
        rd, wr = ctx.Pipe(duplex=False)
        proc = ctx.Process(target=chunk_worker, args=(wr, args, wme, ident, chunk, targets, limit_left))
        proc.start()
        wr.close()
        try:
            result = rd.recv()
        except EOFError:
            result = None
        rd.close()
        proc.join()
        if result is None:
            stats, stop = Stats(chunk_errors=1), False
            print(f"chunk {chunk}: worker died without a report (exit code {proc.exitcode}, -9 is a kill)")
        else:
            stats, stop = Stats(result[0]), result[1]
    print(f"chunk {chunk} ({time.time() - t0:.0f}s): " + ", ".join(f"{k} {v}" for k, v in sorted(stats.items())))
    return stats, stop


def print_total(total, started, nchunks, max_rss_mb):
    print("-" * 80)
    print(f"TOTAL ({time.time() - started:.0f}s, {nchunks} chunk(s) asked)")
    for k, v in sorted(total.items()):
        print(f"  {k}: {v}")
    if max_rss_mb:
        print(f"  max_rss_mb (largest chunk process): {max_rss_mb}")
    if total.get("cmp_sections_db"):
        print(f"  => sections identical {100 * total['cmp_sections_identical'] / total['cmp_sections_db']:.1f} %, "
              f"identical once whitespace is ignored "
              f"{100 * (total['cmp_sections_identical'] + total.get('cmp_sections_identical_but_whitespace', 0)) / total['cmp_sections_db']:.1f} %")
    if total.get("targets"):
        print(f"  => pages with links {100 * total.get('pages_with_links', 0) / total['targets']:.1f} % of target pages")


# -- Automatic mode (1.1.12, WIKIPEDIA-CRAWLER-025) ---------------------------------------------
# run-if-new-snapshot.sh asks --check every day and starts --auto in a detached container when it
# answers "work to do". Per language, three server variables hold the state:
#   strwikipediadumpingest<lang>snapshotdone     date_modified of the last snapshot ingested whole
#   strwikipediadumpingest<lang>snapshotcurrent  date_modified of the snapshot being ingested
#   strwikipediadumpingest<lang>chunksdone       its chunks done without error, as ranges ("0-74,76")
# A run that dies resumes at the next check with the chunks not yet done; a chunk in error is
# retried once at the end of its language, then left for the next run.

AUTO_LANGS = [x for x in (os.getenv("WME_INGEST_LANGS") or "fr,en").split(",") if x]
SV = "strwikipediadumpingest"
INGEST_FLAG = os.path.join("dumpdata", "INGEST_IN_PROGRESS")


def sv_get(name):
    return cp.f_getservervariable(SV + name, 0) or ""


def sv_set(name, value, desc):
    cp.f_setservervariable(SV + name, str(value), desc, 0)


def check(wme):
    """Exit code for run-if-new-snapshot.sh: 0 work to do, 1 nothing to do, 2 cannot tell."""
    asked = []
    for lang in AUTO_LANGS:
        current, done = sv_get(lang + "snapshotcurrent"), sv_get(lang + "snapshotdone")
        if current and current != done:
            print(f"{lang}: snapshot {current} not finished (chunks done {sv_get(lang + 'chunksdone') or 'none'})")
            return 0
        # Snapshots come out on the 1st: one ingested this month cannot have a successor yet,
        # so the API is asked only from the 1st until the new one is in (a few calls a month).
        if done[:7] == datetime.now(timezone.utc).strftime("%Y-%m"):
            print(f"{lang}: snapshot {done} already ingested this month")
            continue
        asked.append((lang, done))
    for lang, done in asked:
        try:
            latest = wme.snapshot(f"{lang}wiki_namespace_0").get("date_modified") or ""
        except Exception as err:
            print(f"{lang}: snapshot metadata unavailable: {err}")
            return 2
        if not latest:
            print(f"{lang}: snapshot metadata without date_modified")
            return 2
        if latest != done:
            print(f"{lang}: new snapshot {latest} (last ingested: {done or 'none'})")
            return 0
        print(f"{lang}: snapshot {latest} already ingested")
    return 1


def mark_complete(wme, lang):
    """Record the current snapshot of lang as ingested whole (after hand-run batches)."""
    meta = wme.snapshot(f"{lang}wiki_namespace_0")
    date, n = meta.get("date_modified") or "", len(snapshot_chunks(meta))
    sv_set(lang + "snapshotcurrent", date, f"{lang}wiki snapshot being ingested (date_modified)")
    sv_set(lang + "chunksdone", chunk_ranges(range(n)), f"{lang}wiki snapshot chunks ingested without error")
    sv_set(lang + "snapshotdone", date, f"{lang}wiki snapshot last ingested whole (date_modified)")
    print(f"{lang}: snapshot {date} marked as ingested whole ({n} chunks)")


def run_auto(args, wme, targets, ctx):
    """Every language's latest snapshot, resumed from the server variables. True when all are whole."""
    args.write = True
    args.title_map = args.title_map or "dumpdata"
    args.limit = 0
    complete = True
    for lang in AUTO_LANGS:
        ident = f"{lang}wiki_namespace_0"
        meta = wme.snapshot(ident)
        date, available = meta.get("date_modified") or "", snapshot_chunks(meta)
        if not date or not available:
            print(f"{lang}: snapshot metadata incomplete, language skipped")
            complete = False
            continue
        if date == sv_get(lang + "snapshotdone"):
            print(f"{lang}: snapshot {date} already ingested")
            continue
        if sv_get(lang + "snapshotcurrent") != date:
            sv_set(lang + "snapshotcurrent", date, f"{lang}wiki snapshot being ingested (date_modified)")
            sv_set(lang + "chunksdone", "", f"{lang}wiki snapshot chunks ingested without error")
            sv_set(lang + "startdatetime", datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"),
                   f"{lang}wiki snapshot ingest start")
        done = parse_ranges(sv_get(lang + "chunksdone"))
        args.lang = lang
        started, total, max_rss_mb, processed = time.time(), Stats(), 0, 0
        print(f"{lang}: snapshot {date}, {len(available)} chunks, {len(done)} already done")
        for attempt in (1, 2):
            todo = [i for i in range(len(available)) if i not in done]
            if not todo:
                break
            if attempt == 2:
                print(f"{lang}: retrying {len(todo)} chunk(s) in error: {chunk_ranges(todo)}")
            for idx in todo:
                sv_set("status", f"{lang} chunk {idx} ({len(done)}/{len(available)} done)", "snapshot ingest progress")
                stats, _ = run_one_chunk(args, wme, ident, available[idx], targets, 0, ctx)
                processed += 1
                max_rss_mb = max(max_rss_mb, stats.pop("max_rss_mb", 0))
                for k, v in stats.items():
                    total.add(k, v)
                if not stats.get("chunk_errors"):
                    done.add(idx)
                    sv_set(lang + "chunksdone", chunk_ranges(done), f"{lang}wiki snapshot chunks ingested without error")
        print_total(total, started, processed, max_rss_mb)
        if len(done) == len(available):
            sv_set(lang + "snapshotdone", date, f"{lang}wiki snapshot last ingested whole (date_modified)")
            sv_set(lang + "enddatetime", datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"),
                   f"{lang}wiki snapshot ingest end")
            print(f"{lang}: snapshot {date} ingested whole")
        else:
            complete = False
            print(f"{lang}: {len(available) - len(done)} chunk(s) still in error, left for the next run")
    sv_set("status", "idle" if complete else "incomplete, resumes at the next check", "snapshot ingest progress")
    return complete


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--lang", choices=["en", "fr"])
    ap.add_argument("--chunks", help="chunk indexes: 0, 0-4, 0,7,12-14")
    ap.add_argument("--write", action="store_true", help="persist the pages (default: dry run)")
    ap.add_argument("--families", default="", help="comma-separated families (default: all)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N target pages (0: no cap)")
    ap.add_argument("--title-map", default="", help="directory for the per-chunk title to Qid maps")
    ap.add_argument("--check", action="store_true", help="exit 0 if a snapshot waits, 1 if not, 2 if unknown")
    ap.add_argument("--auto", action="store_true", help="ingest every language's latest snapshot, resumed")
    ap.add_argument("--mark-complete", choices=["en", "fr"], help="record LANG's snapshot as ingested whole")
    args = ap.parse_args()
    if not (args.check or args.auto or args.mark_complete) and not (args.lang and args.chunks):
        ap.error("--lang and --chunks are required, unless --check, --auto or --mark-complete")

    if args.check:
        # Any failure must read "cannot tell" (2): an uncaught error exits 1, which the cron
        # script would take for "nothing to do".
        try:
            code = check(Enterprise())
        except BaseException as err:
            print(f"check failed: {err!r}")
            code = 2
        sys.exit(code)
    wme = Enterprise()
    if args.mark_complete:
        mark_complete(wme, args.mark_complete)
        return

    families = {f for f in args.families.split(",") if f}
    started = time.time()
    # 1.1.11: one forked child per chunk. Batch 2 of frwiki was killed by the kernel's OOM killer
    # in its 32nd chunk with 21 GB resident: memory grew from chunk to chunk. A child returns all
    # of it when it exits, whatever the cause, and inherits the target map without reloading it.
    ctx = mp.get_context("fork") if "fork" in mp.get_all_start_methods() else None

    if args.auto:
        print(f"wikipedia_dump_ingest --auto {','.join(AUTO_LANGS)} "
              f"{datetime.now(cp.paris_tz).strftime('%Y-%m-%d %H:%M:%S')}")
        targets = load_targets(families)
        print(f"targets: {len(targets)} Qids")
        complete = run_auto(args, wme, targets, ctx)
        if complete and os.path.exists(INGEST_FLAG):
            os.remove(INGEST_FLAG)            # lets wikipedia-crawler.sh start the crawler again
            print(f"{INGEST_FLAG} removed: the crawler may start again")
        print(f"--auto finished ({time.time() - started:.0f}s): {'complete' if complete else 'incomplete'}")
        sys.exit(0 if complete else 1)

    lang = args.lang
    ident = f"{lang}wiki_namespace_0"
    print(f"wikipedia_dump_ingest {ident} chunks {args.chunks} "
          f"{'WRITE' if args.write else 'dry run (nothing written)'} "
          f"{datetime.now(cp.paris_tz).strftime('%Y-%m-%d %H:%M:%S')}")
    meta = wme.snapshot(ident)
    available = snapshot_chunks(meta)
    print(f"snapshot {ident}: modified {meta.get('date_modified')}, {len(available)} chunks")
    chunks = parse_chunks(args.chunks, available)

    targets = load_targets(families)
    print(f"targets: {len(targets)} Qids")

    total = Stats()
    max_rss_mb = 0
    for idx in chunks:
        limit_left = args.limit - total.get("targets", 0) if args.limit else 0
        stats, stop = run_one_chunk(args, wme, ident, available[idx], targets, limit_left, ctx)
        max_rss_mb = max(max_rss_mb, stats.pop("max_rss_mb", 0))
        for k, v in stats.items():
            total.add(k, v)
        if stop:
            print(f"--limit {args.limit} reached")
            break
    print_total(total, started, len(chunks), max_rss_mb)


def chunk_worker(pipe, args, wme, ident, chunk, targets, limit_left):
    """Child process for one chunk: its own database connection, its stats sent back to the parent."""
    cp.connectioncp = None                       # the inherited socket belongs to the parent
    stats, stop = ingest_chunk(args, wme, ident, chunk, targets, limit_left)
    if resource is not None:
        stats["max_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    pipe.send((dict(stats), stop))
    pipe.close()


def ingest_chunk(args, wme, ident, chunk, targets, limit_left):
    """Stream one chunk, write (or compare) its target pages. Returns (stats, stop)."""
    lang = args.lang
    cursor2 = cp.f_getconnection().cursor()
    stats = Stats()
    stop = False
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
            if limit_left and stats.get("targets", 0) >= limit_left:
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
    return stats, stop


if __name__ == "__main__":
    main()
