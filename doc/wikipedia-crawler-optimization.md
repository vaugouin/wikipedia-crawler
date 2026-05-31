# Wikipedia Crawler — Optimization Plan

> **Status:** proposal / design document. No code changes are made by this document.
> **Goal:** cut the end-to-end runtime of `wikipedia_crawler.py` from **many days** down
> to hours (Phase 1) and ultimately to a one-time bulk download + local pass (Phase 2),
> while keeping the existing database output (sections, images, page metadata) intact for
> **English and French**.

---

## 1. Why it is slow today

`wikipedia_crawler.py` enriches ~420k database entities (movies, people, items, series,
characters, lists, awards, …) with English **and** French Wikipedia content. The current
design is **fully sequential** with **blocking HTTP**, and for every
`(entity × language)` pair it issues roughly **five separate web requests**:

| # | Request | Code | Notes |
|---|---------|------|-------|
| 1 | Wikidata `wbgetentities` (Q-id → page title) | `wikipedia_crawler_helpers.py:19` (`get_linked_pages`) | One id, one language per call; hard-coded `time.sleep(0.1)` before **every** call (`:36`) |
| 2 | REST `/page/summary` (lead image) | `wikipedia_images.py:62` (`get_wikipedia_main_image_url`) | |
| 3 | MediaWiki `query` `prop=images` + N× `imageinfo` (all page images) | `wikipedia_images.py:124` (`_get_wikipedia_page_media_items`) | `imageinfo` already batches 50 titles/call |
| 4 | MediaWiki `parse` (rendered HTML) for **image captions** | `wikipedia_images.py:211` (`_get_parsed_page_soup`, called from `get_wikipedia_page_images` `:484`) | |
| 5 | MediaWiki `parse` (rendered HTML) for **section extraction** | `wikipedia_crawler.py:882` | **Same HTML as #4, fetched a second time** |

Order-of-magnitude cost:

```
~420,000 entities × 2 languages × ~5 requests  ≈  4,000,000+ serialized round-trips
```

On top of the request count, three structural problems dominate the wall-clock time:

1. **No concurrency.** A single Python process walks entities one at a time
   (`wikipedia_crawler.py:760` → `:787` → `:793`), and every `requests.get` blocks until
   the response returns. Network latency, not CPU, is the bottleneck — and it is paid
   serially.
2. **No connection reuse.** Each call uses a fresh `requests.get(...)`, so every request
   re-pays DNS + TCP + TLS handshake instead of reusing a keep-alive `requests.Session`.
3. **Redundant work.** The rendered page HTML is fetched **twice** per page (#4 and #5),
   the Wikidata title lookup is done one id at a time even though the API accepts 50, and
   a fixed `time.sleep(0.1)` is added before every Wikidata call regardless of need.

### Database-layer constraint (affects any concurrency design)

`citizenphil.py` exposes a **single module-level PyMySQL connection**
(`f_getconnection` `:61`) and a SELECT-then-INSERT/UPDATE helper
(`f_sqlupdatearray` `:90`). PyMySQL connections are **not thread-safe**, so a
parallelization design must keep **all DB writes on one thread**. Resume state is tracked
through server variables such as `strwikipediacrawler<content>id`
(`wikipedia_crawler.py:991`), which must keep advancing **monotonically** so an
interrupted run resumes correctly.

---

## 2. Does a single downloadable Wikipedia archive exist? — Yes

There are two relevant kinds of bulk Wikipedia "backups" that can be processed locally
instead of hitting the live API per page:

| Option | Contents | Approx. size (en / fr) | Fit with current code |
|--------|----------|------------------------|------------------------|
| **Enterprise HTML dumps** (Snapshot API, NDJSON) | **Rendered HTML** per article — the same shape `action=parse` returns — plus `<img>`/figure markup and the article's **Wikidata Q-id** | enwiki ≈ **100 GB gz**, frwiki much smaller | **Near drop-in**: existing `extract_titles_and_text()` already parses rendered HTML; images parse from the same markup |
| **`pages-articles-multistream.xml.bz2`** | **Wikitext** (not rendered HTML) | enwiki ≈ **24 GB**, frwiki ≈ **6.4 GB** | Requires rewriting the extractor (wikitext ≠ HTML); weak image/caption support |

**Decision:** use the **Enterprise HTML dumps**. They map almost 1:1 onto the existing
parser and — critically — each record carries `main_entity.identifier` (the Wikidata
Q-id), so target entities can be selected **directly by Q-id with no title-resolution
step at all**.

> **Access / logistics notes**
> - As of **2025-03** the Enterprise HTML dumps are **no longer mirrored** on
>   `dumps.wikimedia.org`. They are downloaded via a **free Wikimedia Enterprise
>   account** (Snapshot API), choosing `enwiki` + `frwiki`, namespace 0 (articles).
> - Snapshots refresh **twice monthly** (≈ 2nd and 21st).
> - The English file is large (~100 GB gz). It must be **stream-decompressed and
>   processed line-by-line** — never fully extracted to disk.
> - **Image binaries are not in the article dumps** (images live on Wikimedia Commons).
>   Image **URLs and captions** are still recoverable from the rendered-HTML markup in
>   the dump (see Phase 2), which is what this crawler stores anyway.

---

## 3. Phased plan

The two phases are independent deliverables. Phase 1 is a fast, low-risk win that needs
no new infrastructure. Phase 2 removes the network from the hot path entirely. Doing the
small refactor in **2a** during Phase 1 lets both paths share one HTML-parsing core.

### Phase 1 — Parallelize & batch the live API (days → hours)

Ship these four steps incrementally; each is independently valuable and independently
verifiable.

#### 1a. Reuse connections + be a good API citizen *(smallest, safest)*
- Introduce **one shared `requests.Session`** configured with an `HTTPAdapter`
  (`pool_maxsize` ≈ worker count, a built-in `urllib3` `Retry` for 429/5xx with backoff)
  and `Accept-Encoding: gzip`. Replace the bare `requests.get(...)` calls in
  `wikipedia_crawler_helpers.py`, `wikipedia_images.py`, and `wikipedia_crawler.py:882`.
- Add `maxlag=5` to Wikimedia API params and honor `Retry-After`. Keep the
  `WIKIMEDIA_USER_AGENT` contact string populated (already supported via `.env`).

#### 1b. Fetch the rendered HTML once per page *(removes the duplicate `parse` call)*
- Fetch the parsed-HTML soup **once** per `(page, language)` and reuse it for **both**
  section extraction **and** image captions. Concretely: let
  `get_wikipedia_page_images` accept an **optional pre-fetched `soup`**
  (`wikipedia_images.py:475`/`:484`), and in the main loop fetch the parse HTML once
  (`wikipedia_crawler.py:869–912`), build the soup, pass it to image extraction, and feed
  the same HTML to `extract_titles_and_text`. Removes ~1 of every ~5 requests with zero
  behavior change.

#### 1c. Batch the Wikidata title resolution
- `wbgetentities` accepts **up to 50 ids** and **multiple languages** in a single call
  (`languages='en|fr'`). Add `get_linked_pages_batch(ids, props, languages)` to
  `wikipedia_crawler_helpers.py` and call it once per **50-entity chunk** instead of once
  per `(entity, language)`. This is a ~100× reduction in Wikidata calls and lets us
  **drop the `time.sleep(0.1)`** entirely.

#### 1d. Concurrency with a bounded thread pool *(the main multiplier)*
- Wrap the per-`(entity, language)` **fetch + parse** work in a
  `concurrent.futures.ThreadPoolExecutor` (start at **8–16 workers**, configurable via an
  env var). Workers perform **only network + HTML parsing** and return plain Python
  dicts: `{page_meta, main_image, page_images[], sections[]}`.
- **All database writes stay on the main thread**, draining completed futures **in
  submission order** through the existing `f_sqlupdatearray` / `cursor2` calls. This
  respects the single-connection constraint and keeps the resume checkpoints
  (`wikipedia_crawler.py:991`) monotonic.
- Add a **shared rate limiter** (token bucket / semaphore) capping global requests/sec so
  the added concurrency stays within Wikimedia etiquette.

**Files touched:** `wikipedia_crawler.py` (main loop `787–993`, header `1–21`),
`wikipedia_crawler_helpers.py` (session + batch helper), `wikipedia_images.py` (session +
accept injected soup).
**Expected result:** **~10–30× faster** (days → hours); no schema or infrastructure
change; output rows identical.

---

### Phase 2 — Offline processing from Enterprise HTML dumps (network removed)

Each NDJSON line in the dump is one article JSON with `name` (title),
`main_entity.identifier` (Wikidata Q-id), and `article_body.html` (rendered HTML, same
shape as `action=parse`). This lets us filter by Q-id and reuse the existing parser.

#### 2a. Extract a shared "HTML → rows" core *(do this during Phase 1)*
- Factor the logic that turns **rendered HTML (+soup)** into `(sections[], images[])` out
  of the live path into **one reusable function**, used by both
  `extract_titles_and_text` and the image parser. Both the live crawler (Phase 1) and the
  dump ingester (Phase 2) call this same core — guaranteeing identical output.

#### 2b. New offline ingester: `wikipedia_dump_ingest.py`
- Load the **target Q-id set** from the database — the union of `ID_WIKIDATA` across the
  existing `sqlbuilder` queries (`wikipedia_crawler.py:58–528`) — into a Python `set`.
- Stream each language's `.tar.gz` with `tarfile` + `gzip`, **line by line**. For each
  record whose `main_entity.identifier` is in the target set:
  - run the shared HTML→rows core on `article_body.html`;
  - write `T_WC_WIKIPEDIA_PAGE_LANG` (title/url/`PAGE_EXISTS`),
    `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`, and `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE` via the
    existing `f_sqlupdatearray`;
  - keep the FR-movie "Fiche technique → Format" extraction
    (`wikipedia_crawler.py:956–986`).
- **Images offline:** derive URLs from the HTML `<img>` / `a.mw-file-description` markup
  already handled by `_caption_from_soup` (`wikipedia_images.py:236`). The dump's `src`
  is a thumbnail (`/thumb/…/NNNpx-name`); reverse it to the original with the inverse of
  `_derive_thumb_url_from_original` (`wikipedia_images.py:384`). Captions come straight
  from the same HTML — **no extra requests**. Rare edge formats (SVG/TIFF) can optionally
  fall back to a single live `imageinfo` call, behind a flag.
- This replaces the Wikidata, REST, `query`/`imageinfo`, and `parse` calls **entirely**;
  network use drops to a **one-time download**. Re-run per twice-monthly snapshot.

**Files / artifacts:** new `wikipedia_dump_ingest.py`; shared parser carved out of
`wikipedia_crawler_helpers.py` / `wikipedia_images.py`; reuse
`citizenphil.f_sqlupdatearray`. Update `README.md` (download/run steps, disk + account
requirements) and `AGENTS.md` if the agent workflow context changes; update
`requirements.txt` only if a new dependency is introduced.

---

## 4. Trade-offs at a glance

| | Phase 1 (parallelize live API) | Phase 2 (Enterprise HTML dumps) |
|---|---|---|
| Effort | Low–medium | Medium |
| Infra needed | None | Free Wikimedia Enterprise account; ~100 GB+ disk for streaming en |
| Speed gain | ~10–30× (days → hours) | Near-instant per page after a one-time download |
| Freshness | Always live | As of last snapshot (twice monthly) |
| Network load | Reduced but still per-page | One bulk download, then offline |
| Risk to output | Minimal (same code paths) | Low (shared parser core) |

---

## 5. Verification

**Phase 1**
- Run on a small slice (raise a `resumeid`, or temporarily `LIMIT` one `sqlbuilder`) with
  `workers=1` vs `workers=8`; confirm **identical rows** in
  `T_WC_WIKIPEDIA_PAGE_LANG[_SECTION|_IMAGE]` for sample Q-ids (e.g. `Q24815`) and
  measure the speedup.
- Verify resume server-variables advance monotonically and a **mid-run restart resumes**
  correctly.
- `test_wikipedia_page_images.py` must still pass.

**Phase 2**
- Point the ingester at a small downloaded **frwiki** snapshot slice; for a handful of
  known Q-ids, **diff** sections/images against the live-API output to confirm parity
  (allowing thumbnail-vs-original URL normalization).
- Confirm a full streaming pass **never fully extracts** the tarball (bounded
  memory/disk).

---

## 6. Sources

- [Wikipedia: Database download](https://en.wikipedia.org/wiki/Wikipedia:Database_download)
- [Wikimedia Downloads (dumps)](https://dumps.wikimedia.org/)
- [Wikimedia Enterprise — Snapshot API docs](https://enterprise.wikimedia.com/docs/snapshot/)
- [Wikimedia Enterprise HTML Dump Archive (historical mirror)](https://dumps.wikimedia.org/other/enterprise_html/)
- [Meta-Wiki: Data dumps](https://meta.wikimedia.org/wiki/Data_dumps)
- [MediaWiki Action API — `wbgetentities` / `parse` / `query`](https://www.mediawiki.org/wiki/API:Main_page)
