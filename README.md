# wikipedia-crawler

`wikipedia_crawler.py` is a batch enrichment script that reads Wikidata-linked entities from the database, retrieves their English and French Wikipedia pages, extracts structured section content, and stores both the textual content and selected Wikipedia image URLs back into MySQL.

## Entities crawled

The table below lists every entity family processed by `wikipedia_crawler.py`, in execution order. Each row corresponds to one entry in the `arrprocesses` config in `wikipedia_crawler.py`.

| ID  | Content             | Source table                | Image target                                             |
|-----|---------------------|-----------------------------|----------------------------------------------------------|
| 201 | `movie`             | `T_WC_TMDB_MOVIE`           | `T_WC_WIKIDATA_MOVIE_V1.WIKIPEDIA_POSTER_PATH`           |
| 202 | `person`            | `T_WC_TMDB_PERSON`          | `T_WC_WIKIDATA_PERSON_V1.WIKIPEDIA_PROFILE_PATH`         |
| 203 | `item`              | `T_WC_WIKIDATA_ITEM_V1`     | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 204 | `serie`             | `T_WC_TMDB_SERIE`           | `T_WC_WIKIDATA_SERIE_V1.WIKIPEDIA_POSTER_PATH`           |
| 205 | `wikidatacharacter` | `T_WC_WIKIDATA_CHARACTER_V1`| `T_WC_WIKIDATA_CHARACTER_V1.WIKIPEDIA_PROFILE_PATH`      |
| 209 | `other`             | hard-coded `Q1204187`       | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 210 | `list`              | `T_WC_T2S_LIST`             | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 211 | `movement`          | `T_WC_T2S_MOVEMENT`         | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 212 | `collection`        | `T_WC_T2S_COLLECTION`       | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 213 | `group`             | `T_WC_T2S_GROUP`            | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 214 | `death`             | `T_WC_T2S_DEATH`            | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 215 | `award`             | `T_WC_T2S_AWARD`            | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 216 | `nomination`        | `T_WC_T2S_NOMINATION`       | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 217 | `topic`             | `T_WC_T2S_TOPIC`            | `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`             |
| 223 | `technical`         | `T_WC_T2S_TECHNICAL`        | `T_WC_T2S_TECHNICAL.WIKIPEDIA_IMAGE_PATH`                |
| 218 | `character`         | `T_WC_TMDB_CHARACTER`       | `T_WC_WIKIDATA_CHARACTER_V1.WIKIPEDIA_PROFILE_PATH`      |
| 219 | `tmdbcollection`    | `T_WC_TMDB_COLLECTION`      | _(not yet configured)_                                   |
| 220 | `episode`           | `T_WC_TMDB_EPISODE`         | _(planned: `T_WC_WIKIDATA_EPISODE_V1.WIKIPEDIA_POSTER_PATH` — see [Planned future work](#planned-future-work))_ |
| 221 | `keyword`           | `T_WC_TMDB_KEYWORD`         | _(not yet configured)_                                   |
| 222 | `season`            | `T_WC_TMDB_SEASON`          | _(planned: `T_WC_WIKIDATA_SEASON_V1.WIKIPEDIA_POSTER_PATH` — see [Planned future work](#planned-future-work))_ |

For every row of every entity family, page metadata, page sections, and page images are written to the shared `T_WC_WIKIPEDIA_PAGE_LANG*` tables regardless of whether an "Image target" is configured. The "Image target" column only controls where the **main** image URL is additionally written back into the entity's own table.

## What the crawler does

The crawler processes multiple content types in sequence:

- `movie`
- `person`
- `item`
- `serie`
- `wikidatacharacter`
- `other`
- `list`
- `movement`
- `collection`
- `group`
- `death`
- `award`
- `nomination`
- `topic`
- `character`
- `tmdbcollection`
- `episode`
- `keyword`
- `season`

For each content type, it:

1. Selects records that already have a valid `ID_WIKIDATA`.
2. Resolves the Wikipedia page title for each target language (`en` and `fr`) using the Wikidata API.
3. Retrieves the main image URL from Wikipedia when an image destination table/column is configured for that content type.
4. Downloads parsed Wikipedia page HTML through the MediaWiki `parse` API.
5. Extracts the page into structured sections using `extract_titles_and_text()`.
6. Stores those sections in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`.
7. Performs some content-specific enrichment, such as extracting the French movie `Format` line from the `Fiche technique` section.
8. Updates server variables so the execution state can be monitored and resumed.

## Main modules

### `wikipedia_crawler.py`
Main orchestrator.

Responsibilities:

- determine which content type should be processed
- build SQL queries for each content family
- loop through database rows and languages
- call helper functions and image retrieval logic
- write extracted content to database tables
- maintain progress and resume state in server variables

### `wikipedia_crawler_helpers.py`
Contains extracted helper functions used by the crawler:

- `get_linked_pages()`
- `extract_titles_and_text()`

### `wikipedia_images.py`
Wikipedia image retrieval helpers:

- `get_wikipedia_main_image_url(title, lang)` — lead image URL for a page via the REST summary endpoint.
- `get_wikipedia_page_images(title, lang)` — every image embedded on the page, with URL, thumbnail, dimensions, localized filename, and a caption scraped from the rendered HTML when available. Image enumeration uses the MediaWiki Action API (`prop=images` + `prop=imageinfo`); captions are extracted by parsing the page HTML (`action=parse`) once per page. Each image is matched to its parent `<a class="mw-file-description">` href (with space/underscore normalization and URL decoding) and then the DOM is walked upward looking for one of four specific captioning markers:

  - `<figure>` with an inner `<figcaption>`
  - `<li class="gallerybox">` with a sibling `<div class="gallerytext">` (MediaWiki `<gallery>` tag)
  - `<div class="thumb">` with an inner `<div class="thumbcaption">`
  - `<td class="infobox-image">` with an inner `<div class="infobox-caption">` (en infoboxes) or `<div class="images">` with a sibling `<div class="legend">` (fr `infobox_v3`)

  Decorative icons that do not sit inside one of these markers (flag icons, language tags, Wikipedia logos, etc.) return no caption rather than inheriting a nearby unrelated caption.
- `get_main_image_caption_for_page(title, image_url, lang)` — best-effort caption for the lead image (parsed HTML first, then Commons / local file metadata as fallback).

### `wikipedia_page_writer.py`
Shared per-entity fetch + persist pipeline used by both the full crawler and the single-Qid entry point:

- `f_fetchlangpayload(...)` — network + HTML parsing for one `(entity, language)` page (thread-safe, no DB work).
- `f_writelangtodb(...)` — persists one payload (page metadata, images, sections, main-image and movie-Format writebacks). Its `blnwritecounters` flag lets the single-Qid path skip monitoring-counter writes.
- `CONTENT_CONFIG` — per-content `id` / `imagetable` / `imagecolumn` / source-table mapping mirroring `arrprocesses` in `wikipedia_crawler.py`.

Living here (rather than in `wikipedia_crawler.py`, whose body runs a full crawl at import time) lets `wikipedia_functions.py` reuse this logic without launching a crawl.

### `wikipedia_functions.py`
Single-Qid entry point, analogous to the `tmdb_functions` one-shot helpers:

- `f_wikipediaqidtosqleverything(strwikidataid, strcontent="item", arrlanguages=("en", "fr"))` — fully refreshes the Wikipedia data for one Wikidata entity, parallel-safe with a running crawler. See [Refreshing a single Qid in a parallel container](#refreshing-a-single-qid-in-a-parallel-container).

## Data flow

For each selected row:

1. The crawler reads `ID_WIKIDATA` from the source table.
2. It calls `get_linked_pages(wikidata_id, 'sitelinks', language)`.
3. It looks up the page title in `entities[ID_WIKIDATA]['sitelinks'][<lang>wiki]['title']`.
4. It stores page-level Wikipedia metadata in `T_WC_WIKIPEDIA_PAGE_LANG`, including the site key, page title, page URL, and page existence flag.
5. If configured for that content type, it calls `wikipedia_images.get_wikipedia_main_image_url(page_title, language)` and, when a URL is found, stores it in the configured destination table/column using `cp.f_sqlupdatearray()`.
6. It calls `wikipedia_images.get_wikipedia_page_images(page_title, language)` to enumerate every image on the page and writes one row per image into `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE`, flagging the one matching the main image URL with `IS_MAIN_IMAGE = 1`. Stale trailing rows beyond the new image count are deleted.
7. It then calls the Wikipedia `parse` API to get rendered HTML.
8. It updates `T_WC_WIKIPEDIA_PAGE_LANG` with page crawl status such as the last crawl timestamp, HTTP status, and last successful crawl timestamp.
9. The HTML is normalized by wrapping it in a `<body>` tag.
10. `extract_titles_and_text()` converts the page into `(title, content)` pairs.
11. Each section is inserted or updated in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`.
12. Any old sections beyond the latest `DISPLAY_ORDER` are deleted for that Wikidata ID and language.

## Content types and image destinations

The crawler currently associates image results with these destination tables/columns:

- `movie` -> `T_WC_WIKIDATA_MOVIE_V1.WIKIPEDIA_POSTER_PATH`
- `person` -> `T_WC_WIKIDATA_PERSON_V1.WIKIPEDIA_PROFILE_PATH`
- `item` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `serie` -> `T_WC_WIKIDATA_SERIE_V1.WIKIPEDIA_POSTER_PATH`
- `wikidatacharacter` -> `T_WC_WIKIDATA_CHARACTER_V1.WIKIPEDIA_PROFILE_PATH`
- `other` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `list` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `movement` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `collection` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `group` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `death` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `award` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `nomination` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `topic` -> `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`
- `character` -> `T_WC_TMDB_CHARACTER.WIKIPEDIA_IMAGE_PATH`

The following content families still store full page-level image rows in `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE`, but do not currently write the main image URL back into a source-table image column:

- `tmdbcollection`
- `episode`
- `keyword`
- `season`

For `episode` and `season`, the downloaded Wikipedia pages are sourced from `T_WC_TMDB_EPISODE` and `T_WC_TMDB_SEASON` respectively, and the destination Wikidata tables (`T_WC_WIKIDATA_EPISODE_V1` and `T_WC_WIKIDATA_SEASON_V1`) already exist with a `WIKIPEDIA_POSTER_PATH` column. Wiring the main image URL into those columns is intentionally deferred — see [Planned future work](#planned-future-work).

## Database writes performed by the crawler

### Page-level Wikipedia metadata
Page metadata is written into:

- `T_WC_WIKIPEDIA_PAGE_LANG`

Fields written include:

- `ID_WIKIDATA`
- `LANG`
- `ITEM_TYPE`
- `WIKIPEDIA_SITE_KEY`
- `WIKIPEDIA_PAGE_TITLE`
- `WIKIPEDIA_PAGE_URL`
- `PAGE_EXISTS`
- `HTTP_STATUS`
- `LAST_CRAWLED_AT`
- `LAST_SUCCESS_AT`

### Page-level Wikipedia images
All image items detected for a Wikipedia page are written into:

- `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE`

Fields written include:

- `ID_WIKIDATA`
- `LANG`
- `ITEM_TYPE`
- `DISPLAY_ORDER`
- `IMAGE_URL`
- `IMAGE_URL_NORMALIZED`
- `THUMBNAIL_URL`
- `MEDIA_TYPE`
- `FILE_NAME`
- `COMMONS_TITLE`
- `CAPTION`
- `IS_MAIN_IMAGE`

Rows for a page/language are refreshed on each crawl, and stale trailing image rows are deleted.

### Structured page content
Section content is written into:

- `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`

Fields written include:

- `ID_WIKIDATA`
- `LANG`
- `ITEM_TYPE`
- `DISPLAY_ORDER`
- `TITLE`
- `CONTENT`

### Image URL enrichment
When configured, the main image URL is written back to a content-specific Wikidata table.

#### UI-chrome filtering (which image is allowed to be the main image)

Wikipedia articles embed template decoration that is not the subject: edit pencils, maintenance banners, project logos, stub markers, featured-article stars. The crawler filters these out with `_UI_CHROME_PATTERNS` / `_is_ui_chrome_file` in `wikipedia_images.py`, at three points:

1. **Page-image enumeration** : chrome `File:` titles are dropped before the `imageinfo` step, so they never reach `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE` (and it saves an API call per 50 files filtered).
2. **Lead image** : the REST summary endpoint occasionally returns a maintenance banner as `originalimage`. `is_acceptable_main_image_url()` rejects it and lets the fallback look for a real picture.
3. **Fallback** : when no lead image exists, the crawler picks the first page image that can plausibly *be* the subject, not merely the first image on the page. SVG is excluded **here only** (`allow_svg=False`): that deep in an article a vector file is decoration far more often than it is the subject, whereas a lead image chosen by MediaWiki itself may legitimately be an SVG logo.

When nothing qualifies, the crawler **writes nothing and leaves the column untouched**. An empty illustration is honest; an edit pencil presented as a film poster is not.

Titles are normalized (spaces to underscores) before matching, because the Action API returns display form (`File:OOjs UI icon edit-ltr-progressive.svg`) while upload URLs and the patterns use underscores. Skipping that normalization is what made the filter inert before WIKIPEDIA-CRAWLER-019.

Values written before the filter existed are cleaned by [`migrations/clear_ui_chrome_images.py`](migrations/clear_ui_chrome_images.py) (dry run by default, `--apply` to clear). Run it **after** deploying the code fix, otherwise a crawler still running the old code writes the chrome values straight back.

For `item`, `other`, `list`, `movement`, `collection`, `group`, `death`, `award`, `nomination`, `topic`, and `technical`, the crawler writes to:

- `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`

For `character`, the crawler writes to:

- `T_WC_TMDB_CHARACTER.WIKIPEDIA_IMAGE_PATH`

For `tmdbcollection`, `episode`, `keyword`, and `season`, no source-table main image column is currently configured. These processes still populate:

- `T_WC_WIKIPEDIA_PAGE_LANG`
- `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE`
- `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`

For `episode` and `season` specifically, the rows selected from `T_WC_TMDB_EPISODE` and `T_WC_TMDB_SEASON` are fully crawled (page metadata, page-level images, and section content). Writing the main image URL into `T_WC_WIKIDATA_EPISODE_V1.WIKIPEDIA_POSTER_PATH` and `T_WC_WIKIDATA_SEASON_V1.WIKIPEDIA_POSTER_PATH` is **not yet enabled** and is tracked in [Planned future work](#planned-future-work).

The `other` family is split into multiple independent resumable processes:

- `209` -> `other`
- `210` -> `list`
- `211` -> `movement`
- `212` -> `collection`
- `213` -> `group`
- `214` -> `death`
- `215` -> `award`
- `216` -> `nomination`
- `217` -> `topic`
- `223` -> `technical`
- `218` -> `character`
- `219` -> `tmdbcollection`
- `220` -> `episode`
- `221` -> `keyword`
- `222` -> `season`

To avoid crawling the same Wikidata entity multiple times during a full run, the crawler applies deduplication by process precedence.

The process order is:

- `movie`
- `person`
- `item`
- `serie`
- `wikidatacharacter`
- `other`
- `list`
- `movement`
- `collection`
- `group`
- `death`
- `award`
- `nomination`
- `topic`
- `technical`
- `character`
- `tmdbcollection`
- `episode`
- `keyword`
- `season`

Each later process excludes entities already owned by earlier families in that order. Note that `wikidatacharacter` (process 205, sourced from `T_WC_WIKIDATA_CHARACTER_V1`) and `character` (process 218, sourced from `T_WC_TMDB_CHARACTER`) are two distinct processes operating on character entities from different upstream tables. The Wikidata-native source runs first; the TMDB-sourced process then excludes any IDs already covered by it.

Examples:

- `serie` excludes IDs already covered by `movie`, `person`, and `item`
- `list` excludes IDs already covered by `movie`, `person`, `item`, `serie`, and `other`
- `movement` excludes everything excluded by `list`, plus entities already covered by `list`
- `topic` excludes all earlier families in the chain
- `season` excludes all earlier families plus `character`, `tmdbcollection`, `episode`, and `keyword`

This keeps image enrichment targeted at the configured destination column for each family while still preventing duplicate crawling across the full run.

### Movie-specific technical metadata
For French movie pages, the crawler tries to extract the `Format` line from the `Fiche technique` section and updates:

- `T_WC_TMDB_MOVIE.WIKIPEDIA_FORMAT_LINE`
- `T_WC_TMDB_MOVIE.DAT_WIKIPEDIA_FORMAT_LINE`

## Resume mechanism

The crawler is designed to resume after interruption by using server variables stored in the database.

### 1. Content-level resume
The variable below controls which content family should be resumed next:

- `strwikipediacrawlercurrentcontent`

At startup, the crawler reads this variable and adjusts `arrprocessscope`:

- if empty, it starts from `movie`
- if `person`, it skips `movie`
- if `item`, it skips `movie` and `person`
- if `serie`, it skips `movie`, `person`, and `item`
- if `wikidatacharacter`, it skips `movie`, `person`, `item`, and `serie`
- if `other`, it resumes directly from `other`
- if `list`, it resumes directly from `list`
- if `movement`, it resumes directly from `movement`
- if `collection`, it resumes directly from `collection`
- if `group`, it resumes directly from `group`
- if `death`, it resumes directly from `death`
- if `award`, it resumes directly from `award`
- if `nomination`, it resumes directly from `nomination`
- if `topic`, it resumes directly from `topic`
- if `technical`, it resumes directly from `technical`
- if `character`, it resumes directly from `character`
- if `tmdbcollection`, it resumes directly from `tmdbcollection`
- if `episode`, it resumes directly from `episode`
- if `keyword`, it resumes directly from `keyword`
- if `season`, it resumes directly from `season`

At the end of each content family, it advances `strwikipediacrawlercurrentcontent` to the next stage.

This means that if the crawler stops between content families, the next execution resumes from the next unfinished family rather than restarting from the beginning.

### 2. Per-content last processed ID
For each content family, the crawler keeps the last processed identifier in a server variable:

- `strwikipediacrawlermovieid`
- `strwikipediacrawlerpersonid`
- `strwikipediacrawleritemid`
- `strwikipediacrawlerserieid`
- `strwikipediacrawlerwikidatacharacterid`
- `strwikipediacrawlerotherid`
- `strwikipediacrawlerlistid`
- `strwikipediacrawlermovementid`
- `strwikipediacrawlercollectionid`
- `strwikipediacrawlergroupid`
- `strwikipediacrawlerdeathid`
- `strwikipediacrawlerawardid`
- `strwikipediacrawlernominationid`
- `strwikipediacrawlertopicid`
- `strwikipediacrawlertechnicalid`
- `strwikipediacrawlercharacterid`
- `strwikipediacrawlertmdbcollectionid`
- `strwikipediacrawlerepisodeid`
- `strwikipediacrawlerkeywordid`
- `strwikipediacrawlerseasonid`

These values are read at startup and injected into the SQL query using conditions such as:

- `AND ID_MOVIE >= ...`
- `AND ID_PERSON >= ...`
- `AND ID_WIKIDATA >= ...`

During processing, the crawler updates progress variables such as:

- `strwikipediacrawler<content>wikidataid`
- `strwikipediacrawler<content>id`

These track the current row being processed.

### 3. Execution status tracking
The crawler also stores monitoring variables such as:

- `strwikipediacrawlerstartdatetime`
- `strwikipediacrawlerenddatetime`
- `strwikipediacrawlercurrentprocess`
- `strwikipediacrawlerprocessesexecuted`
- `strwikipediacrawlertotalruntime`
- `strwikipediacrawlertotalruntimesecond`
- per-content start/end datetimes
- per-content English and French page counters

These variables are useful both for observability and for confirming where an interrupted run stopped.

## Quick mode

Quick mode is a parallel-execution hatch that lets a secondary `wikipedia-crawler` container churn through a fixed, ordered subset of processes while the main container keeps running long jobs (typically `202 person`). It is controlled by two top-of-file variables in [wikipedia_crawler.py](wikipedia_crawler.py):

- `intquickmode` — when `True`, restricts the run to the IDs listed in `arrquickprocessids` and **skips writes to shared resume-state server variables** so the two containers do not corrupt each other's checkpoints.
- `arrquickprocessids` — an **ordered list** of process IDs. Quick mode iterates this list in order via an `id → processconfig` lookup, so the list order — not the `arrprocesses` definition order — determines execution order.

The list is ordered from least-recently to most-recently updated `ITEM_TYPE` in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION` (queried with `SELECT MAX(TIM_UPDATED), ITEM_TYPE ... GROUP BY ITEM_TYPE ORDER BY MAX_TIM_UPDATED ASC`). This refreshes the stalest content first. `ITEM_TYPE`s with no rows in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION` (never crawled) are placed at the head of the list.

Current order:

| Position | ID  | Content          | MAX(TIM_UPDATED) |
|----------|-----|------------------|------------------|
| 1        | 212 | `collection`     | never updated    |
| 2        | 213 | `group`          | never updated    |
| 3        | 214 | `death`          | never updated    |
| 4        | 219 | `tmdbcollection` | never updated    |
| 5        | 221 | `keyword`        | never updated    |
| 6        | 209 | `other`          | 2026-04-14       |
| 7        | 203 | `item`           | 2026-05-08       |
| 8        | 204 | `serie`          | 2026-05-09       |
| 9        | 210 | `list`           | 2026-05-09       |
| 10       | 211 | `movement`       | 2026-05-09       |
| 11       | 215 | `award`          | 2026-05-09       |
| 12       | 216 | `nomination`     | 2026-05-09       |
| 13       | 217 | `topic`          | 2026-05-09       |
| 14       | 201 | `movie`          | 2026-05-18       |
| 15       | 205 | `character` (Wikidata) | 2026-05-20 |
| 16       | 218 | `character` (TMDB)     | 2026-05-20 |
| 17       | 223 | `technical`      | 2026-05-20       |
| 18       | 202 | `person`         | 2026-05-23       |
| 19       | 220 | `episode`        | 2026-05-24       |
| 20       | 222 | `season`         | 2026-05-24       |

When `intquickmode = False`, the regular `arrprocessscope = arrprocesses[resume_index:]` path applies and the [Resume mechanism](#resume-mechanism) above governs ordering and checkpointing.

## Refreshing a single Qid in a parallel container

Quick mode parallelizes *whole processes*. When you instead want to refresh **one specific Wikidata entity** on demand — while the main `wikipedia-crawler` container keeps running long jobs like `202 person` — use the single-Qid entry point in [wikipedia_functions.py](wikipedia_functions.py). This mirrors the `tmdb-crawler` pattern of dropping into an interactive Python container and calling a one-shot function (`tf.f_tmdbmovietosqleverything(13860)`).

### The function

```python
import wikipedia_functions as wf

wf.f_wikipediaqidtosqleverything("Q24815")                       # item (default type)
wf.f_wikipediaqidtosqleverything("Q25188", strcontent="movie")   # a movie
wf.f_wikipediaqidtosqleverything("Q42",    strcontent="person")  # a person
wf.f_wikipediaqidtosqleverything("Q1234",  strcontent="serie", arrlanguages=("fr",))  # French only
```

`f_wikipediaqidtosqleverything(strwikidataid, strcontent="item", arrlanguages=("en", "fr"))` runs the exact per-entity pipeline the full crawler runs for one row: it resolves the `en`/`fr` page titles from Wikidata, then writes page metadata to `T_WC_WIKIPEDIA_PAGE_LANG`, every page image to `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE`, and structured sections to `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`, plus the content-specific main-image writeback and (for `strcontent="movie"`) the French `Fiche technique` Format line. `strcontent` must be one of the entity families listed in [Entities crawled](#entities-crawled) (`movie`, `person`, `item`, `serie`, `wikidatacharacter`, `other`, `list`, `movement`, `collection`, `group`, `death`, `award`, `nomination`, `topic`, `technical`, `character`, `tmdbcollection`, `episode`, `keyword`, `season`); it selects the main-image destination column and enables movie-only enrichment.

**Why it is parallel-safe:** the per-entity fetch/persist code is shared with the main crawler (extracted into [wikipedia_page_writer.py](wikipedia_page_writer.py)), but this path writes **no** resume-state or monitoring server variables (it calls the shared writer with `blnwritecounters=False`). It therefore cannot advance or corrupt the main container's `strwikipediacrawler*` checkpoints. All page/section/image writes are keyed upserts on `(ID_WIKIDATA, LANG, DISPLAY_ORDER)`, so refreshing a Qid is idempotent. Run it against a Qid the main crawler is not simultaneously processing.

### Running it in a second container

Use the helper [wikipedia-crawler-manual.sh](wikipedia-crawler-manual.sh), which builds the same image and starts an interactive container under a **different** name (`wikipedia-crawler-manual`) so it coexists with the always-on `wikipedia-crawler` container. It mounts the working tree and reads the same host-managed `--env-file`, exactly like the main launcher.

Interactive REPL:

```bash
./wikipedia-crawler-manual.sh
>>> import wikipedia_functions as wf
>>> wf.f_wikipediaqidtosqleverything("Q25188", strcontent="movie")
```

One-shot (run a single Qid and exit):

```bash
./wikipedia-crawler-manual.sh Q25188 --item-type movie
./wikipedia-crawler-manual.sh Q24815              # defaults to item, en + fr
./wikipedia-crawler-manual.sh Q24815 --lang fr    # French only
```

Or, to reproduce the tmdb-crawler workflow by hand (mirroring the attached screenshot's `docker run -it ... python`):

```bash
docker build -t wikipedia-crawler-python-app .
docker run -it --rm --network="host" \
  --env-file /home/debian/docker/wikipedia-crawler/.env \
  -v $(pwd):/home/debian/docker/wikipedia-crawler \
  --name wikipedia-crawler-manual wikipedia-crawler-python-app python
# then, in the REPL:
>>> import citizenphil as cp
>>> import wikipedia_functions as wf
>>> wf.f_wikipediaqidtosqleverything("Q25188", strcontent="movie")
```

Unlike [test_wikipedia_page_images.py](test_wikipedia_page_images.py) (which only mirrors steps 4–6, the image pipeline), `f_wikipediaqidtosqleverything` runs the **full** per-entity data flow including section extraction and content-specific enrichment.

## Important detail about resume behavior

The resume mechanism is checkpoint-based, not transactionally exact.

That means:

- it resumes from the saved content family and saved identifier range
- some already-processed rows may be revisited depending on where interruption occurred
- this is usually safe because writes use `cp.f_sqlupdatearray()` and section rows are updated by key conditions

So the crawler is designed to be restartable and reasonably idempotent, even if a small overlap happens after a crash or manual stop.

## Planned future work

### Wire main image URL for `episode` and `season` into the V1 Wikidata tables

The `episode` (process 220) and `season` (process 222) processes already download Wikipedia content from `T_WC_TMDB_EPISODE` and `T_WC_TMDB_SEASON` and populate the shared `T_WC_WIKIPEDIA_PAGE_LANG*` tables. The destination tables `T_WC_WIKIDATA_EPISODE_V1` and `T_WC_WIKIDATA_SEASON_V1` both already define a `WIKIPEDIA_POSTER_PATH` column, but `wikipedia_crawler.py` deliberately leaves the `imagetable` / `imagecolumn` fields empty for these two processes today.

A later iteration will:

- set `imagetable` / `imagecolumn` for process 220 to `T_WC_WIKIDATA_EPISODE_V1` / `WIKIPEDIA_POSTER_PATH`
- set `imagetable` / `imagecolumn` for process 222 to `T_WC_WIKIDATA_SEASON_V1` / `WIKIPEDIA_POSTER_PATH`
- backfill `WIKIPEDIA_POSTER_PATH` for episodes and seasons already crawled, since the main image URL is currently discarded for those families

Until that change is made, `T_WC_WIKIDATA_EPISODE_V1` and `T_WC_WIKIDATA_SEASON_V1` are intentionally **not written to** by this crawler.

## External services used

The crawler depends on:

- Wikidata Action API (`wbgetentities`) for sitelinks
- Wikipedia MediaWiki Action API (`parse`) for rendered page HTML and caption scraping
- Wikipedia MediaWiki Action API (`query` with `prop=images` and `prop=imageinfo`) for enumerating every image on a page and resolving URLs, dimensions, and thumbnails
- Wikipedia REST summary API (`/api/rest_v1/page/summary/{title}`) through `wikipedia_images.py` for lead image URLs

## Environment

The script reads a Wikimedia user agent from an environment variable:

- `WIKIMEDIA_USER_AGENT`

Both `wikipedia_images.py` and `wikipedia_crawler_helpers.py` fall back to a static default string when the variable is unset, so the crawler will still run, but Wikimedia's policy asks for an identifying contact in the UA, so setting this is recommended (especially in containerized deployments where `.env` may not be mounted).

It also depends on database connection utilities and helper functions defined in `citizenphil.py`.

## Docker deployment

Secrets must never be baked into the Docker image. The project follows these rules:

- `.env` is listed in [.dockerignore](.dockerignore) so local environment files are excluded from the build context and cannot end up in image layers, build cache, or pushed registries.
- The [Dockerfile](Dockerfile) never `COPY`s `.env` and never sets secrets via `ENV` lines. Only non-sensitive defaults belong in the image.
- At runtime, secrets are injected from a host-managed env file that lives **outside** the application source tree, using Docker's `--env-file` option in `docker run`. The convention is `/home/debian/docker/wikipedia-crawler/.env` on the deployment host.

The startup script [wikipedia-crawler.sh](wikipedia-crawler.sh) already wires this up:

```bash
docker run -d --rm --network="host" \
  --env-file /home/debian/docker/wikipedia-crawler/.env \
  -v $(pwd):/home/debian/docker/wikipedia-crawler \
  --name wikipedia-crawler wikipedia-crawler-python-app
```

To run an interactive container manually with the same secret-handling, mirror the same flag:

```bash
docker run -it --rm --network="host" \
  --env-file /home/debian/docker/wikipedia-crawler/.env \
  --name my-running-app wikipedia-crawler-python-app
```

The env file on the host should contain the runtime configuration, for example:

```
WIKIMEDIA_USER_AGENT=MovieMatchBot/1.0 (https://www.vaugouin.com/moviematch-en/; philippe@vaugouin.com)
# plus database connection variables read by citizenphil.py
```

## Testing

`test_wikipedia_page_images.py` is a single-entity test harness that mirrors steps 4–6 of the data flow for one Wikidata ID, writing into `T_WC_WIKIPEDIA_PAGE_LANG` and `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE`. It is the recommended way to validate the image pipeline before launching a full crawl.

```bash
python test_wikipedia_page_images.py                  # defaults to Q24815 (Citizen Kane), en + fr, ITEM_TYPE=item
python test_wikipedia_page_images.py Q3481393         # crawl a specific entity
python test_wikipedia_page_images.py Q24815 --lang fr # one language only
python test_wikipedia_page_images.py Q24815 --item-type movie
```

The script prints each image as it is written and finishes with a `SELECT COUNT(*) ... GROUP BY LANG` summary so you can verify rows landed.

## Typical execution result

After a run, the database contains:

- structured English and French Wikipedia sections for matched Wikidata entities
- main image URLs for supported content types
- some additional movie technical metadata extracted from French pages
- updated progress and resume metadata in server variables
