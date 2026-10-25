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
| 224 | `sourcework`        | `T_WC_T2S_SOURCE_WORK`      | `T_WC_T2S_SOURCE_WORK.WIKIPEDIA_IMAGE_PATH`              |

For every row of every entity family, page metadata, page sections, and page images are written to the shared `T_WC_WIKIPEDIA_PAGE_LANG*` tables regardless of whether an "Image target" is configured. The "Image target" column only controls where the **main** image URL is additionally written back into the entity's own table.

### `sourcework` (224): the "based on" sources

Added for WIKIPEDIA-CRAWLER-023. `T_WC_T2S_SOURCE_WORK` is built by process 73 of `tmdb-movie-preprocess` (Wikidata P144: the novel, manga, play, game or film a film or series is based on). Until this family existed nothing selected those rows by themselves, so a source no earlier family happened to own had neither long text nor main image.

Row filter (`SOURCEWORK_ROW_FILTER` in [wikipedia_queries.py](wikipedia_queries.py)), applied in a full crawl **and** under `--no-exclusions`:

- `DELETED = 0`: process 73 keeps the sources that left the perimeter as `DELETED = 1`; they are not served, so not crawled.
- a source of type `screen` that has an `ID_MOVIE` or `ID_SERIE` is skipped: its Wikipedia page is the page of that T2S film or series, already crawled by `201 movie` / `204 serie`, and the source-work sheet points to that sheet. A `screen` source without a T2S sheet stays in, since nothing else would crawl it.

It runs last and applies the whole exclusion chain (everything `season` excludes, plus `T_WC_TMDB_SEASON`), so in a full crawl it only owns the sources no other family reaches. That loses nothing: whoever crawls a source writes its sections and its per-language lead image under the same `ID_WIKIDATA`, in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION` and `T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL`. Process 71 of `tmdb-movie-preprocess` copies that `MAIN_IMAGE_URL` into `T_WC_T2S_SOURCE_WORK.WIKIPEDIA_MAIN_IMAGE_URL` / `_FR` (it already lists `SOURCE_WORK` among its entities), and the fiche reads the sections by `(ID_WIKIDATA, LANG)`. To refresh every source work regardless of ownership, for instance after the first build of the table:

```bash
./wikipedia-crawler-manual.sh --content-all sourcework --no-exclusions
./wikipedia-crawler-manual.sh Q129778 --item-type sourcework   # one work: Heart of Darkness
```

Coverage before and after a pass, and the three reference works (Heart of Darkness, Dracula, Nineteen Eighty-Four): [doc/sql/sourcework-wikipedia-coverage.sql](doc/sql/sourcework-wikipedia-coverage.sql), read-only.

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
- `sourcework`

For each content type, it:

1. Selects records that already have a valid `ID_WIKIDATA`.
2. Resolves the Wikipedia page title for each target language (`en` and `fr`) using the Wikidata API.
3. Retrieves the main image URL from Wikipedia when an image destination table/column is configured for that content type.
4. Downloads parsed Wikipedia page HTML through the MediaWiki `parse` API.
5. Converts the page with `wikipedia_content_md.convert_page()`: each section as Markdown (`CONTENT_MD`) and as plain text repaired from it (`CONTENT`), plus the page's internal links and footnotes.
6. Stores the sections in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`, the links (resolved to a Qid) in `T_WC_WIKIPEDIA_PAGE_LANG_LINK` and the footnotes in `T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE`.
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

### `wikipedia_content_md.py`
Since 1.1.0 (topic `wikipedia-rich-sections` in the Nestor t2s-backlog, decisions of 2026-10-15). Turns the `action=parse` HTML into sections, links and references, network-free and database-free except its two resolvers:

- `convert_page(soup, lang)`: the same H2/H3 split as `extract_titles_and_text()` (WIKIPEDIA-CRAWLER-016), walking top-level blocks only. For each section: the Markdown (links by Wikipedia title, `[words](Target_title)`; bold, italic, nested lists, tables, definition lists kept; footnote call-outs, edit links, navboxes, galleries and portal banners dropped) and the plain text derived from it by `md_to_text()` (tables as plain rows, `[1]` call-outs gone, nested lists written once, `- ` bullets kept for the Fiche technique Format line). Reads both HTML shapes: the legacy parser output and Parsoid's (`<section>` wrappers, `./Title` links), which `action=parse` served on 2026-10-15 from 10:00 to 19:00 Paris. A section that renders empty is dropped (an end-matter "Références" holding only the reference list, for instance: its footnotes now live in the reference table).
- Date links: French Wikipedia links every date part (`[20](20_octobre) [octobre](Octobre_2009) [2009](2009_à_la_télévision)`, 5 % of all links on the prototype pages, 45 % on a season page). They are kept and flagged `IS_DATE`: inside the `{{date}}` template's `.date-lien`, or a calendar title (`20 octobre`, `Octobre 2009`, `2009`, `1966 au cinéma`; four-digit years only, so `Mai 68` stays an event).
- `resolve_link_qids(cursor, lang, links)`: runs on the main thread. First a local title join on `T_WC_WIKIPEDIA_PAGE_LANG` (index `LANG_WIKIPEDIA_PAGE_TITLE`), filtered in Python on the exact title because the column's collation ignores case and accents, a title held by two Qids being left to the API. Then the API answers for the rest (`action=query&prop=pageprops`, 50 titles per call, redirects followed, results cached in memory). Since 1.1.1 those API calls run in the fetch worker, for every live title of the page (`api_titles`), and the main thread only reads their answers: in 1.1.0 they ran on the main thread, which does every database write, and the crawl fell from 53 to 38 pages a minute. Red links are never resolved, date links only by the title join.

HTML comments are not text (1.1.3). From 1.1.0 to 1.1.2 the converter wrote out the `NewPP limit report` comment that `action=parse` appends to every page, so the last section of each page converted in that window ends with the parser report, in `CONTENT_MD` and `CONTENT` alike (`doc/sql/check-parser-report-leak.sql` measures it).

Three fixes from the comparison with Wikimedia Enterprise snapshot records (1.1.5, WIKIPEDIA-CRAWLER-024). A link to the page itself (`./Page#Season_1` in Parsoid HTML) is plain text with no link row, as the legacy HTML already gave; the writer passes `parse.title`, and without it the title is read from the Parsoid `<head>`. Text that a template puts straight into a `<div>` is one line again: each text node, italic or spacing `<span>` used to start a line of its own ("1re" / "partie de la trilogie" / "*Les Lavandes*"), in the legacy HTML as in Parsoid, and a `<br>` still ends the line. A section holding only zero-width characters is empty. On four series pages fetched from Wikimedia Enterprise, the links then match the live crawl exactly (2,883 of 2,883) and every remaining Markdown difference is one of those joined lines.

If the converter ever raises on a page, the writer falls back to `extract_titles_and_text()` for that page, with `CONTENT_MD` NULL and no links or references.

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
Shared per-entity fetch + persist pipeline used by both the full crawler and the on-demand entry points:

- `f_fetchlangpayload(...)` — network + HTML parsing for one `(entity, language)` page (thread-safe, no DB work).
- `f_writelangtodb(...)`: persists one payload (page metadata, images, sections with `CONTENT` and `CONTENT_MD`, links, references, main-image and movie-Format writebacks). Its `blnwritecounters` flag lets the single-Qid path skip monitoring-counter writes.
- `f_writelinksandrefs(...)`: the link and reference rows of one page, keyed like its sections: upsert on `(ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER)`, then a prune of the rows this crawl no longer produced. A failure there is logged and never costs the page its sections.
- `CONTENT_CONFIG` — per-content `id` / `imagetable` / `imagecolumn` / source-table mapping mirroring `arrprocesses` in `wikipedia_crawler.py`.

Living here (rather than in `wikipedia_crawler.py`, whose body runs a full crawl at import time) lets `wikipedia_functions.py` reuse this logic without launching a crawl.

### `wikipedia_queries.py`
Per-family row-selection SQL: one `build_<family>_sql(strresumeid)` builder per entity family (source table, the exclusion chain that stops an entity already owned by an earlier family from being crawled twice, and an optional `>= resume id` bound), plus `CONTENT_SQL_BUILDERS` mapping each content family to its builder.

These builders used to live in `wikipedia_crawler.py`. They live here so the on-demand entry point can run a whole family with the **same** query — exclusion chain included — without importing the crawler module, whose body launches a full crawl at import time. They are pure string functions: no database, no network, no import-time work.

### `wikipedia_functions.py`
On-demand entry points, analogous to the `tmdb_functions` one-shot helpers:

- `f_wikipediaqidtosqleverything(strwikidataid, strcontent="item", arrlanguages=("en", "fr"))` — fully refreshes the Wikipedia data for one Wikidata entity.
- `f_wikipediacontenttosqleverything(strcontent, arrlanguages=("en", "fr"), strresumeid="", lnglimit=0, blnnoexclusions=False)` — refreshes **every** entity of one family, with the same concurrent fetch fan-out as the crawler. `blnnoexclusions=True` crawls the family's whole source table, which is the only way to refresh a family the exclusion chain has absorbed.

Both are parallel-safe with a running crawler. See [Refreshing content on demand in a parallel container](#refreshing-content-on-demand-in-a-parallel-container).

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
10. `wikipedia_content_md.convert_page()` converts the page into `(display_order, title, content, content_md)` sections, links and references.
11. Each section is inserted or updated in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`.
12. Any old sections beyond the latest `DISPLAY_ORDER` are deleted for that Wikidata ID and language.
13. The links are resolved to a Qid (local title join, then the API), and links and references are upserted into `T_WC_WIKIPEDIA_PAGE_LANG_LINK` and `T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE`, stale rows pruned.

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
- `sourcework` -> `T_WC_T2S_SOURCE_WORK.WIKIPEDIA_IMAGE_PATH` (the serving columns `WIKIPEDIA_MAIN_IMAGE_URL` / `_FR` are filled by process 71 of `tmdb-movie-preprocess` from `T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL`)

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
- `CONTENT` (since 1.1.0 the plain text repaired from the Markdown)
- `CONTENT_MD` (since 1.1.0; NULL on rows not yet re-crawled, and on a page where the converter failed)

Both text columns are `COMPRESSED` (zlib, about 2.3 times on this text). Since 1.1.0, two tables are keyed like the sections, `(ID_WIKIDATA, LANG, SECTION_DISPLAY_ORDER, DISPLAY_ORDER)`:

- `T_WC_WIKIPEDIA_PAGE_LANG_LINK`: one row per internal link: `ANCHOR`, `TARGET_TITLE`, `FRAGMENT`, `TARGET_FINAL_TITLE` (after redirect), `VIA_REDIRECT`, `IS_REDLINK`, `IS_DATE`, `TARGET_ID_WIKIDATA` and `RESOLVED_BY` (`title` for the local join, `api`, NULL when unresolved). Read backwards by tmdb-front and fastapi-text2sql ("mentioned in": which pages link to an entity) on the covering index `TARGET_LANG_SOURCE` (`TARGET_ID_WIKIDATA`, `LANG`, `ID_WIKIDATA`), added by `doc/sql/alter-link-target-lang-source-index.sql`.
- `T_WC_WIKIPEDIA_PAGE_LANG_REFERENCE`: one row per footnote cited in a section: `NOTE_KEY` (`cite_note-...`), `LABEL` (`[12]`), `URL`, `TITLE`, `REFERENCE_TEXT` (compressed). 43 % of references have no URL.

The backfill is tracked by the `wikipedia-content-md-backfill` report of `data-monitoring`.

### Image URL enrichment
When configured, the main image URL is written back to a content-specific Wikidata table.

#### UI-chrome filtering (which image is allowed to be the main image)

Wikipedia articles embed template decoration that is not the subject: edit pencils, maintenance banners, project logos, stub markers, featured-article stars. The crawler filters these out with `_UI_CHROME_PATTERNS` / `_is_ui_chrome_file` in `wikipedia_images.py`, at three points:

1. **Page-image enumeration** : chrome `File:` titles are dropped before the `imageinfo` step, so they never reach `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE` (and it saves an API call per 50 files filtered).
2. **Lead image** : the REST summary endpoint occasionally returns a maintenance banner as `originalimage`. `is_acceptable_main_image_url()` rejects it, and the column is then left untouched.

#### Where the main image is stored

Since WIKIPEDIA-CRAWLER-020 the resolved lead image is written to **`T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL`**, beside the page it belongs to, in addition to the legacy `T_WC_WIKIDATA_*_V1` column kept for the transition.

This closes the one gap that blocked the V1 decommission (WIKIDATA-CRAWLER-015): the V2 model carries no image column at all, and should not, because a Wikipedia lead image is Wikipedia data rather than a Wikidata statement. `T_WC_WIKIPEDIA_PAGE_LANG` already holds the title, URL, HTTP status and crawl dates of that same page.

It also fixes something the V1 column could not express. V1 had **one** image column per entity while the crawler runs **once per language**, so the second language silently overwrote the first: that is how collection 4845 lost its English lead image (the Man-with-No-Name blu-ray cover) to a French decade-portal banner. Keyed on `(ID_WIKIDATA, LANG)`, the new column stores the main image **per language**, so consumers gain a localized main image they never had.

Schema change and optional backfill: [`migrations/add_main_image_url_to_page_lang.py`](migrations/add_main_image_url_to_page_lang.py). The backfill only copies images already flagged `IS_MAIN_IMAGE = 1` in the gallery, so it never invents one.

#### No first-page-image fallback (removed)

The crawler used to fall back to "the first image on the article" when a page had no lead image. That fallback is **gone**, and this is deliberate: on a Wikipedia article the top of the page belongs to the templates, not to the subject. It produced the great majority of the 1.25M UI-chrome images cleared by [`migrations/clear_ui_chrome_images.py`](migrations/clear_ui_chrome_images.py), `Blue_pencil.svg` alone accounting for 33 734 of the 38 136 wrong movie posters.

Filtering the chrome out was not enough. On frwiki the decade-portal banner carries `Apollo_11_Crew.jpg`, a genuine photograph correctly named, which no filename filter can tell from a real illustration: that is how two Sergio Leone trilogies ended up illustrated with the Apollo 11 crew. Filtering only turned visible garbage (an edit pencil) into plausible garbage (astronauts on a spaghetti western), which is worse, because a wrong image that looks right is read as fact.

**Accepted consequence:** an entity whose Wikipedia page has no lead image now keeps **no main image at all**. An empty illustration is honest; a guess is not. Coverage is therefore lower than before, by design.

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
- `224` -> `sourcework`

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
- `sourcework`

Each later process excludes entities already owned by earlier families in that order. Note that `wikidatacharacter` (process 205, sourced from `T_WC_WIKIDATA_CHARACTER_V1`) and `character` (process 218, sourced from `T_WC_TMDB_CHARACTER`) are two distinct processes operating on character entities from different upstream tables. The Wikidata-native source runs first; the TMDB-sourced process then excludes any IDs already covered by it.

Examples:

- `serie` excludes IDs already covered by `movie`, `person`, and `item`
- `list` excludes IDs already covered by `movie`, `person`, `item`, `serie`, and `other`
- `movement` excludes everything excluded by `list`, plus entities already covered by `list`
- `topic` excludes all earlier families in the chain
- `season` excludes all earlier families plus `character`, `tmdbcollection`, `episode`, and `keyword`
- `sourcework` excludes everything `season` excludes, plus `season`; it also applies its own row filter (`DELETED`, and `screen` sources that have a T2S film or series sheet)

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
- if `sourcework`, it resumes directly from `sourcework`

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
- `strwikipediacrawlersourceworkid`

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

The list is ordered by **how much work the family actually has**, then by staleness — the stalest *productive* content first.

That first criterion was missing until 2026-08-11, and it inverted the list. The order was read off `SELECT MAX(TIM_UPDATED), ITEM_TYPE FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION GROUP BY ITEM_TYPE`, stalest first, with the `ITEM_TYPE`s absent from that table put at the head as "never crawled, most urgent". But absence from that table has **two** causes, and only one of them means stale:

- the family has never been crawled — genuinely urgent;
- the family **selects no rows at all**, because the exclusion chain empties it. It writes nothing, so it appears nowhere, forever.

The second case is the common one. Process `203 item` crawls the whole of `T_WC_WIKIDATA_ITEM_V1` and runs before every `T_WC_T2S_*` family, so any award, group, death or collection that is also a Wikidata item is crawled **as an `item`** and its own family sees an empty result set. Measured on the 31 000 Qids of `T_WC_T2S_AWARD`: 16 258 of their Wikipedia pages are stored under `ITEM_TYPE = 'item'`, 7 010 under `movie`, 6 881 under `person`, 925 under `serie`, 2 under `list` — and **zero** under `award`. Running `215 award` today selects 0 rows.

So the five families the list promoted to the head as "most urgent" were in fact the five with nothing to do, and the sweep opened with five no-ops. They stay in the list — they cost about a second each, and they are the tripwire for the day an entity appears that no earlier family owns — but they now run last.

Row counts below were measured on 2026-08-11 with `--content-all <family>`, which prints `N row(s) to process` before doing any work; that makes an empty family free to verify. The `MAX(TIM_UPDATED)` column is the previous measurement, kept because it still orders the productive families correctly.

Current order — productive families first, stalest first:

| Position | ID  | Content          | Rows selected | MAX(TIM_UPDATED) |
|----------|-----|------------------|---------------|------------------|
| 1        | 209 | `other`          | ≤ 1 (one hard-coded Qid) | 2026-04-14 |
| 2        | 203 | `item`           | not measured (large) | 2026-05-08 |
| 3        | 204 | `serie`          | not measured (large) | 2026-05-09 |
| 4        | 201 | `movie`          | not measured (large) | 2026-05-18 |
| 5        | 205 | `character` (Wikidata) | not measured   | 2026-05-20 |
| 6        | 218 | `character` (TMDB)     | not measured   | 2026-05-20 |
| 7        | 223 | `technical`      | **28**        | 2026-05-20       |
| 8        | 202 | `person`         | not measured (large) | 2026-05-23 |
| 9        | 220 | `episode`        | not measured (large) | 2026-05-24 |
| 10       | 222 | `season`         | not measured (large) | 2026-05-24 |

Then the families the exclusion chain has absorbed, or is expected to absorb:

| Position | ID  | Content          | Rows selected | Note |
|----------|-----|------------------|---------------|------|
| 11       | 211 | `movement`       | not measured  | same shape as the absorbed families below |
| 12       | 216 | `nomination`     | not measured  | same shape |
| 13       | 217 | `topic`          | not measured  | same shape |
| 14       | 219 | `tmdbcollection` | not measured  | listed as "never updated" before |
| 15       | 221 | `keyword`        | not measured  | listed as "never updated" before |
| 16       | 210 | `list`           | **1** (no Wikipedia page) | effectively absorbed |
| 17       | 212 | `collection`     | **0**         | fully absorbed |
| 18       | 213 | `group`          | **0**         | fully absorbed |
| 19       | 214 | `death`          | **0**         | fully absorbed |
| 20       | 215 | `award`          | **0**         | fully absorbed |
| 21       | 224 | `sourcework`     | not measured  | added 2026-10-11; partly absorbed by `201 movie` and `203 item` |

Two consequences of absorption are worth knowing before reading these tables as "content is missing". The content is **not** missing: it is stored under the `ITEM_TYPE` of the family that actually crawled it, so a consumer filtering on `ITEM_TYPE = 'award'` finds nothing while the pages sit there under `item`. And the family's own main-image column is only ever written for the rows it selects, so `T_WC_T2S_TECHNICAL.WIKIPEDIA_IMAGE_PATH` was filled for those 28 technical entities and stays empty for any technical entity that is also an item — that one's image went to `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`.

To refresh an absorbed family on demand anyway, bypass the chain with [`--no-exclusions`](#refreshing-an-absorbed-family---no-exclusions).

When `intquickmode = False`, the regular `arrprocessscope = arrprocesses[resume_index:]` path applies and the [Resume mechanism](#resume-mechanism) above governs ordering and checkpointing.

## Refreshing content on demand in a parallel container

Quick mode parallelizes *whole processes* inside the always-on container. When you instead want to refresh something **on demand** — one specific Wikidata entity, or one whole family such as `list` or `technical` — while the main `wikipedia-crawler` container keeps running long jobs like `202 person`, use the entry points in [wikipedia_functions.py](wikipedia_functions.py). This mirrors the `tmdb-crawler` pattern of dropping into an interactive Python container and calling a one-shot function (`tf.f_tmdbmovietosqleverything(13860)`).

### One Qid

```python
import wikipedia_functions as wf

wf.f_wikipediaqidtosqleverything("Q24815")                       # item (default type)
wf.f_wikipediaqidtosqleverything("Q25188", strcontent="movie")   # a movie
wf.f_wikipediaqidtosqleverything("Q42",    strcontent="person")  # a person
wf.f_wikipediaqidtosqleverything("Q1234",  strcontent="serie", arrlanguages=("fr",))  # French only
```

`f_wikipediaqidtosqleverything(strwikidataid, strcontent="item", arrlanguages=("en", "fr"))` runs the exact per-entity pipeline the full crawler runs for one row: it resolves the `en`/`fr` page titles from Wikidata, then writes page metadata to `T_WC_WIKIPEDIA_PAGE_LANG`, every page image to `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE`, and structured sections to `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`, plus the content-specific main-image writeback and (for `strcontent="movie"`) the French `Fiche technique` Format line. `strcontent` must be one of the entity families listed in [Entities crawled](#entities-crawled) (`movie`, `person`, `item`, `serie`, `wikidatacharacter`, `other`, `list`, `movement`, `collection`, `group`, `death`, `award`, `nomination`, `topic`, `technical`, `character`, `tmdbcollection`, `episode`, `keyword`, `season`, `sourcework`); it selects the main-image destination column and enables movie-only enrichment.

### One whole family (`--content-all`)

```python
wf.f_wikipediacontenttosqleverything("technical")                      # every technical entity
wf.f_wikipediacontenttosqleverything("list", lnglimit=20)              # trial run, first 20
wf.f_wikipediacontenttosqleverything("list", strresumeid="Q123456")    # continue after a stop
wf.f_wikipediacontenttosqleverything("list", arrlanguages=("fr",))     # French only
wf.f_wikipediacontenttosqleverything("award", blnnoexclusions=True)    # absorbed family, see below
```

`f_wikipediacontenttosqleverything(strcontent, arrlanguages=("en", "fr"), strresumeid="", lnglimit=0, blnnoexclusions=False)` selects the family's rows with the family's **own** query from [wikipedia_queries.py](wikipedia_queries.py) — the same source table and the same exclusion chain the full crawler uses, so an entity already owned by an earlier family is not crawled twice — then runs the same pipeline as the crawler: titles resolved 50 Qids per `wbgetentities` call, `(entity, language)` pages fetched concurrently by a thread pool sized by `WIKIPEDIA_CRAWLER_WORKERS`, all database writes performed on the calling thread.

Because it writes no checkpoint (see below), **resuming is manual**: the last processed id is printed when the run ends or is interrupted, and passed back through `strresumeid` / `--resume-from`. The bound is inclusive, so the last entity is refreshed again — harmless, every write is an idempotent keyed upsert.

### Refreshing an absorbed family (`--no-exclusions`)

Most `T_WC_T2S_*` families select **0 rows** in a normal run, and that is not a bug: process `203 item` crawls the whole of `T_WC_WIKIDATA_ITEM_V1` and runs first, so an award that is also a Wikidata item is crawled *as an item* and `215 award` legitimately has nothing left to do (see [Quick mode](#quick-mode) for the measurements). The exclusion chain answers "which family **owns** this entity in a full crawl" — the right question for a full crawl, the wrong one for "refresh the awards now".

`blnnoexclusions=True` / `--no-exclusions` drops the chain and crawls the family's whole source table:

```bash
./wikipedia-crawler-manual.sh --content-all award --no-exclusions
./wikipedia-crawler-manual.sh --content-all death --no-exclusions --limit 50
```

The query is built from `CONTENT_CONFIG`'s `sourcetable` / `idcolumn`, with the `^Q[0-9]+$` guard applied to every family (including those whose own builder omits it), so a malformed id never reaches the Wikidata API. A family's own row filter (`CONTENT_SOURCE_FILTERS` in [wikipedia_queries.py](wikipedia_queries.py), today only `sourcework`) is kept: it is not ownership but what the family refuses to crawl at all. `--resume-from` and `--limit` work as usual. `other` has no source table (a single hard-coded Qid), so the flag prints a notice and changes nothing there.

Two effects to be aware of, neither harmful:

- The rows are written with **this family's `ITEM_TYPE`** (`award` rather than `item`), because `ITEM_TYPE` records the process that did the crawl. `T_WC_WIKIPEDIA_PAGE_LANG` is keyed on `(ID_WIKIDATA, LANG)` with no `ITEM_TYPE` in the key, so the column is last-writer-wins by design and the next full crawl of `203 item` sets it back. No consumer filters on it: `fastapi-text2sql` and `tmdb-front` both select on `(ID_WIKIDATA, LANG)` alone.
- The main image goes to the destination configured for **this** family. For the `T_WC_T2S_*` families that destination is `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`, which is where a full crawl would have put it anyway.

### Why both are parallel-safe

The per-entity fetch/persist code is shared with the main crawler (extracted into [wikipedia_page_writer.py](wikipedia_page_writer.py)), but these paths write **no** resume-state or monitoring server variables (they call the shared writer with `blnwritecounters=False`). They therefore cannot advance or corrupt the main container's `strwikipediacrawler*` checkpoints. All page/section/image writes are keyed upserts on `(ID_WIKIDATA, LANG, DISPLAY_ORDER)`, so a refresh is idempotent. Run it against entities the main crawler is not simultaneously processing.

One shared resource is worth a thought: the Wikimedia rate limiter (`WIKIPEDIA_CRAWLER_MAX_RPS`, default 20) is **per process**, so two containers at the default add up to ~40 rps. Pass a lower cap to the manual container to stay polite — see below.

### Running it in a second container

Use the helper [wikipedia-crawler-manual.sh](wikipedia-crawler-manual.sh), which builds the same image and starts a container under a **different** name (`wikipedia-crawler-manual`) so it coexists with the always-on `wikipedia-crawler` container. It mounts the working tree and reads the same host-managed `--env-file`, exactly like the main launcher. It forwards `WIKIPEDIA_CRAWLER_MAX_RPS` and `WIKIPEDIA_CRAWLER_WORKERS` from the host environment when set, and honors `CONTAINER_NAME` so two families can run at once.

Interactive REPL:

```bash
./wikipedia-crawler-manual.sh
>>> import wikipedia_functions as wf
>>> wf.f_wikipediaqidtosqleverything("Q25188", strcontent="movie")
>>> wf.f_wikipediacontenttosqleverything("technical")
```

One-shot (run and exit):

```bash
./wikipedia-crawler-manual.sh Q25188 --item-type movie
./wikipedia-crawler-manual.sh Q24815              # defaults to item, en + fr
./wikipedia-crawler-manual.sh Q24815 --lang fr    # French only

WIKIPEDIA_CRAWLER_MAX_RPS=5 ./wikipedia-crawler-manual.sh --content-all list
WIKIPEDIA_CRAWLER_MAX_RPS=5 ./wikipedia-crawler-manual.sh --content-all technical
WIKIPEDIA_CRAWLER_MAX_RPS=5 ./wikipedia-crawler-manual.sh --content-all list --limit 20
WIKIPEDIA_CRAWLER_MAX_RPS=5 ./wikipedia-crawler-manual.sh --content-all list --resume-from Q123456
```

Or by hand, reproducing the tmdb-crawler workflow (`docker run -it ... python`):

```bash
docker build -t wikipedia-crawler-python-app .
docker run -it --rm --network="host" \
  --env-file /home/debian/docker/wikipedia-crawler/.env \
  -e WIKIPEDIA_CRAWLER_MAX_RPS=5 \
  -v $(pwd):/home/debian/docker/wikipedia-crawler \
  --name wikipedia-crawler-manual wikipedia-crawler-python-app \
  python wikipedia_functions.py --content-all technical
```

Unlike [test_wikipedia_page_images.py](test_wikipedia_page_images.py) (which only mirrors steps 4–6, the image pipeline), both functions run the **full** per-entity data flow including section extraction and content-specific enrichment.

## Backfill from the Wikimedia Enterprise snapshots (prototype, 1.1.6)

WIKIPEDIA-CRAWLER-024. The live crawl reads one page per `action=parse` call and Wikipedia's rate limits hold it back. The free Wikimedia Enterprise account (`WME_USERNAME` / `WME_PASSWORD` in `.env`) gives the whole of `frwiki` and `enwiki` namespace 0 once a month, as `.tar.gz` chunks of NDJSON records: 150 chunks for frwiki, 435 for enwiki, 1,500 chunk downloads a month on the free tier. Each record carries the page's Qid, title, redirects and Parsoid HTML.

`wikipedia_dump_ingest.py` streams the chunks (nothing is written to disk), keeps the records whose Qid a crawler family selects (the `wikipedia_queries` builders, exclusion chains included), converts them with `convert_page` and, with `--write`, persists them with `f_writelangtodb`: sections, `CONTENT_MD`, links, references, the French movie format line. What differs from a live crawl:

- images are left as they are (the payload sets `keep_images`): a record carries no image list, and `MAIN_IMAGE_URL` is not touched;
- links are resolved by the local title join only, never by the Wikipedia API; `--title-map DIR` writes the snapshot's own title to Qid map (redirects flagged) for a later resolution pass;
- `LAST_CRAWLED_AT` and `LAST_SUCCESS_AT` take the time of the ingest, `HTTP_STATUS` 200;
- a page the live crawl stored in Markdown after the record's revision (`LAST_SUCCESS_AT` at or after the record's `date_modified`) is skipped (1.1.8, `pages_db_fresher` in the report; a dry run still compares it): the snapshot is a monthly cut and would put an older revision back.

Without `--write` it is a dry run: it reads the database and compares each converted page with what the live crawl stored (sections identical, identical once whitespace is ignored, links), and counts the pages that have no section yet. Stop the main crawler before a `--write` run.

```bash
./wikipedia-dump-ingest.sh --lang fr --chunks 0                 # dry run, one chunk
./wikipedia-dump-ingest.sh --lang fr --chunks 0 --write         # main crawler stopped
./wikipedia-dump-ingest.sh --lang fr --chunks 0-149 --write --title-map dumpdata
```

The report is printed and kept in `dumpdata/ingest-<date>-<time>.log` (ignored by git). Since 1.1.9 it holds one summary line per chunk, the total, and the errors only: what `f_writelangtodb` prints for each page (section titles, format line) is dropped unless it is an error line (`writer_error_lines` in the report; since 1.1.10 "error" must be a whole word, so "terrorisme" in a section title no longer counts).

Since 1.1.11 each chunk runs in its own forked process, which gives its memory back when it exits. On 2026-10-18 a single process reached 21 GB after 31 chunks and the kernel's OOM killer stopped the run in chunk 107 (`dmesg -T | grep -i oom`). A chunk whose process dies is reported as `worker died without a report (exit code ...)`, counted in `chunk_errors`, its title map left as `.part`, and the run goes on with the next chunk. Each chunk line and the total show `max_rss_mb`, the peak memory of a chunk process.

### Automatic monthly ingest (1.1.12, WIKIPEDIA-CRAWLER-025)

`run-if-new-snapshot.sh`, in cron once a day, does the whole thing without a command line per batch, on the model of `wikidata-crawler/run-if-new-dump.sh`:

1. a `flock`, and an immediate exit while the `wikipedia-dump-ingest` container runs;
2. `wikipedia_dump_ingest.py --check` in a throwaway container: exit 0 when a snapshot waits (a new one, or a run left unfinished), 1 when every snapshot is in, 2 when it cannot tell (any failure reads 2, never 1);
3. on 0, it writes `dumpdata/INGEST_IN_PROGRESS`, stops the `wikipedia-crawler` container and starts `wikipedia_dump_ingest.py --auto --title-map dumpdata` with `docker run -d`. No `nohup`, no terminal: follow it with `docker logs -f wikipedia-dump-ingest` or `dumpdata/ingest-auto-<date>-<time>.log`.

`--auto` ingests, for every language of `WME_INGEST_LANGS` (default `fr,en`), the latest snapshot not yet ingested whole, chunk by chunk, with `--write`. The state lives in `T_WC_SERVER_VARIABLE`:

| Variable | Holds |
|---|---|
| `strwikipediadumpingest<lang>snapshotdone` | `date_modified` of the last snapshot ingested whole |
| `strwikipediadumpingest<lang>snapshotcurrent` | `date_modified` of the snapshot being ingested |
| `strwikipediadumpingest<lang>chunksdone` | its chunks done without error, as ranges (`0-74,76`) |
| `strwikipediadumpingest<lang>startdatetime`, `...enddatetime` | start and end of that snapshot's ingest (Paris time) |
| `strwikipediadumpingeststatus` | the chunk in progress, or `idle` |

**Disk guard (1.1.15).** Before each chunk, `--auto` and `--write` runs check the free space of the filesystem that holds this directory, which is also the database's on the VPS. Below `WME_INGEST_MIN_FREE_GB` (default 40) no new chunk starts: the run ends as incomplete, the status reads `stopped by the disk guard`, and the next check resumes at the first chunk not done. The chunks themselves are streamed and never stored; what grows is the database. On 2026-10-19/20 the ingest of 515 chunks wrote about 70 GB of Wikipedia tables and pushed the InnoDB undo files to 42 GB, the disk filled up at enwiki chunk 365 and MariaDB crashed with a corrupted page. Measured on enwiki chunks 365 to 370 (`doc/sql/check-table-sizes-20261020.txt` for the baseline): about 0.16 GB of tables and 400 s per chunk. The undo growth was not the ingest's own doing: an idle read transaction held by the API since its startup blocked the InnoDB purge (FASTAPI-TEXT2SQL-336); once it was killed, the undo files went back to 10 MB and about 43 GB came back. The default of 40 GB is kept as the final threshold (2026-10-21, WIKIPEDIA-CRAWLER-026): at 0.16 GB a chunk, a full enwiki snapshot written from scratch costs about 70 GB, so the guard leaves room for a day of the other writers and for a secondary-index build, not for a full rebuild of the link table (about as much free disk as the table itself, to plan on its own). Two indexes of `T_WC_WIKIPEDIA_PAGE_LANG_LINK` that no query reads, `LANG_TARGET_TITLE` and `ITEM_TYPE`, are dropped by `doc/sql/archive/alter-link-drop-unused-indexes-026.sql` (1.1.17).

A chunk in error is retried once at the end of its language. A run that dies resumes at the next daily check with the chunks not done, and the crawler stays stopped meanwhile. When every language is whole, `--auto` deletes `dumpdata/INGEST_IN_PROGRESS` and the crawler's own cron starts the crawler again: `wikipedia-crawler.sh` refuses to start it while the flag exists or a `wikipedia-dump-ingest` container runs.

`--check` calls the Enterprise API only when a language's last whole snapshot is older than the current month: snapshots come out on the 1st, so the API is asked a few times a month at most. Snapshots ingested by hand-run batches are recorded with `./wikipedia-dump-ingest.sh --mark-complete fr`. The title maps keep one file per chunk name, so a new snapshot overwrites the previous month's.

```bash
./run-if-new-snapshot.sh --dry-run          # what it would do
# crontab -e:
41 6 * * * /home/debian/docker/wikipedia-crawler/run-if-new-snapshot.sh >> /home/debian/docker/wikipedia-crawler/dumpdata/run-if-new-snapshot.log 2>&1
```

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

`test_wikipedia_content_md.py` runs offline (no network, no database): sections, Markdown, repaired text, date and red links, references, the title-join-then-API resolution and the writer's link/reference rows, on small pages in both HTML shapes.

```bash
python test_wikipedia_content_md.py
```

## Typical execution result

After a run, the database contains:

- structured English and French Wikipedia sections for matched Wikidata entities
- main image URLs for supported content types
- some additional movie technical metadata extracted from French pages
- updated progress and resume metadata in server variables
