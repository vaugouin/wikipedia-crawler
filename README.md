# wikipedia-crawler

`wikipedia_crawler.py` is a batch enrichment script that reads Wikidata-linked entities from the database, retrieves their English and French Wikipedia pages, extracts structured section content, and stores both the textual content and selected Wikipedia image URLs back into MySQL.

## What the crawler does

The crawler processes multiple content types in sequence:

- `movie`
- `person`
- `item`
- `serie`
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

For `item`, `other`, `list`, `movement`, `collection`, `group`, `death`, `award`, `nomination`, and `topic`, the crawler writes to:

- `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH`

For `character`, the crawler writes to:

- `T_WC_TMDB_CHARACTER.WIKIPEDIA_IMAGE_PATH`

For `tmdbcollection`, `episode`, `keyword`, and `season`, no source-table main image column is currently configured. These processes still populate:

- `T_WC_WIKIPEDIA_PAGE_LANG`
- `T_WC_WIKIPEDIA_PAGE_LANG_IMAGE`
- `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`

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

Each later process excludes entities already owned by earlier families in that order.

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
- if `other`, it resumes directly from `other`
- if `list`, it resumes directly from `list`
- if `movement`, it resumes directly from `movement`
- if `collection`, it resumes directly from `collection`
- if `group`, it resumes directly from `group`
- if `death`, it resumes directly from `death`
- if `award`, it resumes directly from `award`
- if `nomination`, it resumes directly from `nomination`
- if `topic`, it resumes directly from `topic`
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
- `strwikipediacrawlerotherid`
- `strwikipediacrawlerlistid`
- `strwikipediacrawlermovementid`
- `strwikipediacrawlercollectionid`
- `strwikipediacrawlergroupid`
- `strwikipediacrawlerdeathid`
- `strwikipediacrawlerawardid`
- `strwikipediacrawlernominationid`
- `strwikipediacrawlertopicid`
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

## Important detail about resume behavior

The resume mechanism is checkpoint-based, not transactionally exact.

That means:

- it resumes from the saved content family and saved identifier range
- some already-processed rows may be revisited depending on where interruption occurred
- this is usually safe because writes use `cp.f_sqlupdatearray()` and section rows are updated by key conditions

So the crawler is designed to be restartable and reasonably idempotent, even if a small overlap happens after a crash or manual stop.

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
