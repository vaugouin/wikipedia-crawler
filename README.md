# wikipedia-crawler

`wikipedia_crawler.py` is a batch enrichment script that reads Wikidata-linked entities from the database, retrieves their English and French Wikipedia pages, extracts structured section content, and stores both the textual content and selected Wikipedia image URLs back into MySQL.

## What the crawler does

The crawler processes multiple content types in sequence:

- `movie`
- `person`
- `item`
- `serie`
- `other`

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
Used to retrieve the Wikipedia main image URL for a page title and language.

## Data flow

For each selected row:

1. The crawler reads `ID_WIKIDATA` from the source table.
2. It calls `get_linked_pages(wikidata_id, 'sitelinks', language)`.
3. It looks up the page title in `entities[ID_WIKIDATA]['sitelinks'][<lang>wiki]['title']`.
4. If configured for that content type, it calls:

   - `wikipedia_images.get_wikipedia_main_image_url(page_title, language)`

5. If an image URL is found, it stores it in the configured destination table/column using `cp.f_sqlupdatearray()`.
6. It then calls the Wikipedia `parse` API to get rendered HTML.
7. The HTML is normalized by wrapping it in a `<body>` tag.
8. `extract_titles_and_text()` converts the page into `(title, content)` pairs.
9. Each section is inserted or updated in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION`.
10. Any old sections beyond the latest `DISPLAY_ORDER` are deleted for that Wikidata ID and language.

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

## Database writes performed by the crawler

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

Each later process excludes entities already owned by earlier families in that order.

Examples:

- `serie` excludes IDs already covered by `movie`, `person`, and `item`
- `list` excludes IDs already covered by `movie`, `person`, `item`, `serie`, and `other`
- `movement` excludes everything excluded by `list`, plus entities already covered by `list`
- `topic` excludes all earlier families in the chain

This keeps image enrichment targeted at `T_WC_WIKIDATA_ITEM_V1.WIKIPEDIA_IMAGE_PATH` while still preventing duplicate crawling across the full run.

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
- Wikipedia MediaWiki API (`parse`) for rendered page HTML
- Wikipedia REST summary API through `wikipedia_images.py` for lead image URLs

## Environment

The script expects a Wikimedia user agent in environment variables:

- `WIKIMEDIA_USER_AGENT`

It also depends on database connection utilities and helper functions defined in `citizenphil.py`.

## Typical execution result

After a run, the database contains:

- structured English and French Wikipedia sections for matched Wikidata entities
- main image URLs for supported content types
- some additional movie technical metadata extracted from French pages
- updated progress and resume metadata in server variables
