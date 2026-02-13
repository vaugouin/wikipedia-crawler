# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Wikipedia crawler that systematically retrieves and stores structured content from Wikipedia and Wikidata. The crawler fetches both English and French Wikipedia pages for entities (movies, TV series, people, and general items) that have Wikidata IDs in a TMDb-based MySQL database.

## Architecture

### Core Components

**wikipedia-crawler.py** - Main crawler script that:
- Processes entities in sequence: movies → persons → items → series → other
- Maintains resumable state through server variables (stored in database)
- Fetches data from Wikidata API to get Wikipedia page titles
- Retrieves full Wikipedia page content via Wikipedia API
- Parses HTML content into structured sections using BeautifulSoup
- Stores sections in `T_WC_WIKIPEDIA_PAGE_LANG_SECTION` table

**citizenphil.py** - Large utility library (143KB) providing:
- Database connection management and SQL helper functions
- Server variable storage/retrieval for crawler state persistence
- TMDb API integration functions
- Timezone utilities (Paris timezone by default)
- Various helper functions used throughout the project

### Data Flow

1. Query database for entities with Wikidata IDs (from TMDb tables: `T_WC_TMDB_MOVIE`, `T_WC_TMDB_PERSON`, `T_WC_TMDB_SERIE`, `T_WC_WIKIDATA_ITEM`)
2. For each entity, fetch both English and French Wikipedia data:
   - Call Wikidata API to get Wikipedia page title for the language
   - Call Wikipedia API to get full page HTML content
   - Parse HTML into sections (intro + all h2 sections)
3. Store each section in database with metadata: `ID_WIKIDATA`, `LANG`, `ITEM_TYPE`, `DISPLAY_ORDER`, `TITLE`, `CONTENT`
4. Track progress via server variables to enable resume on interruption

### State Management

The crawler maintains its state through server variables (database-stored):
- `strwikipediacrawlerprocessesexecuted` - List of completed process IDs
- `strwikipediacrawler{content}id` - Last processed ID for each content type
- `strwikipediacrawler{content}wikidataid` - Last processed Wikidata ID
- `strwikipediacrawlercurrentcontent` - Current content type being processed
- Timing variables: start/end datetime, total runtime

This allows the crawler to resume from where it left off after interruption.

### Database Schema

Key tables (prefix: `T_WC_`):
- `T_WC_TMDB_MOVIE`, `T_WC_TMDB_PERSON`, `T_WC_TMDB_SERIE` - Source entities with Wikidata IDs
- `T_WC_WIKIDATA_ITEM` - General Wikidata items
- `T_WC_WIKIPEDIA_PAGE_LANG_SECTION` - Stores parsed Wikipedia sections
- Server variables table (accessed via `citizenphil.f_getservervariable()`/`f_setservervariable()`)

## Configuration

**citizenphilsecrets.py** (create from [citizenphilsecrets.example.py](citizenphilsecrets.example.py)):
- Database credentials (MariaDB/MySQL)
- TMDb API credentials
- Timezone configuration

**.env** file:
- `WIKIMEDIA_USER_AGENT` - User agent string for Wikipedia/Wikidata API requests (required by Wikimedia policy)

## Running the Crawler

### Local Python
```bash
python wikipedia-crawler.py
```

### Docker
```bash
# Build and run
./wikipedia-crawler.sh

# Or manually:
docker build -t wikipedia-crawler-python-app .
docker run -d --rm --network="host" -v $(pwd):/home/debian/docker/wikipedia-crawler --name wikipedia-crawler wikipedia-crawler-python-app
```

### Dependencies
```bash
pip install -r requirements.txt
```

## Development Notes

### Content Processing Order

The crawler processes content types sequentially (configured in `arrprocessscope`):
1. Movies (201)
2. Persons (202)
3. Items (203)
4. Series (204)
5. Other (209)

The order can be modified by changing the `arrprocessscope` dictionary or setting `strwikipediacrawlercurrentcontent` to resume from a specific type.

### HTML Parsing

`extract_titles_and_text()` function (line 51 in [wikipedia-crawler.py](wikipedia-crawler.py)):
- Extracts intro section (content before first h2)
- Processes each h2 section with its subsections (h3, h4)
- Handles lists (ul, ol) and galleries
- Returns array of tuples: `(section_title, section_content)`

### Special Extraction

For French movie pages, the crawler extracts the "Format" field from the "Fiche technique" section and stores it separately in the `T_WC_TMDB_MOVIE.WIKIPEDIA_FORMAT_LINE` field.

### API Rate Limiting

The crawler includes `time.sleep(0.1)` between Wikidata API calls to respect rate limits.

### Language Support

Currently configured to fetch both English (`en`) and French (`fr`) Wikipedia pages. Language codes are hardcoded in the `arrlang` dictionary (line 260).
