# AGENTS.md - Agent Guide for Wikipedia Crawler

Rules and traps for agents. Project overview, entity families, modules, data flow, resume mechanism, quick mode, parallel refresh, snapshot backfill, environment, Docker deployment, tests: @README.md (canonical). Reference DDL: `doc/sql/*.sql` (`T2S-tables.sql`, `TMDb-tables.sql`, `Wikidata-tables.sql`, `Wikipedia-tables.sql`), read-only unless the user asks for schema-doc edits; do not duplicate table definitions elsewhere.

Update `README.md` for user-facing behavior, configuration or deployment changes; update this file only when agent workflow or safety context changes.

---

## Related repositories (project ecosystem)

One stage of **Agent BBB** (GitHub `vaugouin`; siblings at `%USERPROFILE%/Code/<repo>` and `github.com/vaugouin/<repo>`, converging on a shared MariaDB with `T_WC_*` tables). Canonical roster and stage map: `%USERPROFILE%/Nestor/projets/t2s-backlog/topics/related-repositories.txt`.

**This repository's role:** acquisition. Fetches English and French Wikipedia article sections for Wikidata-linked movies, series, persons, items and the T2S entity families into `T_WC_WIKIPEDIA_*`, and extracts French «Fiche technique» film-format metadata. Consumed by `tmdb-movie-preprocess` and the Wikipedia sections of `tmdb-front`. It also writes the image columns of the V1 Wikidata tables (`WIKIPEDIA_*_PATH`).

**Adding an entity family** (last done for `sourcework`, process 224, WIKIPEDIA-CRAWLER-023) touches four places that must stay in sync: a `build_<family>_sql` in `wikipedia_queries.py` plus its `CONTENT_SQL_BUILDERS` key, a `CONTENT_CONFIG` entry in `wikipedia_page_writer.py` (same process id, image table/column, source table/id column), the import, resume server variable and `arrprocesses` entry in `wikipedia_crawler.py` (next free id, 2xx; appended last so the exclusion chain of earlier families is unchanged), and `arrquickprocessids`. A row filter the family applies to its own source table goes in `CONTENT_SOURCE_FILTERS` so `--no-exclusions` keeps it. Then update the README lists (entities table, process order, resume variables, quick-mode table). The lead image needs no downstream wiring: process 71 of `tmdb-movie-preprocess` copies `T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL` into the T2S tables it lists.

---

## Code conventions

- Legacy Hungarian notation: `str` strings, `lng` integers, `dbl` floats, `arr` lists/dicts, `int` boolean-like flags.
- Public functions use the `f_` prefix, private helpers `_`; Google-style docstrings on public functions.
- Keep Markdown, JSON and SQL UTF-8: the data is multilingual and any encoding rewrite introduces mojibake.
- Secrets come in at runtime (`--env-file`), never baked into the image.

## SQL Object Naming Conventions

- Uppercase snake case (except legacy TMDb genre columns `id`, `name`). Prefixes: `T_WC_*` persistent, `T_WC_T2S_*` Text2SQL read model, `T_WC_TMDB_*` TMDb, `T_WC_WIKIDATA_*` Wikidata (`STG_T_WC_WIKIDATA_*` staging), `T_WC_WIKIPEDIA_*` Wikipedia. Join tables `T_WC_T2S_{PARENT}_{CHILD}`.
- Keys: `ID_{ENTITY}`, `ID_ROW` for generic/join rows, or a table-specific surrogate; foreign keys reuse the referenced key name.
- Suffixes/prefixes: `DAT_*` dates, `TIM_*` timestamps, `IS_*` or legacy integer flags (`DELETED`), `DISPLAY_ORDER`, `*_COUNT`, `*_PATH`, `_FR` / `LANG` for languages, `*_NORM` / `*_KEY` for RapidFuzz columns.
- Index names are mixed legacy style; preserve it: `KEY COLUMN_NAME`, `IDX_*`, `UK_*`, `FK_*`, `ft_*`.

## Snapshot ingest: safety rules

`wikipedia_dump_ingest.py` / `run-if-new-snapshot.sh` (WIKIPEDIA-CRAWLER-024/025/026) are described in README § "Backfill from the Wikimedia Enterprise snapshots". Rules:
- While `dumpdata/INGEST_IN_PROGRESS` exists or the `wikipedia-dump-ingest` container runs, `wikipedia-crawler.sh` does not start the crawler (both write the same rows). **Never delete the flag by hand during a run.**
- Each chunk runs in a forked child (1.1.11, after an OOM kill at 21 GB). Test changes to the chunk loop under Linux: Windows has no fork and runs chunks inline.
- **Never remove the disk guard** (`WME_INGEST_MIN_FREE_GB`, default 40): a full snapshot grows the database by tens of GB on the same filesystem, and on 2026-10-20 it filled the VPS disk and crashed MariaDB. Measure with `doc/sql/check-table-sizes.sql` before and after a batch.

---

**Last Updated**: 2026-10-31
**Current Version**: 1.1.32 

## Backlog (Nestor second-brain)

Backlog for this repo: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\wikipedia-crawler.md` (dashboard: `...\t2s-backlog\index.md`). Tasks are `WIKIPEDIA-CRAWLER-NNN`; consult it before implementing. Local paths on Philippe's PC only, they do not resolve on the VPS or cloud agents.

## SQL files live in `doc/sql/`

Stack-wide convention, set 2026-08-20. Every **read-only** `.sql` (audit, monitoring, export, reference DDL dump) lives in `doc/sql/`, never at the root nor in a `doc/queries/`. Two exceptions: a `.sql` **executed by code** stays where the code expects it (moving it breaks a run silently), and a `.sql` that **writes** (migration, seed, cleanup) stays with its procedure. Test: if running it twice by accident would change the database, it is not a `doc/sql/` file.
