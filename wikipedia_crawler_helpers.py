import os
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from wikipedia_http import get_session, rate_limit

load_dotenv()

strwikipediauseragent = (
    os.getenv("WIKIMEDIA_USER_AGENT")
    or "wikipedia-crawler/1.0 (https://github.com/; contact: unknown)"
)
headers = {
    'User-Agent': strwikipediauseragent
}

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"

# WIKIPEDIA-CRAWLER-017. These are READ-ONLY sitelink lookups, so we no longer send
# `maxlag`: on Wikidata `maxlag` reflects the Query Service (WDQS) lag, which is unrelated
# to reading sitelinks and was false-blocking every read whenever WDQS was lagged (e.g.
# maxlag 23.9s persistent). The error-detection + retry below stays as a safety net in case
# wbgetentities ever returns an error body for another reason: such a 200+error body is NOT
# data, so it must be retried (the shared session only retries 429/5xx), never treated as
# "no sitelinks" (which caused silent skip / content loss).
WBGETENTITIES_MAX_RETRIES = 5
WBGETENTITIES_MAX_BACKOFF = 60  # seconds, cap for a single wait


class WikidataTransientError(Exception):
    """A transient failure (maxlag / rate-limit / network) that persisted through every
    retry, so the caller should leave the entity for a later pass rather than mark its
    Wikipedia page empty."""


def _wbgetentities_backoff(attempt, retry_after):
    """Sleep before a retry: honor Retry-After when present, else exponential backoff
    (5, 10, 20, 40 ... capped at WBGETENTITIES_MAX_BACKOFF)."""
    wait = None
    if retry_after:
        try:
            wait = float(retry_after)
        except (TypeError, ValueError):
            wait = None
    if wait is None:
        wait = 5 * (2 ** (attempt - 1))
    time.sleep(min(WBGETENTITIES_MAX_BACKOFF, wait))


def _wbgetentities(params, label):
    """Call the Wikidata wbgetentities API, handling the maxlag error (200 + error body).

    Returns the parsed JSON on success; returns ``None`` on a non-retryable API/HTTP error;
    raises ``WikidataTransientError`` if maxlag / a transient error persists through every
    retry (so the caller does not silently skip a page that actually has sitelinks)."""
    session = get_session()
    for attempt in range(1, WBGETENTITIES_MAX_RETRIES + 1):
        try:
            rate_limit()
            response = session.get(WIKIDATA_API_URL, params=params, timeout=30)
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RetryError) as err:
            print(f"{label} transient error (attempt {attempt}/{WBGETENTITIES_MAX_RETRIES}): {err}")
            _wbgetentities_backoff(attempt, None)
            continue

        if response.status_code != 200:
            print(f"{label} HTTP {response.status_code} (non-retryable)")
            return None

        data = response.json()
        error = data.get("error") if isinstance(data, dict) else None
        if not error:
            return data  # success

        if error.get("code") == "maxlag":
            print(f"{label} maxlag {error.get('lag')}s "
                  f"(attempt {attempt}/{WBGETENTITIES_MAX_RETRIES}); backing off")
            _wbgetentities_backoff(attempt, response.headers.get("Retry-After"))
            continue

        # Any other API error is not retryable here.
        print(f"{label} API error {error.get('code')}: {error.get('info')}")
        return None

    raise WikidataTransientError(
        f"{label}: wbgetentities still maxlag/failing after {WBGETENTITIES_MAX_RETRIES} retries")


def get_linked_pages(wikidata_id, strprops, strlanguage):
    """Resolve a single Wikidata id to its sitelink/page data for one language.

    Retained for callers (and tests) that resolve one id at a time;
    ``get_linked_pages_batch`` is the fast path used by the crawler.
    """
    params = {
        'action': 'wbgetentities',
        'format': 'json',
        'ids': wikidata_id,
        'languages': strlanguage,
    }
    if strprops != '':
        params['props'] = strprops
    return _wbgetentities(params, f"get_linked_pages({wikidata_id}, {strlanguage})")


def get_linked_pages_batch(wikidata_ids, strprops='sitelinks', strlanguages='en|fr'):
    """Resolve up to 50 Wikidata ids in a single ``wbgetentities`` call.

    ``wbgetentities`` accepts up to 50 ids and multiple languages at once, so one
    batched call replaces the previous one-call-per-(entity, language). Pass the
    languages pipe-joined (e.g. ``"en|fr"``). Returns the parsed JSON (with the
    ``entities`` map), ``None`` on a non-retryable error, or raises
    ``WikidataTransientError`` on persistent maxlag / transient failure.
    """
    ids = list(wikidata_ids)
    if not ids:
        return {"entities": {}}
    if len(ids) > 50:
        raise ValueError("wbgetentities accepts at most 50 ids per call")
    params = {
        'action': 'wbgetentities',
        'format': 'json',
        'ids': '|'.join(ids),
        'languages': strlanguages,
    }
    if strprops != '':
        params['props'] = strprops
    return _wbgetentities(params, f"get_linked_pages_batch({len(ids)} ids)")


# WIKIPEDIA-CRAWLER-016: bottom-of-page sections are NEVER sub-split on <h3> — their
# subsections would otherwise escape the end-matter exclusion filters that downstream
# repos apply by exact H2 title (e.g. `TITLE NOT IN ('References','See also',...)` in
# tmdb-front / fastapi-text2sql). Compared case-insensitively against the H2 title.
NO_SUBSPLIT_SECTION_TITLES = {
    # EN
    "references", "see also", "external links", "further reading",
    "notes and references", "notes", "bibliography", "citations", "sources",
    # FR
    "références", "voir aussi", "liens externes", "notes et références",
    "bibliographie",
}


def _append_block_text(section_text, sibling):
    """Append one rendered-HTML block's flattened text to the running section text,
    matching the historical extraction rules (p / h3 / h4 as lines, ul / ol as bullets)."""
    name = sibling.name
    if name == 'p':
        text = sibling.get_text()
        if text:
            section_text += '\n' + text + " "
    elif name == 'h3' or name == 'h4':
        text = sibling.get_text()
        if text:
            section_text += '\n' + text + " "
    elif name == 'ul':
        for li in sibling.find_all('li', recursive=True):
            section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
    elif name == 'ol':
        for li in sibling.find_all('li', recursive=True):
            section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
    return section_text


def _clean_section_text(section_text):
    section_text = section_text.strip()
    while "\n\n" in section_text:
        section_text = section_text.replace("\n\n", "\n")
    return section_text


def _normalize_heading_separator(title):
    """Escape a spaced hyphen INSIDE a single heading so it can't be mistaken for the
    composite ``"H2 - H3"`` join.

    WIKIPEDIA-CRAWLER-016 keeps ``" - "`` (spaced hyphen-minus) as the separator between the
    H2 parent and the H3 child. But some headings contain a spaced hyphen of their own --
    date ranges (``"2015 - present"``, ``"1926 - 1929"``) are common in person filmographies
    and career phases, and appositive titles use it too. Left as-is, a downstream split on
    ``" - "`` would recover the wrong parent/child (observed in the person-section audit,
    2026-07-23). Replacing the intra-title spaced hyphen with a spaced en-dash (which Wikipedia
    itself uses for ranges) guarantees the ONLY ``" - "`` in a composite title is the join.
    Only the spaced ASCII hyphen-minus collides; an unspaced ``"1926-1929"`` or an existing
    en/em dash does not, so those are left untouched.
    """
    return title.replace(" - ", " – ")


def extract_titles_and_text(html_content=None, soup=None):
    """Turn rendered page HTML into ``[(section_title, section_text), ...]``.

    Accepts either raw ``html_content`` or a pre-parsed ``soup``; passing an
    already-built soup lets the caller parse the page HTML once and reuse it for
    both section extraction and image captions (Phase 1b).

    WIKIPEDIA-CRAWLER-016: sections split on ``<h2>`` **and** ``<h3>`` (``<h4>`` stays
    inline inside its ``<h3>``). An ``<h3>`` sub-section is titled ``"Parent - Child"``
    (composite); the ``<h2>`` lead text before the first ``<h3>`` keeps the bare ``<h2>``
    title. Bottom-of-page sections (References, See also, …) are never sub-split so their
    subsections don't escape downstream end-matter exclusion filters. Empty sections are
    dropped.
    """
    if soup is None:
        soup = BeautifulSoup(html_content, 'html.parser')
    headers = soup.find_all('h2')
    result = []
    first_h2 = headers[0] if headers else None

    # Intro = everything before the first <h2> (never sub-split).
    section_text = ""
    for sibling in soup.body.find_all(recursive=True):
        if sibling == first_h2 or sibling.name == "h2":
            break
        section_text = _append_block_text(section_text, sibling)
    result.append(('Intro', _clean_section_text(section_text)))

    for h2 in headers:
        h2_title = _normalize_heading_separator(h2.get_text().strip())
        no_subsplit = h2_title.lower() in NO_SUBSPLIT_SECTION_TITLES
        current_title = h2_title  # the H2 lead (chapô) keeps the bare H2 title
        section_text = ""
        for sibling in h2.find_all_next():
            if sibling.name == "h2":
                break
            if sibling.name == 'h3' and not no_subsplit:
                # Flush the current (sub)section and open a new composite one.
                cleaned = _clean_section_text(section_text)
                if cleaned:
                    result.append((current_title, cleaned))
                h3_title = _normalize_heading_separator(sibling.get_text().strip())
                current_title = f"{h2_title} - {h3_title}" if h3_title else h2_title
                section_text = ""
                continue
            section_text = _append_block_text(section_text, sibling)
        cleaned = _clean_section_text(section_text)
        if cleaned:
            result.append((current_title, cleaned))
    return result
