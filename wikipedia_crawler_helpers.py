import os

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
        'maxlag': 5,
    }
    if strprops != '':
        params['props'] = strprops
    session = get_session()
    try:
        rate_limit()
        response = session.get(WIKIDATA_API_URL, params=params, timeout=30)
        print(response)
        if response.status_code == 200:
            return response.json()
        return f"Error: {response.status_code}"
    except (requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as err:
        # The shared session already retries/backs off on transient HTTP errors;
        # this guards the rarer connection-level failures it surfaces.
        print(f"get_linked_pages transient error for {wikidata_id} ({strlanguage}): {err}")
        return None


def get_linked_pages_batch(wikidata_ids, strprops='sitelinks', strlanguages='en|fr'):
    """Resolve up to 50 Wikidata ids in a single ``wbgetentities`` call.

    ``wbgetentities`` accepts up to 50 ids and multiple languages at once, so one
    batched call replaces the previous one-call-per-(entity, language). Pass the
    languages pipe-joined (e.g. ``"en|fr"``). Returns the parsed JSON (with the
    ``entities`` map) or ``None`` on failure.
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
        'maxlag': 5,
    }
    if strprops != '':
        params['props'] = strprops
    session = get_session()
    try:
        rate_limit()
        response = session.get(WIKIDATA_API_URL, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        print(f"get_linked_pages_batch HTTP {response.status_code} for {len(ids)} ids")
        return None
    except (requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as err:
        print(f"get_linked_pages_batch transient error for {len(ids)} ids: {err}")
        return None


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
        h2_title = h2.get_text().strip()
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
                h3_title = sibling.get_text().strip()
                current_title = f"{h2_title} - {h3_title}" if h3_title else h2_title
                section_text = ""
                continue
            section_text = _append_block_text(section_text, sibling)
        cleaned = _clean_section_text(section_text)
        if cleaned:
            result.append((current_title, cleaned))
    return result
