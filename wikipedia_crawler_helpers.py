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


def extract_titles_and_text(html_content=None, soup=None):
    """Turn rendered page HTML into ``[(section_title, section_text), ...]``.

    Accepts either raw ``html_content`` or a pre-parsed ``soup``; passing an
    already-built soup lets the caller parse the page HTML once and reuse it for
    both section extraction and image captions (Phase 1b).
    """
    if soup is None:
        soup = BeautifulSoup(html_content, 'html.parser')
    headers = soup.find_all('h2')
    result = []
    first_h2 = headers[0] if headers else None
    section_text = ""
    for sibling in soup.body.find_all(recursive=True):
        if sibling == first_h2:
            break
        elif sibling.name == "h2":
            break
        elif sibling.name == 'p':
            text = sibling.get_text()
            if text:
                section_text += '\n' + text + " "
        elif sibling.name == 'h3' or sibling.name == 'h4':
            text = sibling.get_text()
            if text:
                section_text += '\n' + text + " "
        elif sibling.name == 'ul':
            for li in sibling.find_all('li', recursive=True):
                section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
        elif sibling.name == 'ol':
            for li in sibling.find_all('li', recursive=True):
                section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
        elif sibling.name == 'ul' and 'gallery' in sibling.get('class', []):
            caption = sibling.find('li', class_='gallerycaption')
            if caption:
                section_text += '\n' + caption.get_text() + " "
            for gallery_text in sibling.find_all('div', class_='gallerytext'):
                text = gallery_text.get_text()
                if text:
                    section_text += '\n' + text
    section_text = section_text.strip()
    while "\n\n" in section_text:
        section_text = section_text.replace("\n\n", "\n")
    result.append(('Intro', section_text))

    for h2 in headers:
        title = h2.get_text()
        section_text = ""
        for sibling in h2.find_all_next():
            if sibling.name == "h2":
                break
            elif sibling.name == 'p':
                text = sibling.get_text()
                if text:
                    section_text += '\n' + sibling.get_text() + " "
            elif sibling.name == 'h3' or sibling.name == 'h4':
                text = sibling.get_text()
                if text:
                    section_text += '\n' + text + " "
            elif sibling.name == 'ul':
                for li in sibling.find_all('li', recursive=True):
                    section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
            elif sibling.name == 'ol':
                for li in sibling.find_all('li', recursive=True):
                    section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
            elif sibling.name == 'ul' and 'gallery' in sibling.get('class', []):
                caption = sibling.find('li', class_='gallerycaption')
                if caption:
                    section_text += '\n' + caption.get_text() + " "
                for gallery_text in sibling.find_all('div', class_='gallerytext'):
                    text = gallery_text.get_text()
                    if text:
                        section_text += '\n' + text
        section_text = section_text.strip()
        while "\n\n" in section_text:
            section_text = section_text.replace("\n\n", "\n")
        result.append((title, section_text))
    return result
