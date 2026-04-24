import os
import time

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

strwikipediauseragent = os.getenv("WIKIMEDIA_USER_AGENT")
headers = {
    'User-Agent': strwikipediauseragent
}


def get_linked_pages(wikidata_id, strprops, strlanguage):
    url = f"https://www.wikidata.org/w/api.php"
    if strprops == '':
        params = {
            'action': 'wbgetentities',
            'format': 'json',
            'ids': wikidata_id,
            'languages': strlanguage
        }
    else:
        params = {
            'action': 'wbgetentities',
            'format': 'json',
            'ids': wikidata_id,
            'props': strprops,
            'languages': strlanguage
        }
    time.sleep(0.1)
    response = requests.get(url, params=params, headers=headers)
    print(response)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return f"Error: {response.status_code}"


def extract_titles_and_text(html_content):
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
