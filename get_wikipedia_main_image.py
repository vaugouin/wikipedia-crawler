import os
import re
import tempfile
import urllib.parse
import webbrowser
from html import unescape

import requests
from bs4 import BeautifulSoup


strwikidataid = "Q8740"
strwikidataid = "Q24815"
strlang = "fr"


def _get_user_agent() -> str:
    ua = os.getenv("WIKIMEDIA_USER_AGENT")
    if ua:
        return ua
    return "wikipedia-crawler/1.0 (https://github.com/; contact: unknown)"


def get_wikipedia_title_from_wikidata_id(wikidata_id: str, lang: str) -> str:
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "format": "json",
        "ids": wikidata_id,
        "props": "sitelinks",
    }
    headers = {"User-Agent": _get_user_agent()}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    entity = data.get("entities", {}).get(wikidata_id)
    if not entity:
        raise RuntimeError(f"No entity found for Wikidata ID {wikidata_id}")

    sitelinks = entity.get("sitelinks", {})
    key = f"{lang}wiki"
    if key not in sitelinks:
        available = ", ".join(sorted(sitelinks.keys()))
        raise RuntimeError(
            f"No sitelink for language '{lang}' (expected '{key}'). Available: {available}"
        )

    title = sitelinks[key].get("title")
    if not title:
        raise RuntimeError(f"Sitelink '{key}' has no title for Wikidata ID {wikidata_id}")

    return title


def get_wikipedia_main_image_url(title: str, lang: str) -> str:
    encoded = urllib.parse.quote(title.replace(" ", "_"), safe="")
    url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{encoded}"
    headers = {"User-Agent": _get_user_agent()}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if isinstance(data, dict):
        original = data.get("originalimage")
        if isinstance(original, dict) and original.get("source"):
            return original["source"]

        thumb = data.get("thumbnail")
        if isinstance(thumb, dict) and thumb.get("source"):
            return thumb["source"]

    raise RuntimeError(f"No main image found in summary for '{title}' ({lang})")


def _get_wikipedia_page_media_items(title: str, lang: str) -> list[dict]:
    encoded = urllib.parse.quote(title.replace(" ", "_"), safe="")
    url = f"https://{lang}.wikipedia.org/api/rest_v1/page/media/{encoded}"
    headers = {"User-Agent": _get_user_agent()}
    resp = requests.get(url, headers=headers, params={"redirect": "true"}, timeout=30)
    if resp.status_code != 200:
        return []
    data = resp.json()
    items = data.get("items")
    if isinstance(items, list):
        return items
    return []


def _caption_from_wikipedia_page_media(title: str, lang: str, image_url: str) -> str:
    filename = _extract_commons_filename_from_url(image_url)
    items = _get_wikipedia_page_media_items(title, lang)

    for item in items:
        if not isinstance(item, dict) or item.get("type") != "image":
            continue

        # Try to match the lead image by URL or filename.
        original = item.get("original") or {}
        original_source = (original.get("source") or "")
        item_title = (item.get("title") or "")

        matches = False
        if original_source and original_source == image_url:
            matches = True
        elif filename and (filename in original_source or filename in item_title):
            matches = True

        if not matches:
            continue

        caption = item.get("caption") or {}
        html = caption.get("html")
        if isinstance(html, str):
            cleaned = _strip_html(html)
            if cleaned:
                return cleaned

        text = caption.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()

    return ""


def _caption_from_wikipedia_parsed_html(title: str, lang: str, image_url: str) -> str:
    filename = _extract_commons_filename_from_url(image_url)

    url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "format": "json",
        "page": title,
        "prop": "text",
        "redirects": 1,
    }
    headers = {"User-Agent": _get_user_agent()}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    if resp.status_code != 200:
        return ""
    data = resp.json()
    html = (((data.get("parse") or {}).get("text") or {}).get("*") or "")
    if not isinstance(html, str) or not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    # Try to locate a figure/infobox element that contains our target image.
    def matches_img(tag) -> bool:
        if not tag or tag.name != "img":
            return False
        src = tag.get("src") or ""
        alt = tag.get("alt") or ""
        if filename and (filename in src or filename in alt):
            return True
        return False

    img = soup.find(matches_img)
    if not img:
        return ""

    # Common patterns: <figure>...<figcaption>...</figcaption></figure>
    figure = img.find_parent("figure")
    if figure:
        figcaption = figure.find("figcaption")
        if figcaption:
            cleaned = figcaption.get_text(" ", strip=True)
            if cleaned:
                return cleaned

    # Infobox pattern: captions can be in elements with class 'infobox-caption'
    infobox = img.find_parent(class_=re.compile(r"\binfobox\b"))
    if infobox:
        caption_el = infobox.find(class_=re.compile(r"\binfobox-caption\b"))
        if caption_el:
            cleaned = caption_el.get_text(" ", strip=True)
            if cleaned:
                return cleaned

        # frwiki infobox_v3 commonly uses a simple 'legend' div for the image caption.
        legend_el = infobox.find(class_=re.compile(r"\blegend\b"))
        if legend_el:
            cleaned = legend_el.get_text(" ", strip=True)
            if cleaned:
                return cleaned

    # Thumb pattern: <div class="thumbcaption">...</div>
    thumb = img.find_parent(class_=re.compile(r"\bthumb\b"))
    if thumb:
        caption_el = thumb.find(class_=re.compile(r"\bthumbcaption\b"))
        if caption_el:
            cleaned = caption_el.get_text(" ", strip=True)
            if cleaned:
                return cleaned

    return ""


def _strip_html(html_text: str) -> str:
    if not html_text:
        return ""
    text = re.sub(r"<[^>]+>", "", html_text)
    return unescape(text).strip()


def _extract_lang_text_from_html(html_text: str, lang: str) -> str:
    if not html_text or not lang:
        return ""
    # Try to extract text from elements explicitly tagged with the requested language.
    # Commons ImageDescription often contains spans/divs like: <span lang="fr">...</span>
    pattern = re.compile(
        rf"<(?P<tag>[^\s>/]+)[^>]*\blang=['\"]{re.escape(lang)}['\"][^>]*>(?P<inner>.*?)</(?P=tag)>",
        re.IGNORECASE | re.DOTALL,
    )
    matches = [m.group("inner") for m in pattern.finditer(html_text)]
    if not matches:
        return ""
    combined = "\n".join(_strip_html(m) for m in matches)
    return combined.strip()


def _extract_commons_filename_from_url(image_url: str) -> str:
    path = urllib.parse.urlparse(image_url).path
    filename = os.path.basename(path)
    return urllib.parse.unquote(filename)


def _get_image_caption_from_api(filename: str, api_base: str, lang: str) -> str:
    url = f"{api_base}/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "extmetadata",
        "uselang": lang,
    }
    headers = {"User-Agent": _get_user_agent()}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    pages = (data.get("query") or {}).get("pages") or {}
    for _page_id, page in pages.items():
        imageinfo = (page or {}).get("imageinfo") or []
        if not imageinfo:
            continue
        ext = (imageinfo[0] or {}).get("extmetadata") or {}

        for key in ("ImageDescription", "ObjectName"):
            value = (ext.get(key) or {}).get("value")
            localized = _extract_lang_text_from_html(value or "", lang)
            if localized:
                return localized

            cleaned = _strip_html(value or "")
            if cleaned:
                return cleaned

    return ""


def get_main_image_caption(image_url: str, lang: str) -> str:
    filename = _extract_commons_filename_from_url(image_url)
    caption = _get_image_caption_from_api(filename, "https://commons.wikimedia.org", lang)
    if caption:
        return caption
    return _get_image_caption_from_api(filename, f"https://{lang}.wikipedia.org", lang)


def get_main_image_caption_for_page(title: str, image_url: str, lang: str) -> str:
    # Prefer the page-provided caption (localized to the wiki language) when present.
    caption = _caption_from_wikipedia_page_media(title, lang, image_url)
    if caption:
        return caption

    caption = _caption_from_wikipedia_parsed_html(title, lang, image_url)
    if caption:
        return caption

    return get_main_image_caption(image_url, lang)


def get_thumbnail_url_for_width(image_url: str, target_width: int) -> tuple[str, int | None, int | None]:
    filename = _extract_commons_filename_from_url(image_url)
    url = "https://commons.wikimedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "url|size",
        "iiurlwidth": str(target_width),
    }
    headers = {"User-Agent": _get_user_agent()}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    if resp.status_code != 200:
        return (image_url, None, None)
    data = resp.json()

    pages = (data.get("query") or {}).get("pages") or {}
    for _page_id, page in pages.items():
        imageinfo = (page or {}).get("imageinfo") or []
        if not imageinfo:
            continue
        info0 = imageinfo[0] or {}
        thumb_url = info0.get("thumburl")
        thumb_w = info0.get("thumbwidth")
        thumb_h = info0.get("thumbheight")
        if isinstance(thumb_url, str):
            return (
                thumb_url,
                thumb_w if isinstance(thumb_w, int) else None,
                thumb_h if isinstance(thumb_h, int) else None,
            )

    return (image_url, None, None)


def display_image_with_caption(
    image_url: str,
    caption: str,
    thumbnails: list[tuple[int, int, str, int | None, int | None]],
    main_display_url: str,
) -> None:
    safe_caption = (caption or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    safe_image_url = (image_url or "").replace("&", "%26")
    safe_main_url = (main_display_url or "").replace("&", "%26")

    thumbs_html = ""
    for target_w, target_h, thumb_url, actual_w, actual_h in thumbnails:
        safe_thumb_url = (thumb_url or "").replace("&", "%26")
        size_text = ""
        if isinstance(actual_w, int) and isinstance(actual_h, int):
            size_text = f"{actual_w}x{actual_h}"
        thumbs_html += (
            "<div style='margin-top: 16px;'>"
            f"<div style='font-size: 14px; margin-bottom: 6px;'>Requested {target_w}x{target_h}  Actual {size_text}</div>"
            f"<img src='{safe_thumb_url}' style='max-width: 100%; height: auto; display: block; border: 1px solid #ddd;'/>"
            "</div>"
        )

    html = (
        "<!doctype html>\n"
        "<html><head><meta charset='utf-8'><title>Wikipedia image</title></head>\n"
        "<body style='font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px;'>\n"
        f"<a href='{safe_image_url}' target='_blank' rel='noreferrer'>"
        f"<img src='{safe_main_url}' style='max-width: 100%; height: auto; display: block;'/></a>\n"
        f"<div style='margin-top: 12px; font-size: 16px; line-height: 1.4;'>{safe_caption}</div>\n"
        f"{thumbs_html}\n"
        "</body></html>\n"
    )

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html", mode="w", encoding="utf-8") as f:
        f.write(html)
        html_path = f.name

    try:
        os.startfile(html_path)  # Windows: open with default browser
    except Exception:
        webbrowser.open(f"file:///{html_path}")


def main() -> None:
    title = get_wikipedia_title_from_wikidata_id(strwikidataid, strlang)
    image_url = get_wikipedia_main_image_url(title, strlang)
    caption = get_main_image_caption_for_page(title, image_url, strlang)
    print(image_url)
    print(caption)

    thumbnails: list[tuple[int, int, str, int | None, int | None]] = []
    for w, h in [(185, 245), (342, 454)]:
        thumb_url, tw, th = get_thumbnail_url_for_width(image_url, w)
        thumbnails.append((w, h, thumb_url, tw, th))
        print(f"{tw}x{th}: {thumb_url}")

    main_thumb_url, _, _ = get_thumbnail_url_for_width(image_url, 342)
    display_image_with_caption(image_url, caption, thumbnails, main_thumb_url)


if __name__ == "__main__":
    main()
