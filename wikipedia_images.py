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
    """Return the User-Agent string used for Wikimedia API requests.

    Prefers the ``WIKIMEDIA_USER_AGENT`` environment variable when set, otherwise
    falls back to a static default.
    """
    ua = os.getenv("WIKIMEDIA_USER_AGENT")
    if ua:
        return ua
    return "wikipedia-crawler/1.0 (https://github.com/; contact: unknown)"

def get_wikipedia_title_from_wikidata_id(wikidata_id: str, lang: str) -> str:
    """Resolve a Wikidata entity id (e.g. ``Q8740``) to a Wikipedia page title.

    Uses the Wikidata Action API and returns the sitelink title for the specified
    Wikipedia language edition.
    """
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
    """Return the main image URL for a Wikipedia page.

    Uses the Wikipedia REST API summary endpoint and prefers ``originalimage`` when
    available, otherwise falls back to ``thumbnail``.
    """
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
    """Fetch image items for a page via the MediaWiki Action API.

    Replaces the retired REST v1 ``/page/media/{title}`` endpoint. Returns dicts
    shaped like the former REST response (``type``, ``title``, ``original``,
    ``thumbnail``, ``caption``) so downstream callers keep working. The Action
    API does not supply per-page captions; ``caption`` is left empty and
    captions are filled in by ``get_wikipedia_page_images`` via HTML scraping.
    """
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    headers = {"User-Agent": _get_user_agent()}

    # Step 1: enumerate every File: title embedded on the page (paginated).
    file_titles: list[str] = []
    params: dict = {
        "action": "query",
        "format": "json",
        "titles": title,
        "prop": "images",
        "imlimit": "max",
        "redirects": 1,
    }
    while True:
        resp = requests.get(api_url, params=params, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"page images HTTP {resp.status_code} for {title} ({lang})")
            return []
        data = resp.json()
        pages = (data.get("query") or {}).get("pages") or {}
        for _pid, page in pages.items():
            for img in (page or {}).get("images") or []:
                t = img.get("title")
                if isinstance(t, str) and t:
                    file_titles.append(t)
        cont = data.get("continue")
        if not cont:
            break
        params.update(cont)

    if not file_titles:
        return []

    # Step 2: resolve URL, dimensions, and thumbnail for each File: in batches of 50.
    items: list[dict] = []
    for i in range(0, len(file_titles), 50):
        batch = file_titles[i : i + 50]
        info_params = {
            "action": "query",
            "format": "json",
            "titles": "|".join(batch),
            "prop": "imageinfo",
            "iiprop": "url|size|mime",
            "iiurlwidth": "320",
        }
        resp = requests.get(api_url, params=info_params, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"imageinfo HTTP {resp.status_code} for {title} ({lang})")
            continue
        data = resp.json()
        pages_dict = (data.get("query") or {}).get("pages") or {}
        for _pid, page in pages_dict.items():
            page_title = page.get("title") or ""
            info_list = page.get("imageinfo") or []
            if not info_list:
                continue
            info0 = info_list[0] or {}
            src = info0.get("url") or ""
            if not src:
                continue
            mime = info0.get("mime") or ""
            if mime and not mime.startswith("image/"):
                continue
            items.append({
                "type": "image",
                "title": page_title,
                "original": {"source": src},
                "thumbnail": {"source": info0.get("thumburl") or ""},
                "caption": {},
            })

    return items

def _get_parsed_page_soup(title: str, lang: str):
    """Fetch and parse the rendered HTML of a Wikipedia page.

    Returns a ``BeautifulSoup`` document, or ``None`` on failure.
    """
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
        print(f"parse HTML HTTP {resp.status_code} for {title} ({lang})")
        return None
    data = resp.json()
    html = (((data.get("parse") or {}).get("text") or {}).get("*") or "")
    if not isinstance(html, str) or not html:
        return None
    return BeautifulSoup(html, "html.parser")


def _caption_from_soup(soup, filename: str) -> str:
    """Find a caption in already-parsed page HTML for the image matching ``filename``.

    Tries figure/figcaption, infobox-caption, frwiki infobox legend, then thumbcaption.
    """
    if soup is None or not filename:
        return ""

    # MediaWiki normalizes spaces to underscores in image URLs.
    filename_underscored = filename.replace(" ", "_")

    def href_targets_image(href: str) -> bool:
        if not href:
            return False
        try:
            decoded = urllib.parse.unquote(href)
        except Exception:
            decoded = href
        # mw-file-description hrefs look like "/wiki/File:Foo.jpg" or "/wiki/Fichier:Foo.jpg"
        return (
            decoded.endswith(":" + filename)
            or decoded.endswith(":" + filename_underscored)
        )

    def matches_img(tag) -> bool:
        if not tag or tag.name != "img":
            return False
        # Prefer exact match via the parent <a class="mw-file-description"> href.
        parent_a = tag.find_parent("a", class_=re.compile(r"\bmw-file-description\b"))
        if parent_a is not None:
            return href_targets_image(parent_a.get("href") or "")
        # Fall back to src/alt substring for the rare img not wrapped in such an <a>.
        src = tag.get("src") or ""
        alt = tag.get("alt") or ""
        return (
            filename in src
            or filename in alt
            or filename_underscored in src
            or filename_underscored in alt
        )

    img = soup.find(matches_img)
    if not img:
        return ""

    # Walk up from the img looking for a specific captioning marker. Each marker
    # is treated as definitive: if its expected caption element is missing, we
    # give up rather than falling back to a broader (and frequently wrong)
    # ancestor caption. This prevents decorative icons sitting inside an infobox
    # row from inheriting the lead image's caption.
    node = img
    while node is not None and node.name is not None:
        classes = node.get("class") or []

        if node.name == "figure":
            figcaption = node.find("figcaption")
            if figcaption:
                cleaned = figcaption.get_text(" ", strip=True)
                if cleaned:
                    return cleaned
            return ""

        if "gallerybox" in classes:
            caption_el = node.find(class_=re.compile(r"\bgallerytext\b"))
            if caption_el:
                cleaned = caption_el.get_text(" ", strip=True)
                if cleaned:
                    return cleaned
            return ""

        if "thumb" in classes:
            caption_el = node.find(class_=re.compile(r"\bthumbcaption\b"))
            if caption_el:
                cleaned = caption_el.get_text(" ", strip=True)
                if cleaned:
                    return cleaned
            # No thumbcaption here — the img may still be inside a gallerybox
            # one level up, so keep walking instead of returning.

        if "infobox-image" in classes:
            caption_el = node.find(class_=re.compile(r"\binfobox-caption\b"))
            if caption_el:
                cleaned = caption_el.get_text(" ", strip=True)
                if cleaned:
                    return cleaned
            tr = node.find_parent("tr")
            if tr is not None:
                next_tr = tr.find_next_sibling("tr")
                if next_tr is not None:
                    caption_el = next_tr.find(class_=re.compile(r"\binfobox-caption\b"))
                    if caption_el:
                        cleaned = caption_el.get_text(" ", strip=True)
                        if cleaned:
                            return cleaned
            return ""

        if node.name == "div" and "images" in classes:
            sib = node.find_next_sibling("div", class_=re.compile(r"\blegend\b"))
            if sib is not None:
                cleaned = sib.get_text(" ", strip=True)
                if cleaned:
                    return cleaned
            return ""

        node = node.parent

    return ""


def _caption_from_wikipedia_parsed_html(title: str, lang: str, image_url: str) -> str:
    """Extract a caption for an image by parsing rendered page HTML."""
    filename = _extract_commons_filename_from_url(image_url)
    soup = _get_parsed_page_soup(title, lang)
    return _caption_from_soup(soup, filename)

def _strip_html(html_text: str) -> str:
    """Remove HTML tags and unescape entities, returning plain text."""
    if not html_text:
        return ""
    text = re.sub(r"<[^>]+>", "", html_text)
    return unescape(text).strip()

def _extract_lang_text_from_html(html_text: str, lang: str) -> str:
    """Extract language-specific text from an HTML snippet.

    Searches for elements with ``lang=<lang>`` and returns their combined text.
    Returns an empty string when no matching language nodes are found.
    """
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
    """Extract and URL-decode the filename from a Wikimedia upload URL."""
    path = urllib.parse.urlparse(image_url).path
    filename = os.path.basename(path)
    return urllib.parse.unquote(filename)

def _derive_thumb_url_from_original(image_url: str, width: int) -> str:
    """Derive a thumbnail URL using the common upload.wikimedia.org thumb path rule.

    If ``image_url`` does not match the expected Commons upload path layout, returns
    ``image_url`` unchanged.
    """
    parsed = urllib.parse.urlparse(image_url)
    path = parsed.path
    # Expected:
    #   /wikipedia/commons/a/ab/Filename.jpg
    # Thumb:
    #   /wikipedia/commons/thumb/a/ab/Filename.jpg/<width>px-Filename.jpg
    m = re.match(r"^(?P<prefix>/wikipedia/commons)/(?P<a>[^/]+)/(?P<ab>[^/]+)/(?P<name>[^/]+)$", path)
    if not m:
        return image_url

    prefix = m.group("prefix")
    a = m.group("a")
    ab = m.group("ab")
    name = m.group("name")

    thumb_path = f"{prefix}/thumb/{a}/{ab}/{name}/{width}px-{name}"
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, thumb_path, "", "", ""))

def _get_filename_from_url(url: str) -> str:
    """Return the last path component (filename) for a URL."""
    return os.path.basename(urllib.parse.urlparse(url).path)

def _get_image_caption_from_api(filename: str, api_base: str, lang: str) -> str:
    """Fetch a file description/caption from a MediaWiki API endpoint.

    Queries ``imageinfo.extmetadata`` for ``ImageDescription`` / ``ObjectName``.
    Attempts to extract ``lang``-tagged fragments when present.
    """
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
    """Get an image caption for a Wikimedia-hosted image.

    Prefers Commons file metadata first, then falls back to the specified language
    Wikipedia file page metadata.
    """
    filename = _extract_commons_filename_from_url(image_url)
    caption = _get_image_caption_from_api(filename, "https://commons.wikimedia.org", lang)
    if caption:
        return caption
    return _get_image_caption_from_api(filename, f"https://{lang}.wikipedia.org", lang)

def get_main_image_caption_for_page(title: str, image_url: str, lang: str) -> str:
    """Get the best available caption for a page's lead image in ``lang``.

    Tries HTML parsing of the page first, then file metadata (Commons /
    local wiki) as a fallback.
    """
    caption = _caption_from_wikipedia_parsed_html(title, lang, image_url)
    if caption:
        return caption

    return get_main_image_caption(image_url, lang)

def get_wikipedia_page_images(title: str, lang: str) -> list[dict]:
    """Return all image items available for a Wikipedia page.

    Each returned dict is normalized for crawler/database usage and may include:
    ``display_order``, ``image_url``, ``image_url_normalized``, ``thumbnail_url``,
    ``media_type``, ``file_name``, ``commons_title``, and ``caption``.
    """
    arrimages = []
    items = _get_wikipedia_page_media_items(title, lang)
    soup = _get_parsed_page_soup(title, lang) if items else None

    for item in items:
        if not isinstance(item, dict) or item.get("type") != "image":
            continue

        original = item.get("original") or {}
        thumbnail = item.get("thumbnail") or {}
        image_url = original.get("source") or ""
        if not image_url:
            continue

        commons_title = item.get("title") or ""
        # The File namespace prefix is localized on non-English wikis
        # (e.g. "Fichier:" on fr-wiki). Strip whatever sits before the first colon.
        file_name = commons_title.split(":", 1)[1] if ":" in commons_title else commons_title
        if file_name == "":
            file_name = _extract_commons_filename_from_url(image_url)

        caption = _caption_from_soup(soup, file_name) if soup is not None else ""

        arrimages.append({
            "display_order": len(arrimages) + 1,
            "image_url": image_url,
            "image_url_normalized": image_url,
            "thumbnail_url": thumbnail.get("source") or "",
            "media_type": item.get("type") or "image",
            "file_name": file_name,
            "commons_title": commons_title,
            "caption": caption,
        })

    return arrimages

def get_thumbnail_url_for_width(image_url: str, target_width: int) -> tuple[str, int | None, int | None]:
    """Return a Wikimedia thumbnail URL for the given image and target width.

    This function queries the MediaWiki Action API on Wikimedia Commons for a thumbnail
    rendition of the file referenced by ``image_url``.

    Inputs:
    - ``image_url``: The original upload.wikimedia.org URL of the image.
    - ``target_width``: Desired thumbnail width in pixels (MediaWiki will preserve aspect ratio).

    Returns a tuple ``(thumb_url, thumb_width, thumb_height)``:
    - ``thumb_url``: The thumbnail URL if available; otherwise the original ``image_url``.
    - ``thumb_width`` / ``thumb_height``: The actual thumbnail dimensions returned by the API,
      or ``None`` if unavailable.

    Notes:
    - The API is authoritative: thumbnails can differ from the common URL pattern for some
      formats (e.g. SVG/TIFF may render to PNG).
    - If the request fails or the API response is missing thumbnail data, the function
      falls back to returning ``(image_url, None, None)``.
    """
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
        if isinstance(thumb_url, str) and thumb_url:
            return (
                thumb_url,
                thumb_w if isinstance(thumb_w, int) else None,
                thumb_h if isinstance(thumb_h, int) else None,
            )

    return (image_url, None, None)

def get_original_image_info(image_url: str) -> tuple[int | None, int | None, str | None]:
    """Return original image dimensions and canonical URL from Wikimedia Commons.

    Returns ``(width, height, url)`` or ``(None, None, None)`` on failure.
    """
    filename = _extract_commons_filename_from_url(image_url)
    url = "https://commons.wikimedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "url|size",
    }
    headers = {"User-Agent": _get_user_agent()}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    if resp.status_code != 200:
        return (None, None, None)
    data = resp.json()

    pages = (data.get("query") or {}).get("pages") or {}
    for _page_id, page in pages.items():
        imageinfo = (page or {}).get("imageinfo") or []
        if not imageinfo:
            continue
        info0 = imageinfo[0] or {}
        w = info0.get("width")
        h = info0.get("height")
        u = info0.get("url")
        return (
            w if isinstance(w, int) else None,
            h if isinstance(h, int) else None,
            u if isinstance(u, str) else None,
        )
    return (None, None, None)

def get_thumbnail_gallery(image_url: str) -> list[dict]:
    """Build a gallery of thumbnail candidates for an image.

    Since Wikimedia thumbnails can be requested at many widths, this returns a
    practical set of requested widths (small dense set + common widths), plus the
    original image entry.
    """
    orig_w, orig_h, orig_url = get_original_image_info(image_url)

    # Wikimedia thumbnails aren't a finite fixed set; in practice you can request many widths.
    # We generate a reasonable gallery of widths: a dense range for small sizes plus some key widths.
    max_w = orig_w if isinstance(orig_w, int) else 2048
    max_w = min(max_w, 2048)

    widths = list(range(50, min(max_w, 600) + 1, 50))
    widths += [64, 100, 120, 150, 185, 200, 250, 300, 342, 400, 500, 640, 800, 1024, 1280, 1600, 1920]
    widths = [w for w in widths if w <= max_w]

    # Ensure uniqueness and stable ordering
    seen = set()
    widths = [w for w in widths if not (w in seen or seen.add(w))]

    items: list[dict] = []
    for w in widths:
        thumb_url, tw, th = get_thumbnail_url_for_width(image_url, w)
        if not isinstance(thumb_url, str) or not thumb_url:
            continue
        items.append(
            {
                "kind": "thumb",
                "requested_width": w,
                "url": thumb_url,
                "width": tw,
                "height": th,
            }
        )

    if isinstance(orig_url, str) and orig_url:
        items.append(
            {
                "kind": "original",
                "requested_width": orig_w,
                "url": orig_url,
                "width": orig_w,
                "height": orig_h,
            }
        )

    return items

def display_image_with_caption(
    image_url: str,
    caption: str,
    gallery: list[dict],
    main_display_url: str,
) -> None:
    """Create and open a temporary HTML page showing the image, caption, and gallery.

    The HTML references Wikimedia URLs directly (no image downloading).
    """
    safe_caption = (caption or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    safe_image_url = (image_url or "").replace("&", "%26")
    safe_main_url = (main_display_url or "").replace("&", "%26")

    # Explain the Wikimedia thumb URL rule (when original URL follows upload.wikimedia.org layout).
    rule_html = "<div style='margin-top: 20px;'>"
    rule_html += "<div style='font-size: 16px; font-weight: 600; margin-bottom: 10px;'>Thumbnail URL rule</div>"
    rule_html += "<div style='font-size: 13px; line-height: 1.35; color: #333;'>"
    rule_html += "For files hosted on upload.wikimedia.org under <code>/wikipedia/commons/&lt;a&gt;/&lt;ab&gt;/&lt;filename&gt;</code>, "
    rule_html += "a common thumbnail URL form is:<br/>"
    rule_html += "<code>/wikipedia/commons/thumb/&lt;a&gt;/&lt;ab&gt;/&lt;filename&gt;/&lt;width&gt;px-&lt;filename&gt;</code>" 
    rule_html += "</div>"
    rule_html += "<div style='font-size: 12px; color: #666; margin-top: 6px;'>Note: some formats (SVG, TIFF) and some images may involve slightly different thumbnail filenames. The API output below is authoritative.</div>"
    rule_html += "</div>"

    thumbs_html = "<div style='margin-top: 20px;'>"
    thumbs_html += "<div style='font-size: 16px; font-weight: 600; margin-bottom: 10px;'>Available sizes</div>"
    thumbs_html += "<div style='display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 14px;'>"

    for item in gallery:
        url = item.get("url")
        w = item.get("width")
        h = item.get("height")
        kind = item.get("kind")
        requested_w = item.get("requested_width")

        if not isinstance(url, str) or not url:
            continue
        safe_url = url.replace("&", "%26")
        size_text = ""
        if isinstance(w, int) and isinstance(h, int):
            size_text = f"{w}x{h}px"

        label = "Original" if kind == "original" else "Thumbnail"
        requested_text = ""
        if isinstance(requested_w, int):
            requested_text = f"requested {requested_w}px"

        # Also show a derived URL based on the common thumb path rule, when applicable.
        derived_url = ""
        derived_filename = ""
        if kind == "thumb" and isinstance(requested_w, int):
            derived_url = _derive_thumb_url_from_original(image_url, requested_w)
            derived_filename = _get_filename_from_url(derived_url)

        derived_html = ""
        if derived_filename and derived_url:
            derived_html = (
                f"<div style='margin-top: 4px; font-size: 12px; line-height: 1.25; color: #444; word-break: break-all;'>"
                f"Derived URL: <code>{derived_url}</code></div>"
                f"<div style='margin-top: 4px; font-size: 12px; line-height: 1.25; color: #444; word-break: break-all;'>"
                f"Derived URL filename: <code>{derived_filename}</code></div>"
            )
        thumbs_html += (
            "<div style='border: 1px solid #e5e5e5; padding: 10px; border-radius: 8px;'>"
            f"<a href='{safe_url}' target='_blank' rel='noreferrer' style='text-decoration: none; color: inherit;'>"
            f"<img src='{safe_url}' style='max-width: 100%; height: auto; display: block; margin: 0 auto 8px auto;'/>"
            f"<div style='font-size: 13px; line-height: 1.2;'>{label} | {requested_text} | {size_text}</div>"
            f"<div style='margin-top: 6px; font-size: 12px; line-height: 1.25; color: #444; word-break: break-all;'>API URL filename: <code>{_get_filename_from_url(url)}</code></div>"
            f"{derived_html}"
            "</a>"
            "</div>"
        )

    thumbs_html += "</div></div>"

    html = (
        "<!doctype html>\n"
        "<html><head><meta charset='utf-8'><title>Wikipedia image</title></head>\n"
        "<body style='font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px;'>\n"
        f"<a href='{safe_image_url}' target='_blank' rel='noreferrer'>"
        f"<img src='{safe_main_url}' style='max-width: 100%; height: auto; display: block;'/></a>\n"
        f"<div style='margin-top: 12px; font-size: 16px; line-height: 1.4;'>{safe_caption}</div>\n"
        f"{rule_html}\n"
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
    """Script entry point for manual testing."""
    title = get_wikipedia_title_from_wikidata_id(strwikidataid, strlang)
    image_url = get_wikipedia_main_image_url(title, strlang)
    caption = get_main_image_caption_for_page(title, image_url, strlang)
    print(title)
    print(image_url)
    print(caption)
    
    if 0:
        main_thumb_url, _, _ = get_thumbnail_url_for_width(image_url, 342)
        if not main_thumb_url or main_thumb_url == image_url:
            main_thumb_url = image_url
        gallery = get_thumbnail_gallery(image_url)
        display_image_with_caption(image_url, caption, gallery, main_thumb_url)

if __name__ == "__main__":
    main()
