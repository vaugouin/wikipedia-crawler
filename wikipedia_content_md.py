"""Wikipedia page HTML to Markdown sections, repaired plain text, links and references (1.1.0).

Topic wikipedia-rich-sections (Nestor t2s-backlog), decisions of 2026-10-15, built from
``prototype_content_md.py`` after two rounds on 22 pages:

- CONTENT_MD: each section as Markdown. Links are written by Wikipedia title
  (``[words](Target_title)``), bold/italic, nested lists, tables and definition lists are
  kept; footnote call-outs, edit links, navboxes, galleries and portal banners are dropped.
- CONTENT (repaired): the plain text derived from the Markdown (``md_to_text``). Tables come
  back as plain rows, ``[1]`` call-outs are gone, nested lists are written once, and the
  ``- `` bullets stay, so the French "Fiche technique" ``- Format`` extraction still works
  (checked on 60 French films, 2026-10-15).
- Links: one row per internal link, with the date flag (French Wikipedia links every date
  part, ``[20](20_octobre) [octobre](Octobre_2009) [2009](2009_à_la_télévision)``: kept and
  flagged, not dropped) and, once resolved, the target's Qid.
- References: one row per footnote cited in a section, with URL, title and the full
  reference text (43 % of references have no URL: books, short citations).

The section split follows WIKIPEDIA-CRAWLER-016 (H2 and H3, composite "H2 - H3" titles,
end-matter sections never sub-split) and walks top-level blocks only, so nothing is visited
twice. Both HTML shapes are read: the legacy parser output and the Parsoid one that
``action=parse`` served on 2026-10-15 from 10:00 to 19:00 Paris (sections wrapped in
``<section>``, links as ``./Title`` or absolute URLs).

Everything here is network-free and database-free except the two resolvers at the bottom,
which the writer calls on the main thread.
"""

import re
import threading
import urllib.parse

from bs4 import NavigableString, Tag
from bs4.element import PreformattedString

from wikipedia_crawler_helpers import NO_SUBSPLIT_SECTION_TITLES, _normalize_heading_separator
from wikipedia_http import get_session, rate_limit

# Blocks that are page furniture, not article content.
DROP_CLASSES = {
    "mw-editsection", "reference", "navbox", "navbox-styles", "navbox-container",
    "vertical-navbox", "sistersitebox", "metadata", "noprint", "mw-empty-elt",
    "bandeau-portail", "portal-bar", "catlinks", "mw-references-wrap",
    "printfooter", "authority-control", "bandeau-container", "homonymie", "mw-references",
    "plainlinks", "mw-authority-control", "navigation-not-searchable",
}
DROP_TAGS = {"style", "script", "link", "meta", "figure", "img", "audio", "video"}

# Column widths of T_WC_WIKIPEDIA_PAGE_LANG_LINK / _REFERENCE (doc/sql/Wikipedia-tables.sql).
MAX_ANCHOR = 500
MAX_TITLE = 300
MAX_NOTE_KEY = 300
MAX_LABEL = 50
MAX_REF_TITLE = 1000

FR_MONTHS = "janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre"
# Calendar pages French Wikipedia links outside the {{date}} template too: "20 octobre",
# "1er janvier", "Octobre 2009" (four-digit years only: "Mai 68" is an event), "2009", and the year-in-a-field pages the template's third
# link points to ("2009 à la télévision", "1966 au cinéma"), counted as dates in round 2.
FR_DATE_TARGET = re.compile(
    rf"^(?:(?:1er|\d{{1,2}}) (?:{FR_MONTHS})|(?:{FR_MONTHS}) \d{{4}}|\d{{1,4}}(?: (?:au|aux|à la|à l'|en|dans le|dans la) .+)?)$",
    re.IGNORECASE)


def _classes(tag):
    return set(tag.get("class") or [])


def _is_dropped(tag):
    if tag.name in DROP_TAGS:
        return True
    if _classes(tag) & DROP_CLASSES:
        return True
    if tag.name == "sup" and "reference" in _classes(tag):
        return True
    if "display:none" in (tag.get("style") or "").replace(" ", ""):
        return True
    if tag.name == "div" and (_classes(tag) & {"thumb", "tsingle", "gallery", "image"}):
        return True
    if tag.name == "ul" and "gallery" in _classes(tag):
        return True
    return False


def wiki_target(href):
    """Return (title, fragment) for an internal article link, else None.

    Accepts the legacy form ("/wiki/Title") and the Parsoid ones
    ("https://<lang>.wikipedia.org/wiki/Title", "./Title").
    """
    href = re.sub(r"^https?://[a-z-]+\.wikipedia\.org(?=/wiki/)", "", href or "")
    if href.startswith("./"):
        href = "/wiki/" + href[2:]
    if not href.startswith("/wiki/"):
        return None
    path = href[len("/wiki/"):]
    path, _, fragment = path.partition("#")
    path = path.split("?", 1)[0]          # "?action=edit&redlink=1"; a real "?" in a title is %3F
    title = urllib.parse.unquote(path).replace("_", " ").strip()
    if not title:
        return None
    # Namespaced pages (File:, Help:, Category:, Portail:, Fichier:...) are not entities.
    if re.match(r"^[^ :]+:", title) and not title.startswith(":"):
        return None
    return title, urllib.parse.unquote(fragment).replace("_", " ")


def is_date_link(lang, a_tag, target):
    """True for a link that is part of a date (French Wikipedia only).

    Structural first: the {{date}} template wraps its three links in ``.date-lien``.
    Then the calendar-page titles the template does not cover.
    """
    if lang != "fr":
        return False
    if a_tag.find_parent(class_="date-lien") is not None:
        return True
    return bool(FR_DATE_TARGET.match(target or ""))


class MarkdownRenderer:
    """Turn one page's top-level HTML blocks into Markdown, collecting links and footnotes.

    ``self.section`` is a running number set by the splitter for each section it opens; the
    splitter maps it to the final DISPLAY_ORDER once empty sections are dropped.
    """

    def __init__(self, lang):
        self.lang = lang
        self.links = []          # dicts: section, anchor, target, fragment, redlink, is_date
        self.refs = []           # dicts: section, note, label
        self._refkeys = set()
        self.section = 0

    # -- inline --------------------------------------------------------------
    def inline(self, node):
        # 1.1.3: comments (the "NewPP limit report" action=parse appends) are not text.
        if isinstance(node, PreformattedString):
            return ""
        if isinstance(node, NavigableString):
            return re.sub(r"\s+", " ", str(node))
        if isinstance(node, Tag) and node.name == "sup" and "reference" in _classes(node):
            # Footnote call-out: out of the text, into the reference table.
            a = node.find("a", href=True)
            href = a["href"] if a else ""
            if "#" in href:
                note = href.split("#", 1)[1]
                if note and (self.section, note) not in self._refkeys:
                    self._refkeys.add((self.section, note))
                    self.refs.append({"section": self.section, "note": note, "label": node.get_text(strip=True)})
            return ""
        if not isinstance(node, Tag) or _is_dropped(node):
            return ""
        inner = "".join(self.inline(c) for c in node.children)
        name = node.name
        if name in ("b", "strong"):
            return f"**{inner.strip()}**" if inner.strip() else ""
        if name in ("i", "em", "cite"):
            return f"*{inner.strip()}*" if inner.strip() else ""
        if name == "br":
            return " "
        if name in ("ul", "ol"):
            # A list inside a cell or a sentence (hlist, plainlist): items joined by commas.
            items = [re.sub(r"\s+", " ", self.inline(li)).strip() for li in node.find_all("li", recursive=False)]
            return " " + ", ".join(i for i in items if i) + " "
        if name == "a":
            href = node.get("href") or ""
            text = inner.strip()
            if not text:
                return ""
            if "new" in _classes(node) or "redlink=1" in href:
                # Red link: no article yet. Kept in the link table, plain in the text.
                m = re.search(r"title=([^&]+)", href)
                t2 = wiki_target(href)
                target = urllib.parse.unquote(m.group(1)).replace("_", " ") if m else (t2[0] if t2 else text)
                self.links.append({"section": self.section, "anchor": text, "target": target, "fragment": "",
                                   "redlink": 1, "is_date": int(is_date_link(self.lang, node, target))})
                return text
            target = wiki_target(href)
            if target:
                title, fragment = target
                self.links.append({"section": self.section, "anchor": text, "target": title, "fragment": fragment,
                                   "redlink": 0, "is_date": int(is_date_link(self.lang, node, title))})
                link = title.replace(" ", "_") + (f"#{fragment.replace(' ', '_')}" if fragment else "")
                return f"[{text}]({link})"
            if href.startswith("http") or href.startswith("//"):
                return f"[{text}]({href if href.startswith('http') else 'https:' + href})"
            return text
        return inner

    def text_of(self, node):
        return re.sub(r"[ \t]+", " ", self.inline(node)).strip()

    # -- blocks --------------------------------------------------------------
    def block(self, node, depth=0):
        """Return a list of Markdown lines for one block-level node."""
        if isinstance(node, PreformattedString):
            return []
        if isinstance(node, NavigableString):
            t = str(node).strip()
            return [t] if t else []
        if not isinstance(node, Tag) or _is_dropped(node):
            return []
        name = node.name
        if name == "p":
            t = self.text_of(node)
            return [t, ""] if t else []
        if name in ("ul", "ol"):
            return self.list_lines(node, depth, ordered=(name == "ol")) + ([""] if depth == 0 else [])
        if name == "dl":
            out = []
            for child in node.find_all(["dt", "dd"], recursive=False):
                t = self.text_of(child)
                if t:
                    out.append(f"**{t}**" if child.name == "dt" else f": {t}")
            return out + [""] if out else []
        if name == "blockquote":
            t = self.text_of(node)
            return [f"> {t}", ""] if t else []
        if name == "table":
            return self.table_lines(node)
        if name in ("h4", "h5", "h6"):
            t = self.text_of(node)
            return [f"#### {t}", ""] if t else []
        if name == "div" and "mw-heading" in _classes(node):
            h = node.find(re.compile(r"^h[4-6]$"))
            return self.block(h) if h else []
        if name in ("div", "section", "center", "span"):
            if "hatnote" in _classes(node) or "dablink" in _classes(node):
                t = self.text_of(node)
                return [f"*{t}*", ""] if t else []
            out = []
            for c in node.children:
                out += self.block(c, depth)
            return out
        t = self.text_of(node)
        return [t] if t else []

    def list_lines(self, node, depth, ordered):
        out = []
        n = 0
        for li in node.find_all("li", recursive=False):
            if _is_dropped(li):
                continue
            n += 1
            head = []
            nested = []
            for c in li.children:
                if isinstance(c, Tag) and c.name in ("ul", "ol"):
                    nested += self.list_lines(c, depth + 1, ordered=(c.name == "ol"))
                elif isinstance(c, Tag) and c.name == "dl":
                    nested += ["  " * (depth + 1) + l for l in self.block(c) if l]
                else:
                    head.append(self.inline(c))
            text = re.sub(r"\s+", " ", "".join(head)).strip()
            bullet = f"{n}." if ordered else "-"
            if text:
                out.append("  " * depth + f"{bullet} {text}")
            out += nested
        return out

    def table_lines(self, table):
        classes = _classes(table)
        is_infobox = bool(classes & {"infobox", "infobox_v2", "infobox_v3"}) or "infobox" in " ".join(classes)
        # Expand rowspan/colspan into a plain grid.
        grid, spans = [], {}
        for tr in table.find_all("tr"):
            if tr.find_parent("table") is not table:
                continue
            row, col = [], 0
            cells = [c for c in tr.find_all(["th", "td"], recursive=False)]
            ci = 0
            while ci < len(cells) or (col in spans):
                if col in spans:
                    text, left = spans[col]
                    row.append(text)
                    if left <= 1:
                        del spans[col]
                    else:
                        spans[col] = (text, left - 1)
                    col += 1
                    continue
                cell = cells[ci]
                ci += 1
                text = self.text_of(cell).replace("|", "\\|")
                colspan = min(int(re.sub(r"\D", "", cell.get("colspan", "1")) or 1), 100)
                rowspan = min(int(re.sub(r"\D", "", cell.get("rowspan", "1")) or 1), 1000)
                for _ in range(colspan):
                    row.append(text)
                    if rowspan > 1:
                        spans[col] = (text, rowspan - 1)
                    col += 1
            if any(x.strip() for x in row):
                grid.append(row)
        if not grid:
            return []
        if is_infobox:
            # Key/value pairs read better than a ragged table.
            out = []
            for row in grid:
                vals = [v for v in row if v.strip()]
                if len(vals) >= 2:
                    out.append(f"- **{vals[0]}**: {' '.join(dict.fromkeys(vals[1:]))}")
                elif len(vals) == 1:
                    out.append(f"- {vals[0]}")
            return ["**Infobox**", ""] + out + [""]
        width = max(len(r) for r in grid)
        grid = [r + [""] * (width - len(r)) for r in grid]
        caption = table.find("caption")
        captext = self.text_of(caption) if caption else ""
        out = [f"*{captext}*", ""] if captext else []
        out.append("| " + " | ".join(grid[0]) + " |")
        out.append("|" + "---|" * width)
        for r in grid[1:]:
            out.append("| " + " | ".join(r) + " |")
        return out + [""]


def _top_blocks(parent):
    # Parsoid HTML wraps each section in <section>: walk through them.
    for c in parent.children:
        if isinstance(c, Tag) and c.name == "section":
            yield from _top_blocks(c)
        else:
            yield c


def split_sections(soup, lang):
    """Split the page like extract_titles_and_text and render each section.

    Returns (sections, links, refs). ``sections`` is a list of (display_order, title, md)
    with display_order 1..n over the non-empty sections; every link and footnote carries
    the display_order of its section.
    """
    root = soup.find("div", class_="mw-parser-output") or soup.body or soup
    renderer = MarkdownRenderer(lang)
    sections = []
    seq_to_order = {}
    state = {"seq": 0, "h2": None, "title": "Intro", "lines": []}
    renderer.section = 0

    def flush():
        md = "\n".join(state["lines"])
        md = re.sub(r"\n{3,}", "\n\n", md).strip()
        if md:
            seq_to_order[state["seq"]] = len(sections) + 1
            sections.append((len(sections) + 1, state["title"], md))

    def open_section(title):
        flush()
        state["seq"] += 1
        state["title"], state["lines"] = title, []
        renderer.section = state["seq"]

    for node in _top_blocks(root):
        heading = None
        if isinstance(node, Tag):
            if node.name in ("h2", "h3"):
                heading = node
            elif node.name == "div" and "mw-heading" in _classes(node):
                heading = node.find(["h2", "h3"])
        if heading is not None and heading.name == "h2":
            state["h2"] = _normalize_heading_separator(heading.get_text().strip())
            open_section(state["h2"])
            continue
        if heading is not None and heading.name == "h3" and state["h2"] is not None \
                and state["h2"].lower() not in NO_SUBSPLIT_SECTION_TITLES:
            h3 = _normalize_heading_separator(heading.get_text().strip())
            open_section(f"{state['h2']} - {h3}" if h3 else state["h2"])
            continue
        if heading is not None:  # h3 inside an end-matter section stays inline
            state["lines"] += [f"### {heading.get_text().strip()}", ""]
            continue
        state["lines"] += renderer.block(node)
    flush()

    # Links and footnotes met in a section that rendered empty have nowhere to go.
    links = [dict(l, section=seq_to_order[l["section"]]) for l in renderer.links if l["section"] in seq_to_order]
    refs = [dict(r, section=seq_to_order[r["section"]]) for r in renderer.refs if r["section"] in seq_to_order]
    return sections, links, refs


def collect_references(soup):
    """Map each footnote id (cite_note-...) to its URL, title and full text."""
    out = {}
    for li in soup.select('li[id^="cite_note"]'):
        body = li.find(class_="reference-text") or li
        ext = body.find("a", class_="external", href=True)
        url = ext["href"] if ext else ""
        if url.startswith("//"):
            url = "https:" + url
        cite = body.find("cite")
        title = (ext.get_text(" ", strip=True) if ext else "") or (cite.get_text(" ", strip=True) if cite else "")
        out[li["id"]] = {"url": url, "title": re.sub(r"\s+", " ", title).strip(),
                         "text": re.sub(r"\s+", " ", body.get_text(" ", strip=True))}
    return out


def md_to_text(md):
    """Plain text from the Markdown: links to their words, emphasis gone, tables as plain rows."""
    out = []
    for line in md.split("\n"):
        if re.match(r"^\|(-+\|)+$", line):
            continue                                   # table separator
        if line.strip() == "**Infobox**":
            continue
        # [words](target) -> words; a target may hold balanced parentheses: Richard_Harrison_(acteur)
        line = re.sub(r"\[([^\]]+)\]\((?:[^()\s]|\([^()\s]*\))*\)", r"\1", line)
        if line.startswith("| ") and line.endswith(" |"):
            cells = [c.strip().replace("\\|", "|") for c in re.split(r"(?<!\\)\|", line[1:-1])]
            line = " | ".join(cells)
        line = line.replace("**", "")
        line = re.sub(r"(?<![\w*])\*(\S[^*]*?)\*(?![\w*])", r"\1", line)
        line = re.sub(r"^#{3,6} ", "", line)
        line = re.sub(r"^> ", "", line)
        out.append(line)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(out)).strip()


def convert_page(soup, lang):
    """One page's sections, links and references, ready for the writer.

    Returns {"sections": [(display_order, title, text, md)], "links": [...], "refs": [...]}.
    Link dicts: section, display_order, anchor, target, fragment, redlink, is_date.
    Reference dicts: section, display_order, note, label, url, title, text.
    Nothing is resolved here: Qids come from ``resolve_link_qids`` on the main thread.
    """
    md_sections, links, refs = split_sections(soup, lang)
    notes = collect_references(soup)
    sections = [(order, title, md_to_text(md), md) for order, title, md in md_sections]
    counters = {}
    for l in links:
        counters[l["section"]] = counters.get(l["section"], 0) + 1
        l["display_order"] = counters[l["section"]]
    counters = {}
    for r in refs:
        counters[r["section"]] = counters.get(r["section"], 0) + 1
        r["display_order"] = counters[r["section"]]
        r.update(notes.get(r["note"], {"url": "", "title": "", "text": ""}))
    return {"sections": sections, "links": links, "refs": refs}


# -- Qid resolution (main thread) ----------------------------------------------

API_BATCH = 50
_API_CACHE_MAX = 200_000
_api_cache = {}                 # (lang, title) -> (qid, final_title, via_redirect)
_api_cache_lock = threading.Lock()


def resolve_by_title(cursor, lang, titles):
    """Local title join on T_WC_WIKIPEDIA_PAGE_LANG (index LANG_WIKIPEDIA_PAGE_TITLE).

    The column collation is accent- and case-insensitive, so candidates are filtered here
    on the exact title; a title held by two Qids is left to the API. Returns {title: qid}.
    """
    out = {}
    titles = sorted({t for t in titles if t})
    for i in range(0, len(titles), 500):
        batch = titles[i:i + 500]
        placeholders = ", ".join(["%s"] * len(batch))
        cursor.execute(
            "SELECT ID_WIKIDATA, WIKIPEDIA_PAGE_TITLE FROM T_WC_WIKIPEDIA_PAGE_LANG "
            f"WHERE LANG = %s AND WIKIPEDIA_PAGE_TITLE IN ({placeholders}) AND PAGE_EXISTS = 1",
            [lang] + batch)
        hits = {}
        for row in cursor.fetchall():
            hits.setdefault(row["WIKIPEDIA_PAGE_TITLE"], set()).add(row["ID_WIKIDATA"])
        wanted = set(batch)
        for title, qids in hits.items():
            if title in wanted and len(qids) == 1:
                out[title] = next(iter(qids))
    return out


def resolve_by_api(lang, titles):
    """Resolve titles through the Wikipedia API, redirects followed, 50 titles per call.

    Returns {title: (qid, final_title, via_redirect)}; qid is "" for a page with no
    Wikidata item. A title whose call failed is absent from the result.
    """
    out = {}
    todo = []
    with _api_cache_lock:
        for t in sorted(set(titles)):
            hit = _api_cache.get((lang, t))
            if hit is not None:
                out[t] = hit
            else:
                todo.append(t)
    for i in range(0, len(todo), API_BATCH):
        batch = todo[i:i + API_BATCH]
        try:
            rate_limit()
            r = get_session().get(f"https://{lang}.wikipedia.org/w/api.php", params={
                "action": "query", "titles": "|".join(batch), "redirects": 1, "prop": "pageprops",
                "ppprop": "wikibase_item", "format": "json", "formatversion": 2}, timeout=60)
            r.raise_for_status()
            data = r.json()
        except Exception as err:
            print(f"Link resolution API error ({lang}, {len(batch)} titles): {err}")
            continue
        if "error" in data:
            print(f"Link resolution API error ({lang}): {data['error'].get('code')}")
            continue
        q = data.get("query", {})
        norm = {n["from"]: n["to"] for n in q.get("normalized", [])}
        redir = {n["from"]: n["to"] for n in q.get("redirects", [])}
        pages = {p["title"]: p for p in q.get("pages", [])}
        with _api_cache_lock:
            if len(_api_cache) > _API_CACHE_MAX:
                _api_cache.clear()
            for t in batch:
                t1 = norm.get(t, t)
                t2 = redir.get(t1, t1)
                p = pages.get(t2)
                if p is None:
                    continue
                res = ((p.get("pageprops") or {}).get("wikibase_item", ""), t2, int(t2 != t1))
                out[t] = res
                _api_cache[(lang, t)] = res
    return out


def api_titles(links):
    """The titles the API is asked about: every live link, red links and date links aside."""
    return {l["target"] for l in links if not l["redlink"] and not l["is_date"]}


def resolve_link_qids(cursor, lang, links, api_results=None):
    """Fill target_qid, final_title, via_redirect, resolved_by on each link, in place.

    Red links are not resolved. The local title join comes first (84 % of links on the
    prototype pages); the API answer fills the rest, date links excepted, which only point
    to calendar pages.

    Since 1.1.1 the crawler passes ``api_results``, computed beforehand in the fetch worker
    for every title of ``api_titles`` (``resolve_by_api``): the main thread then makes no
    network call, and a title the worker could not resolve stays unresolved. Without it,
    the API is called here for the titles the join left (1.1.0 behaviour, kept for tests
    and one-off use). In 1.1.0 that call ran on the main thread, which also does every
    database write, and cut the crawl from 53 to 38 pages a minute on 2026-10-15.
    """
    for l in links:
        l.update({"target_qid": None, "final_title": None, "via_redirect": 0, "resolved_by": None})
    live = [l for l in links if not l["redlink"]]
    local = resolve_by_title(cursor, lang, [l["target"] for l in live])
    rest = set()
    for l in live:
        qid = local.get(l["target"])
        if qid:
            l.update({"target_qid": qid, "final_title": l["target"], "resolved_by": "title"})
        elif not l["is_date"]:
            rest.add(l["target"])
    if api_results is not None:
        api = api_results
    else:
        api = resolve_by_api(lang, rest) if rest else {}
    for l in live:
        if l["resolved_by"] is None and not l["is_date"] and l["target"] in api:
            qid, final, via = api[l["target"]]
            l.update({"target_qid": qid or None, "final_title": final, "via_redirect": via, "resolved_by": "api"})
    return links
