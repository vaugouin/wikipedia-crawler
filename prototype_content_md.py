"""Prototype: Wikipedia sections as Markdown (CONTENT_MD) plus a link table, beside today's plain text.

Not wired into the crawler. Fetches the same ``action=parse`` HTML the crawler uses,
splits it with the same H2/H3 rules (WIKIPEDIA-CRAWLER-016), and for every section
writes three things side by side so the formats can be judged on real pages:

- TEXT: today's ``CONTENT``, produced by ``extract_titles_and_text`` itself;
- MD:   a Markdown rendering that keeps links, bold/italic, nested lists, tables,
        and drops footnote call-outs, edit links, navboxes and portal banners;
- LINKS: one row per internal link (section, anchor, target title), resolved to a
        Wikidata Qid through the API (redirects followed);
- REPAIRED: the plain text derived from MD (round 2, decision 2 of 2026-10-15): tables kept
        as plain rows, footnote call-outs gone, nested lists written once, "- " bullets kept
        so the Fiche technique "- Format" extraction still finds its line;
- REFS: one row per footnote cited in a section (round 2, decision 4): URL and title, the
        provenance kept out of the Markdown.

Usage:
    python prototype_content_md.py OUTDIR "en:Dollars Trilogy" "fr:Trilogie du dollar" ...

Writes OUTDIR/<lang>-<slug>.md (sections), -text.md (today), -repaired.md, -links.csv,
-refs.csv, .html (raw parse HTML) and OUTDIR/summary.json (sizes, counts).
"""

import csv
import json
import re
import sys
import urllib.parse
from pathlib import Path

from bs4 import BeautifulSoup, NavigableString, Tag

from wikipedia_crawler_helpers import (
    NO_SUBSPLIT_SECTION_TITLES,
    _normalize_heading_separator,
    extract_titles_and_text,
)
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
    # "[edit]" style links and thumbnails
    if tag.name == "div" and (_classes(tag) & {"thumb", "tsingle", "gallery", "image"}):
        return True
    if tag.name == "ul" and "gallery" in _classes(tag):
        return True
    return False


def _wiki_target(href):
    """Return (title, fragment) for an internal article link, else None.

    Accepts the legacy form ("/wiki/Title") and the Parsoid one that action=parse returns
    for nine hours on 2026-10-15, and may again ("https://<lang>.wikipedia.org/wiki/Title", or "./Title").
    """
    href = re.sub(r"^https?://[a-z-]+\.wikipedia\.org(?=/wiki/)", "", href or "")
    if href.startswith("./"):
        href = "/wiki/" + href[2:]
    if not href.startswith("/wiki/"):
        return None
    path = href[len("/wiki/"):]
    path, _, fragment = path.partition("#")
    path = path.split("?", 1)[0]          # "?action=edit&redlink=1"; a real "?" in a title is %3F
    title = urllib.parse.unquote(path).replace("_", " ")
    # Namespaced pages (File:, Help:, Category:, Portail:, Fichier:...) are not entities.
    if re.match(r"^[^ :]+:", title) and not title.startswith(":"):
        return None
    return title, urllib.parse.unquote(fragment).replace("_", " ")


class MarkdownRenderer:
    """Turn one section's HTML blocks into Markdown, collecting the links it meets."""

    def __init__(self):
        self.links = []          # dicts: anchor, target, fragment, redlink
        self.refs = []           # dicts: section, note (cite_note id), label
        self.section = None

    # -- inline --------------------------------------------------------------
    def inline(self, node):
        if isinstance(node, NavigableString):
            return re.sub(r"\s+", " ", str(node))
        if isinstance(node, Tag) and node.name == "sup" and "reference" in _classes(node):
            # Footnote call-out: out of the text, into the reference table.
            a = node.find("a", href=True)
            if a and a["href"].startswith("#"):
                note = a["href"][1:]
                if not any(r["section"] == self.section and r["note"] == note for r in self.refs):
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
                t2 = _wiki_target(href)
                target = urllib.parse.unquote(m.group(1)).replace("_", " ") if m else (t2[0] if t2 else text)
                self.links.append({"section": self.section, "anchor": text, "target": target, "fragment": "", "redlink": 1})
                return text
            target = _wiki_target(href)
            if target:
                title, fragment = target
                self.links.append({"section": self.section, "anchor": text, "target": title, "fragment": fragment, "redlink": 0})
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
                colspan = int(re.sub(r"\D", "", cell.get("colspan", "1")) or 1)
                rowspan = int(re.sub(r"\D", "", cell.get("rowspan", "1")) or 1)
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


def split_sections_md(soup):
    """Same split as extract_titles_and_text, walking top-level blocks only (no double visit)."""
    root = soup.find("div", class_="mw-parser-output") or soup.body
    renderer = MarkdownRenderer()
    sections = []
    h2_title, title, lines = None, "Intro", []
    renderer.section = title

    def flush():
        md = "\n".join(lines)
        md = re.sub(r"\n{3,}", "\n\n", md).strip()
        if md:
            sections.append((title, md))

    def top_blocks(parent):
        # Parsoid HTML (served by action=parse on 2026-10-15, 10:00 to 19:00 Paris) wraps each section in <section>: walk through them.
        for c in parent.children:
            if isinstance(c, Tag) and c.name == "section":
                yield from top_blocks(c)
            else:
                yield c

    for node in top_blocks(root):
        heading = None
        if isinstance(node, Tag):
            if node.name in ("h2", "h3"):
                heading = node
            elif node.name == "div" and "mw-heading" in _classes(node):
                heading = node.find(["h2", "h3"])
        if heading is not None and heading.name == "h2":
            flush()
            h2_title = _normalize_heading_separator(heading.get_text().strip())
            title, lines = h2_title, []
            renderer.section = title
            continue
        if heading is not None and heading.name == "h3" and h2_title is not None \
                and h2_title.lower() not in NO_SUBSPLIT_SECTION_TITLES:
            flush()
            h3 = _normalize_heading_separator(heading.get_text().strip())
            title, lines = f"{h2_title} - {h3}", []
            renderer.section = title
            continue
        if heading is not None:  # h3 inside an end-matter section stays inline
            lines += [f"### {heading.get_text().strip()}", ""]
            continue
        lines += renderer.block(node)
    flush()
    return sections, renderer.links, renderer.refs


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


def format_line(content):
    """The writer's Fiche technique extraction (wikipedia_page_writer.py), to check it still works."""
    i = content.find("\n- Format")
    if i == -1:
        i = content.find("- Format")
        if i == -1:
            return ""
    else:
        i += 1
    j = content.find("\n- ", i + 2)
    return content[i:j if j != -1 else len(content)].strip()


def fetch_parse_html(lang, title):
    rate_limit()
    r = get_session().get(f"https://{lang}.wikipedia.org/w/api.php", params={
        "action": "parse", "page": title, "prop": "text", "formatversion": 2,
        "format": "json", "redirects": 1}, timeout=60)
    r.raise_for_status()
    return r.json()["parse"]["text"]


def resolve_qids(lang, titles):
    """Map link titles to Qids, following redirects. Returns {title: (qid, final_title, via_redirect)}."""
    out = {}
    titles = sorted(set(titles))
    for i in range(0, len(titles), 50):
        batch = titles[i:i + 50]
        rate_limit()
        r = get_session().get(f"https://{lang}.wikipedia.org/w/api.php", params={
            "action": "query", "titles": "|".join(batch), "redirects": 1, "prop": "pageprops",
            "ppprop": "wikibase_item", "format": "json", "formatversion": 2}, timeout=60)
        r.raise_for_status()
        q = r.json().get("query", {})
        norm = {n["from"]: n["to"] for n in q.get("normalized", [])}
        redir = {n["from"]: n["to"] for n in q.get("redirects", [])}
        pages = {p["title"]: p for p in q.get("pages", [])}
        for t in batch:
            t1 = norm.get(t, t)
            t2 = redir.get(t1, t1)
            p = pages.get(t2, {})
            out[t] = ((p.get("pageprops") or {}).get("wikibase_item", ""), t2, int(t2 != t1))
    return out


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def main():
    outdir = Path(sys.argv[1])
    outdir.mkdir(parents=True, exist_ok=True)
    summary = []
    for spec in sys.argv[2:]:
        lang, title = spec.split(":", 1)
        html = fetch_parse_html(lang, title)
        soup = BeautifulSoup("<body>" + html + "</body>", "html.parser")
        text_sections = extract_titles_and_text(soup=BeautifulSoup("<body>" + html + "</body>", "html.parser"))
        text_sections = [(t, c.replace("[edit]", "")) for t, c in text_sections]
        refnotes = collect_references(BeautifulSoup("<body>" + html + "</body>", "html.parser"))
        md_sections, links, refs = split_sections_md(soup)
        repaired = [(t, md_to_text(m)) for t, m in md_sections]
        for r in refs:
            r.update(refnotes.get(r["note"], {"url": "", "title": "", "text": ""}))
        qids = resolve_qids(lang, [l["target"] for l in links if not l["redlink"]])
        for l in links:
            qid, final, via = qids.get(l["target"], ("", l["target"], 0))
            l.update({"qid": qid, "final_title": final, "via_redirect": via})

        base = f"{lang}-{slug(title)}"
        (outdir / f"{base}.html").write_text(html, encoding="utf-8")
        text_by_title = dict(text_sections)
        with open(outdir / f"{base}.md", "w", encoding="utf-8") as f:
            f.write(f"# {title} ({lang}), CONTENT_MD prototype\n\n")
            for i, (t, md) in enumerate(md_sections):
                f.write(f"## [{i}] {t}\n\n{md}\n\n")
        with open(outdir / f"{base}-text.md", "w", encoding="utf-8") as f:
            f.write(f"# {title} ({lang}), today's CONTENT\n\n")
            for i, (t, c) in enumerate(text_sections):
                f.write(f"## [{i}] {t}\n\n```text\n{c}\n```\n\n")
        with open(outdir / f"{base}-repaired.md", "w", encoding="utf-8") as f:
            f.write(f"# {title} ({lang}), repaired CONTENT\n\n")
            for i, (t, c) in enumerate(repaired):
                f.write(f"## [{i}] {t}\n\n```text\n{c}\n```\n\n")
        with open(outdir / f"{base}-refs.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["section", "note", "label", "url", "title", "text"])
            w.writeheader()
            w.writerows(refs)
        with open(outdir / f"{base}-links.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["section", "anchor", "target", "fragment", "redlink", "final_title", "via_redirect", "qid"])
            w.writeheader()
            w.writerows(links)

        n_int = sum(1 for l in links if not l["redlink"])
        summary.append({
            "page": f"{lang}:{title}",
            "file": base,
            "text_sections": len(text_sections),
            "md_sections": len(md_sections),
            "text_titles_missing_in_md": [t for t in text_by_title if t not in dict(md_sections)],
            "md_titles_missing_in_text": [t for t, _ in md_sections if t not in text_by_title],
            "chars_html": len(html),
            "chars_text": sum(len(c) for _, c in text_sections),
            "chars_md": sum(len(m) for _, m in md_sections),
            "links_total": len(links),
            "links_internal": n_int,
            "links_red": sum(1 for l in links if l["redlink"]),
            "links_with_qid": sum(1 for l in links if l.get("qid")),
            "links_via_redirect": sum(1 for l in links if l.get("via_redirect")),
            "distinct_qids": len({l["qid"] for l in links if l.get("qid")}),
            "md_tables": sum(m.count("\n|---") for _, m in md_sections),
            "chars_repaired": sum(len(c) for _, c in repaired),
            "refs_cited": len(refs),
            "refs_distinct": len({r["note"] for r in refs}),
            "refs_with_url": len({r["note"] for r in refs if r["url"]}),
            "refs_unresolved": len({r["note"] for r in refs if r["note"] not in refnotes}),
            "format_today": next((format_line(c) for t, c in text_sections if t == "Fiche technique"), None),
            "format_repaired": next((format_line(c) for t, c in repaired if t == "Fiche technique"), None),
        })
        print(json.dumps(summary[-1], ensure_ascii=False))
    (outdir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
