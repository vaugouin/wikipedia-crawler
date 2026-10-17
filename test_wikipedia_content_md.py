"""Offline tests for wikipedia_content_md.py and the 1.1.0 link/reference rows of the writer.

No network, no database: small HTML pages in both shapes ``action=parse`` serves (legacy
parser output, and the Parsoid one seen on 2026-10-15), a fake cursor for the title join,
and the API resolver stubbed out.

Usage (pytest is not required):
    python test_wikipedia_content_md.py
    python -m pytest test_wikipedia_content_md.py
"""

from bs4 import BeautifulSoup

import wikipedia_content_md as wcmd

LEGACY = """<body><div class="mw-parser-output">
<p><b>Sergio Leone</b> était un <a href="/wiki/R%C3%A9alisateur">réalisateur</a> né le
<span class="nowrap date-lien"><a href="/wiki/3_janvier">3</a> <a href="/wiki/Janvier_1929">janvier</a>
<a href="/wiki/1929_au_cin%C3%A9ma">1929</a></span> à <a href="/wiki/Rome">Rome</a>.<sup class="reference"><a href="#cite_note-1">[1]</a></sup></p>
<div class="mw-heading mw-heading2"><h2 id="Carri.C3.A8re">Carrière</h2><span class="mw-editsection">[modifier]</span></div>
<p>Il tourne <i><a href="/wiki/Pour_une_poign%C3%A9e_de_dollars" title="x">Pour une poignée de dollars</a></i>
et <a href="/w/index.php?title=Film_inexistant&amp;action=edit&amp;redlink=1" class="new">un film perdu</a>,
puis vit <a href="/wiki/Mai_68">Mai 68</a>.<sup class="reference"><a href="#cite_note-1">[1]</a></sup><sup class="reference"><a href="#cite_note-livre-2">[2]</a></sup></p>
<div class="mw-heading mw-heading3"><h3>Fiche technique</h3></div>
<ul><li>Format : couleur (<a href="/wiki/Technicolor">Technicolor</a>)</li><li>Durée : 99 minutes</li></ul>
<div class="mw-heading mw-heading3"><h3>Vide</h3></div>
<div class="navbox"><a href="/wiki/Navbox_link">nav</a></div>
<div class="mw-heading mw-heading2"><h2>Notes et références</h2></div>
<div class="mw-references-wrap"><ol class="references">
<li id="cite_note-1"><span class="reference-text"><a class="external text" href="https://example.org/a">Article A</a>, 2004.</span></li>
<li id="cite_note-livre-2"><span class="reference-text">Smith 2004, p. 3.</span></li>
</ol></div>
</div></body>"""

PARSOID = """<body><section data-mw-section-id="0">
<p>Twin Peaks is a series by <a rel="mw:WikiLink" href="./David_Lynch">David Lynch</a>.<sup class="mw-ref reference"><a href="./Twin_Peaks#cite_note-3"><span>[3]</span></a></sup></p>
</section>
<section data-mw-section-id="1"><div class="mw-heading mw-heading2"><h2 id="Cast">Cast</h2></div>
<table class="wikitable"><tr><th>Actor</th><th>Role</th></tr>
<tr><td><a href="https://en.wikipedia.org/wiki/Kyle_MacLachlan">Kyle MacLachlan</a></td><td>Dale Cooper</td></tr></table>
</section>
<ol class="references"><li id="cite_note-3"><span class="reference-text"><cite>A book</cite></span></li></ol>
</body>"""


def _convert(html, lang):
    return wcmd.convert_page(BeautifulSoup(html, "html.parser"), lang)


def test_sections_numbered_and_empty_ones_dropped():
    r = _convert(LEGACY, "fr")
    titles = [(o, t) for o, t, _, _ in r["sections"]]
    # "Carrière - Vide" holds only a navbox and "Notes et références" only the reference list: both empty, both gone.
    assert titles == [(1, "Intro"), (2, "Carrière"), (3, "Carrière - Fiche technique")], titles


def test_markdown_and_repaired_text():
    r = _convert(LEGACY, "fr")
    intro_text, intro_md = r["sections"][0][2], r["sections"][0][3]
    assert "[réalisateur](Réalisateur)" in intro_md
    assert "**Sergio Leone**" in intro_md
    assert "[1]" not in intro_md and "[1]" not in intro_text
    assert "réalisateur" in intro_text and "](" not in intro_text and "**" not in intro_text
    fiche_text = r["sections"][2][2]
    # The writer's "- Format" extraction needs the bullet and the plain words.
    assert "- Format : couleur (Technicolor)" in fiche_text, fiche_text


def test_links_dates_and_redlinks():
    r = _convert(LEGACY, "fr")
    links = {l["target"]: l for l in r["links"]}
    assert "Navbox link" not in links
    assert links["3 janvier"]["is_date"] == 1
    assert links["Janvier 1929"]["is_date"] == 1
    assert links["1929 au cinéma"]["is_date"] == 1
    assert links["Rome"]["is_date"] == 0
    assert links["Mai 68"]["is_date"] == 0          # an event, not a calendar page
    assert links["Film inexistant"]["redlink"] == 1
    sec2 = [l for l in r["links"] if l["section"] == 2]
    assert [l["display_order"] for l in sec2] == list(range(1, len(sec2) + 1))
    assert sec2[0]["target"] == "Pour une poignée de dollars"


def test_references_per_section_with_text():
    r = _convert(LEGACY, "fr")
    refs = [(x["section"], x["display_order"], x["note"], x["url"], x["text"]) for x in r["refs"]]
    assert refs == [
        (1, 1, "cite_note-1", "https://example.org/a", "Article A , 2004."),
        (2, 1, "cite_note-1", "https://example.org/a", "Article A , 2004."),
        (2, 2, "cite_note-livre-2", "", "Smith 2004, p. 3."),
    ], refs


def test_parsoid_shape():
    r = _convert(PARSOID, "en")
    assert [t for _, t, _, _ in r["sections"]] == ["Intro", "Cast"]
    targets = [(l["section"], l["target"]) for l in r["links"]]
    assert targets == [(1, "David Lynch"), (2, "Kyle MacLachlan")], targets
    assert all(l["is_date"] == 0 for l in r["links"])
    assert "| [Kyle MacLachlan](Kyle_MacLachlan) | Dale Cooper |" in r["sections"][1][3]
    assert "Kyle MacLachlan | Dale Cooper" in r["sections"][1][2]
    assert [(x["note"], x["title"]) for x in r["refs"]] == [("cite_note-3", "A book")]


class FakeCursor:
    """Answers the title join like MariaDB's accent- and case-insensitive collation would."""

    def __init__(self, rows):
        self.rows = rows
        self.result = []

    def execute(self, sql, params):
        lang, titles = params[0], {t.lower() for t in params[1:]}
        self.result = [r for r in self.rows if r[0] == lang and r[1].lower() in titles]

    def fetchall(self):
        return [{"ID_WIKIDATA": q, "WIKIPEDIA_PAGE_TITLE": t} for _, t, q in self.result]


def test_resolution_title_join_then_api(monkeypatch=None):
    r = _convert(LEGACY, "fr")
    cursor = FakeCursor([
        ("fr", "Rome", "Q220"),
        ("fr", "réalisateur", "Q999"),       # wrong case: the exact filter must reject it
        ("fr", "Technicolor", "Q1"), ("fr", "Technicolor", "Q2"),   # ambiguous: left to the API
    ])
    asked = []

    def fake_api(lang, titles):
        asked.extend(sorted(titles))
        return {t: (f"Q-{t}", t, 0) for t in titles if t != "Pour une poignée de dollars"}

    saved = wcmd.resolve_by_api
    wcmd.resolve_by_api = fake_api
    try:
        wcmd.resolve_link_qids(cursor, "fr", r["links"])
    finally:
        wcmd.resolve_by_api = saved
    links = {l["target"]: l for l in r["links"]}
    assert (links["Rome"]["target_qid"], links["Rome"]["resolved_by"]) == ("Q220", "title")
    assert links["Réalisateur"]["resolved_by"] == "api"
    assert links["Technicolor"]["resolved_by"] == "api"
    assert links["Film inexistant"]["resolved_by"] is None          # red link: never resolved
    assert links["3 janvier"]["resolved_by"] is None                # date link: no API call
    assert links["Pour une poignée de dollars"]["target_qid"] is None
    assert "3 janvier" not in asked and "Rome" not in asked and "Film inexistant" not in asked


def test_resolution_with_worker_api_results():
    """1.1.1: the worker's API answers are used as given, and the main thread calls nothing."""
    r = _convert(LEGACY, "fr")
    titles = wcmd.api_titles(r["links"])
    assert "Film inexistant" not in titles and "3 janvier" not in titles and "Rome" in titles
    worker = {t: (f"Q-{t}", t, 0) for t in titles if t != "Réalisateur"}   # "Réalisateur": the worker's call failed

    def no_network(lang, titles):
        raise AssertionError("the main thread must not call the API")

    saved = wcmd.resolve_by_api
    wcmd.resolve_by_api = no_network
    try:
        wcmd.resolve_link_qids(FakeCursor([("fr", "Rome", "Q220")]), "fr", r["links"], api_results=worker)
    finally:
        wcmd.resolve_by_api = saved
    links = {l["target"]: l for l in r["links"]}
    assert (links["Rome"]["target_qid"], links["Rome"]["resolved_by"]) == ("Q220", "title")   # the join still comes first
    assert (links["Technicolor"]["target_qid"], links["Technicolor"]["resolved_by"]) == ("Q-Technicolor", "api")
    assert links["Réalisateur"]["resolved_by"] is None
    assert links["3 janvier"]["resolved_by"] is None


def test_writer_rows_and_prune():
    import wikipedia_page_writer as wpw
    r = _convert(LEGACY, "fr")
    for l in r["links"]:
        l.update({"target_qid": None, "final_title": None, "via_redirect": 0, "resolved_by": None})
    rows, counts = wpw.f_buildlinkrows(r["links"], "Q1", "fr", "person")
    assert {(x["SECTION_DISPLAY_ORDER"], x["DISPLAY_ORDER"]) for x in rows} == \
        {(l["section"], l["display_order"]) for l in r["links"]}
    assert all(x["FRAGMENT"] is None for x in rows)
    cond = wpw._prunecondition(3, counts)
    assert cond.startswith("(SECTION_DISPLAY_ORDER > 3 OR ")
    assert f"(SECTION_DISPLAY_ORDER = 3 AND DISPLAY_ORDER > {counts[3]})" in cond
    rrows, rcounts = wpw.f_buildrefrows(r["refs"], "Q1", "fr", "person")
    assert rcounts == {1: 1, 2: 2}
    assert "(SECTION_DISPLAY_ORDER = 3 AND DISPLAY_ORDER > 0)" in wpw._prunecondition(3, rcounts)
    assert rrows[2]["URL"] is None and rrows[2]["REFERENCE_TEXT"] == "Smith 2004, p. 3."


def test_html_comments_dropped():
    # 1.1.0 to 1.1.2 wrote the parser report comment out as text, in the page's last section
    # (a section holding nothing else came out as a "References" section of report lines).
    report = "<!-- \nNewPP limit report\nParsed by mw-api-int\nCPU time usage: 0.9 seconds\n-->"
    html = ("<body><div class=\"mw-parser-output\"><p>Intro <!-- inline note -->text.</p>"
            "<h2>Liens externes</h2><ul><li><a href=\"https://example.org\">Site</a></li></ul>"
            "<h2>References</h2>" + report + "</div>" + report + "</body>")
    r = _convert(html, "en")
    titles = [t for _, t, _, _ in r["sections"]]
    assert titles == ["Intro", "Liens externes"], titles
    assert all("NewPP" not in md and "NewPP" not in text for _, _, text, md in r["sections"])
    assert r["sections"][0][3] == "Intro text.", r["sections"][0][3]


def test_self_links_are_text():
    # 1.1.5: Parsoid (and Wikimedia Enterprise) write [[#Season 1|1]] as "./Page#Season_1".
    body = ("<section data-mw-section-id=\"0\"><p>Seasons: <a rel=\"mw:WikiLink\" href=\"./Twin_Peaks#Season_1\">1</a>, "
            "<a rel=\"mw:WikiLink\" href=\"./twin_Peaks\">this page</a> and <a rel=\"mw:WikiLink\" href=\"./David_Lynch\">Lynch</a>.</p></section>")
    for html, title in (("<body>" + body + "</body>", "Twin Peaks"),
                        ("<html><head><title>x</title><link rel=\"dc:isVersionOf\" href=\"//en.wikipedia.org/wiki/Twin_Peaks\"/>"
                         "</head><body>" + body + "</body></html>", None)):
        r = wcmd.convert_page(BeautifulSoup(html, "html.parser"), "en", title)
        assert [l["target"] for l in r["links"]] == ["David Lynch"], r["links"]
        assert r["sections"][0][3] == "Seasons: 1, this page and [Lynch](David_Lynch).", r["sections"][0][3]


def test_inline_runs_in_a_div_are_one_line():
    # 1.1.5: template output puts text straight into a <div>; each text node, <i> or spacing
    # <span> used to start a line of its own. A <br> still ends the line.
    html = ("<body><div class=\"mw-parser-output\"><h2>Épisodes</h2><div style=\"padding:5px\">Connue comme "
            "<i>La Compagnie</i>, elle publie une annonce<span typeof=\"mw:DisplaySpace\"> </span>:<br/>"
            "«<span typeof=\"mw:DisplaySpace\"> </span>Je peux aider.<span typeof=\"mw:DisplaySpace\"> </span>»"
            "</div><b>Commentaires</b><br/><div>Dédié à <a href=\"/wiki/Richard_Lindheim\">Richard Lindheim</a>.</div>"
            "<h2>Vide</h2><p>‌</p></div></body>")
    r = _convert(html, "fr")
    assert [t for _, t, _, _ in r["sections"]] == ["Épisodes"], r["sections"]    # "Vide" holds only a U+200C
    assert r["sections"][0][3].split("\n") == [
        "Connue comme *La Compagnie*, elle publie une annonce :",     # no-break spaces collapse, as before
        "« Je peux aider. »",
        "**Commentaires**",
        "Dédié à [Richard Lindheim](Richard_Lindheim).",
    ], r["sections"][0][3]


def test_snapshot_payload_keeps_images():
    # 1.1.6: wikipedia_dump_ingest.py writes pages with no image list; their image rows stay.
    import wikipedia_page_writer as wpw
    sql, saved = [], {}

    class Cur:
        def execute(self, s, params=None):
            sql.append(s)

        def fetchone(self):
            return None

    class Conn:
        def commit(self):
            pass

    names = ("f_sqlupdatearray", "f_sqlbulkupsert", "connectioncp", "f_setservervariable")
    saved = {n: getattr(wpw.cp, n, None) for n in names}
    wpw.cp.f_sqlupdatearray = lambda *a, **k: None
    wpw.cp.f_sqlbulkupsert = lambda table, *a, **k: sql.append("UPSERT " + table)
    wpw.cp.connectioncp = Conn()
    wpw.cp.f_setservervariable = lambda *a, **k: None
    saved_links = wpw.f_writelinksandrefs
    wpw.f_writelinksandrefs = lambda *a, **k: None
    try:
        r = _convert(LEGACY, "fr")
        payload = {"site_key": "frwiki", "page_title": "Sergio Leone", "page_url": "", "main_image_url": "",
                   "main_image_checked": False, "page_images": [], "keep_images": True, "http_status": 200,
                   "success": True, "has_content": True, "sections": r["sections"], "links": [], "refs": [], "api_qids": {}}
        wpw.f_writelangtodb(payload, 1, "Q1", "fr", "person", 202, "T_WC_WIKIDATA_PERSON_V1", "WIKIPEDIA_PROFILE_PATH",
                            Cur(), 0, 0, blnwritecounters=False)
        assert not any("PAGE_LANG_IMAGE" in s for s in sql), sql
        assert "UPSERT T_WC_WIKIPEDIA_PAGE_LANG_SECTION" in sql, sql
        del payload["keep_images"]
        sql.clear()
        wpw.f_writelangtodb(payload, 1, "Q1", "fr", "person", 202, "T_WC_WIKIDATA_PERSON_V1", "WIKIPEDIA_PROFILE_PATH",
                            Cur(), 0, 0, blnwritecounters=False)
        assert any(s.startswith("DELETE FROM T_WC_WIKIPEDIA_PAGE_LANG_IMAGE") for s in sql), sql   # the crawl's rule
    finally:
        for n, v in saved.items():
            setattr(wpw.cp, n, v)
        wpw.f_writelinksandrefs = saved_links


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"{len(tests)} tests passed")
