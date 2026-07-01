from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "fetch_sec_13f_recent.py"


def load_module():
    spec = importlib.util.spec_from_file_location("fetch_sec_13f_recent", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_feed_keeps_recent_13f_entries() -> None:
    module = load_module()
    feed = """<?xml version="1.0" encoding="ISO-8859-1" ?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <entry>
        <title>13F-HR/A - JANA Partners Management, LP (0001998597) (Filer)</title>
        <link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/1998597/000090266426002957/0000902664-26-002957-index.htm"/>
        <summary type="html">&lt;b&gt;Filed:&lt;/b&gt; 2026-06-30 &lt;b&gt;AccNo:&lt;/b&gt; 0000902664-26-002957</summary>
        <updated>2026-06-30T16:15:23-04:00</updated>
      </entry>
      <entry>
        <title>13F-HR - Old Manager (0000000001) (Filer)</title>
        <link rel="alternate" type="text/html" href="https://www.sec.gov/old-index.htm"/>
        <summary type="html">&lt;b&gt;Filed:&lt;/b&gt; 2026-06-01 &lt;b&gt;AccNo:&lt;/b&gt; 0000000000-26-000001</summary>
        <updated>2026-06-01T16:15:23-04:00</updated>
      </entry>
    </feed>
    """

    rows = module.parse_feed(feed, lookback_hours=48)

    assert len(rows) == 1
    assert rows[0].form == "13F-HR/A"
    assert rows[0].manager == "JANA Partners Management, LP"
    assert rows[0].cik == "1998597"
    assert rows[0].accession == "0000902664-26-002957"


def test_extract_information_table_links_skips_xsl_viewer() -> None:
    module = load_module()
    index_html = """
    <table>
      <tr><td><a href="/Archives/edgar/data/1998597/xslForm13F_X02/infotable.xml">infotable.html</a></td><td>INFORMATION TABLE</td></tr>
      <tr><td><a href="/Archives/edgar/data/1998597/000090266426002957/infotable.xml">infotable.xml</a></td><td>INFORMATION TABLE</td></tr>
      <tr><td><a href="/Archives/edgar/data/1998597/000090266426002957/primary_doc.xml">primary_doc.xml</a></td><td>13F-HR/A</td></tr>
    </table>
    """

    links = module.extract_information_table_links(
        index_html,
        "https://www.sec.gov/Archives/edgar/data/1998597/000090266426002957/0000902664-26-002957-index.htm",
    )

    assert links == ["https://www.sec.gov/Archives/edgar/data/1998597/000090266426002957/infotable.xml"]
