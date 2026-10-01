#!/usr/bin/env python3
"""Print a guided, read-only snapshot from public SEC EDGAR data endpoints.

Run (include your real name and contact email in the SEC User-Agent):
  python examples/sec_edgar_snapshot.py AAPL --user-agent "Your Name your@email.com"

Uses only Python's standard library. Public data.sec.gov APIs do not require an
API key. This script does not access authenticated filer-management or filing
submission APIs. SEC responses and available XBRL tags vary by issuer.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
DATA_SEC = "https://data.sec.gov"
SEC_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
MIN_REQUEST_INTERVAL = 0.20  # 5 requests/second; SEC's current maximum is 10.


class SecClient:
    """Small paced JSON client that sends the SEC-required identifying header."""

    def __init__(self, user_agent: str, timeout: float = 30.0) -> None:
        self.user_agent = user_agent
        self.timeout = timeout
        self._last_request = 0.0

    def get_json(self, url: str) -> Any:
        delay = MIN_REQUEST_INTERVAL - (time.monotonic() - self._last_request)
        if delay > 0:
            time.sleep(delay)
        request = Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            },
        )
        self._last_request = time.monotonic()
        with urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))


def heading(title: str, explanation: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'-' * 78}\n{explanation}")


def show(value: Any, max_items: int = 8) -> None:
    if value is None:
        print("  (no data returned)")
    elif isinstance(value, dict):
        if not value:
            print("  (empty object)")
        for key, item in list(value.items())[:max_items]:
            print(f"  {key}: {item}")
        if len(value) > max_items:
            print(f"  ... {len(value) - max_items} more fields")
    elif isinstance(value, list):
        if not value:
            print("  (empty list)")
        for item in value[:max_items]:
            print(f"  - {item}")
        if len(value) > max_items:
            print(f"  ... {len(value) - max_items} more items")
        print(f"  Items: {len(value)}")
    else:
        print(f"  {value}")


def try_section(title: str, explanation: str, fetch: Any, render: bool = True) -> Any:
    heading(title, explanation)
    try:
        result = fetch()
        if render:
            show(result)
        elif isinstance(result, dict):
            print(f"  Received JSON object with fields: {', '.join(result.keys())}")
        else:
            print(f"  Received {type(result).__name__}")
        return result
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, KeyError, ValueError) as exc:
        print(f"  Endpoint unavailable: {type(exc).__name__}: {exc}")
        return None
    except Exception as exc:
        print(f"  Could not read this response: {type(exc).__name__}: {exc}")
        return None


def resolve_company(client: SecClient, ticker: str) -> dict[str, Any]:
    directory = client.get_json(SEC_TICKERS_URL)
    matches = [row for row in directory.values() if row.get("ticker", "").upper() == ticker.upper()]
    if not matches:
        raise ValueError(f"Ticker {ticker!r} was not found in the SEC company ticker directory")
    match = matches[0]
    return {
        "ticker": match["ticker"],
        "title": match["title"],
        "cik": int(match["cik_str"]),
        "cik_padded": f"{int(match['cik_str']):010d}",
    }


def latest_company_facts(payload: dict[str, Any], tags: list[str]) -> list[dict[str, Any]]:
    """Make a compact, comparable preview from the issuer-wide XBRL response."""
    output: list[dict[str, Any]] = []
    taxonomies = payload.get("facts", {})
    for tag in tags:
        fact = taxonomies.get("us-gaap", {}).get(tag)
        if not fact:
            output.append({"tag": tag, "status": "not reported under this standard tag"})
            continue
        units = fact.get("units", {})
        unit_name = next(iter(units), None)
        rows = units.get(unit_name, []) if unit_name else []
        rows = [row for row in rows if row.get("form") in {"10-K", "10-Q", "20-F", "40-F"}]
        rows.sort(key=lambda row: (row.get("end", ""), row.get("filed", "")), reverse=True)
        output.append({"tag": tag, "label": fact.get("label"), "unit": unit_name, "latest_facts": rows[:4]})
    return output


def completed_quarter_frame() -> str:
    today = date.today()
    quarter = (today.month - 1) // 3
    if quarter == 0:
        year, quarter = today.year - 1, 4
    else:
        year = today.year
    return f"CY{year}Q{quarter}I"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ticker", nargs="?", default="AAPL", help="company ticker symbol (default: AAPL)")
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("SEC_USER_AGENT"),
        help='SEC contact header, e.g. "Jane Doe jane@example.com" (or set SEC_USER_AGENT)',
    )
    parser.add_argument("--fact-tag", default="Assets", help="us-gaap tag for the company-concept example (default: Assets)")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds (default: 30)")
    args = parser.parse_args()

    if not args.user_agent or "@" not in args.user_agent:
        parser.error("provide a real contact email using --user-agent or SEC_USER_AGENT")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", args.fact_tag):
        parser.error("--fact-tag must be a plain XBRL tag name, such as Assets or Revenues")

    client = SecClient(args.user_agent, args.timeout)
    symbol = args.ticker.upper()
    print(f"SEC EDGAR public-data snapshot | {date.today().isoformat()} | ticker={symbol}")
    print("Read-only requests; standard-library Python; one request is paced every 0.20 seconds.")

    heading("1. Ticker to company identity", "SEC company_tickers.json maps a ticker to the issuer name and 10-digit CIK.")
    try:
        company = resolve_company(client, symbol)
        show(company)
    except Exception as exc:
        print(f"Could not resolve ticker: {type(exc).__name__}: {exc}")
        raise SystemExit(1) from exc

    cik = company["cik_padded"]
    cik_number = str(company["cik"])
    submissions_url = f"{DATA_SEC}/submissions/CIK{cik}.json"
    facts_url = f"{DATA_SEC}/api/xbrl/companyfacts/CIK{cik}.json"
    concept_url = f"{DATA_SEC}/api/xbrl/companyconcept/CIK{cik}/us-gaap/{args.fact_tag}.json"

    submissions = try_section(
        "2. Submissions and recent filing history",
        "The submissions JSON includes issuer metadata and a recent columnar table of accession numbers, forms, and dates.",
        lambda: client.get_json(submissions_url),
        render=False,
    )
    if submissions:
        issuer = {
            key: submissions.get(key)
            for key in ("name", "tickers", "exchanges", "sic", "sicDescription", "stateOfIncorporation")
        }
        show(issuer)
        recent = submissions.get("filings", {}).get("recent", {})
        entries = []
        fields = ("accessionNumber", "filingDate", "reportDate", "form", "primaryDocument", "primaryDocDescription")
        count = len(recent.get("accessionNumber", []))
        for index in range(min(6, count)):
            entries.append({key: recent.get(key, [])[index] if index < len(recent.get(key, [])) else None for key in fields})
        show(entries)

    def historical_filings() -> Any:
        if not submissions:
            raise RuntimeError("Submissions response is unavailable")
        chunks = submissions.get("filings", {}).get("files", [])
        if not chunks:
            return "No separate historical submissions file is listed for this issuer."
        name = chunks[0]["name"]
        chunk = client.get_json(f"{DATA_SEC}/submissions/{name}")
        accessions = chunk.get("accessionNumber", [])
        sample = []
        for index in range(min(5, len(accessions))):
            sample.append({key: chunk.get(key, [])[index] if index < len(chunk.get(key, [])) else None for key in fields})
        return {"chunk_file": name, "historical_filing_count": len(accessions), "sample_filings": sample}

    try_section(
        "3. Historical submissions chunk",
        "When an issuer has more filing history than the submissions response embeds, filings.files points to additional JSON chunks.",
        historical_filings,
    )

    facts = try_section(
        "4. Companyfacts: issuer-wide XBRL data",
        "One JSON response contains standardized us-gaap and dei facts, reported units, filing forms, periods, and accession IDs.",
        lambda: client.get_json(facts_url),
        render=False,
    )
    if facts:
        tags = ["Revenues", "NetIncomeLoss", "Assets", "Liabilities", "StockholdersEquity"]
        print("Selected recent annual/quarterly facts (values follow the issuer's reported units):")
        show(latest_company_facts(facts, tags), max_items=5)
        print(f"Taxonomies returned: {', '.join(facts.get('facts', {}).keys())}")

    concept = try_section(
        "5. Companyconcept: one issuer and one XBRL tag",
        f"The endpoint below isolates us-gaap/{args.fact_tag}; its units map to arrays of dated fact observations.",
        lambda: client.get_json(concept_url),
        render=False,
    )
    if concept:
        concept_preview = {
            "entity": concept.get("entityName"),
            "tag": concept.get("tag"),
            "label": concept.get("label"),
            "units": {
                unit: sorted(rows, key=lambda row: (row.get("end", ""), row.get("filed", "")), reverse=True)[:5]
                for unit, rows in concept.get("units", {}).items()
            },
        }
        print("Recent reported values:")
        show(concept_preview)

    frame = completed_quarter_frame()
    frames_url = f"{DATA_SEC}/api/xbrl/frames/us-gaap/{args.fact_tag}/USD/{frame}.json"
    frame_data = try_section(
        "6. Frames: cross-company XBRL comparison",
        f"The frame endpoint requests the {frame} instant for {args.fact_tag} in USD across reporting entities. Frames align periods approximately, not to each company's fiscal calendar.",
        lambda: client.get_json(frames_url),
        render=False,
    )
    if frame_data:
        print("Frame metadata and sample companies:")
        show({key: value for key, value in frame_data.items() if key != "data"})
        show(frame_data.get("data", [])[:5])

    def filing_index() -> Any:
        if not submissions:
            raise RuntimeError("Submissions response is unavailable, so no accession number can be selected")
        recent = submissions.get("filings", {}).get("recent", {})
        accessions = recent.get("accessionNumber", [])
        if not accessions:
            return "No recent filing accession was returned."
        accession = accessions[0]
        archive_url = f"{SEC_ARCHIVES}/{cik_number}/{accession.replace('-', '')}/index.json"
        payload = client.get_json(archive_url)
        directory = payload.get("directory", {})
        return {
            "filing_date": recent.get("filingDate", [None])[0],
            "form": recent.get("form", [None])[0],
            "accession": accession,
            "filing_directory_url": archive_url.removesuffix("index.json"),
            "archive_index_metadata": {key: directory.get(key) for key in ("name", "last-modified", "size")},
            "documents": directory.get("item", []),
        }

    try_section(
        "7. Filing archive index and document inventory",
        "Archives/<CIK>/<accession>/index.json lists the files in one filing directory (HTML/iXBRL, XML, exhibits, and other documents).",
        filing_index,
    )

    print("\nSnapshot complete. Sections show parsed JSON previews; the source endpoints return full JSON documents.")
    print("Public data APIs are unauthenticated, but the SEC requires an identifying User-Agent and fair-access pacing.")
    print("For bulk historical retrieval, use the SEC's published nightly ZIP files instead of many individual requests.")


if __name__ == "__main__":
    main()
