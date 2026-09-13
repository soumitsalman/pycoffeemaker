#!/usr/bin/env python3
"""Merge a production RSS export into factory/feeds.yaml."""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit

import yaml


RSS_GROUPS = ("rss", "rss_blogs", "rss_press_releases")
PRESS_HOST_MARKERS = (
    "businesswire.",
    "newsroom",
    "presswire.",
    "prnewswire.",
)
PRESS_PATH_RE = re.compile(
    r"(?:^|/)(?:press(?:[-_]?release|releases?|room)?|"
    r"news(?:[-_]?release|releases?|room)|media[-_]?releases?|"
    r"announcements?|investor[-_]?relations?|press_reports?)(?:[./_-]|$)"
)
BLOG_HOST_MARKERS = (
    ".blogspot.",
    ".substack.",
    ".wordpress.",
    ".typepad.",
    ".bearblog.",
    ".mataroa.",
    ".ghost.",
    "medium.com",
)
BLOG_PATH_RE = re.compile(
    r"(?:^|/)(?:blog|blogs|weblog|journal|opinion|opinions|essays|notes)(?:[./_-]|$)"
)


def canonical_key(value: str) -> tuple[str, str]:
    """Return a stable key for URL duplicates without changing stored URLs."""
    value = value.strip()
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        return ("value", value)

    scheme = parsed.scheme.lower()
    host = parsed.hostname.lower()
    port = parsed.port
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        host = f"{host}:{port}"
    path = parsed.path.rstrip("/") or "/"
    return ("url", urlunsplit((scheme, host, path, parsed.query, "")))


def classify_feed(url: str) -> str:
    """Infer the editorial bucket from URL-only evidence in the export."""
    parsed = urlsplit(url.lower())
    host = parsed.hostname or ""
    path = unquote(parsed.path)
    host_and_path = f"{host}{path}"

    if any(marker in host for marker in PRESS_HOST_MARKERS) or PRESS_PATH_RE.search(path):
        return "rss_press_releases"
    if (
        host.startswith("blog.")
        or any(marker in host for marker in BLOG_HOST_MARKERS)
        or BLOG_PATH_RE.search(path)
        or re.search(r"(?:^|[./_-])blog(?:[./_-]|$)", path)
        or re.search(r"(?:^|/)(?:feed|rss)[^/]*blog", host_and_path)
    ):
        return "rss_blogs"
    return "rss"


def load_feeds(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if "rss_feed" not in (reader.fieldnames or []):
            raise ValueError(f"{path} must contain an rss_feed column")
        return [row["rss_feed"].strip() for row in reader if row.get("rss_feed", "").strip()]


def deduplicate_sources(sources: dict) -> tuple[dict, int]:
    seen: set[tuple[str, str]] = set()
    removed = 0
    result = {}
    for group, values in sources.items():
        result[group] = []
        for value in values or []:
            key = canonical_key(str(value))
            if key in seen:
                removed += 1
                continue
            seen.add(key)
            result[group].append(value)
    return result, removed


def merge(csv_path: Path, yaml_path: Path, dry_run: bool = False) -> None:
    config = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    sources = config["sources"]
    for group in RSS_GROUPS:
        sources.setdefault(group, [])

    sources, removed_existing = deduplicate_sources(sources)
    existing_keys = {
        canonical_key(str(value))
        for values in sources.values()
        for value in values or []
    }

    added = Counter()
    seen_csv: set[tuple[str, str]] = set()
    for url in load_feeds(csv_path):
        key = canonical_key(url)
        if key in existing_keys or key in seen_csv:
            continue
        group = classify_feed(url)
        sources[group].append(url)
        seen_csv.add(key)
        existing_keys.add(key)
        added[group] += 1

    config["sources"] = sources
    if not dry_run:
        yaml_path.write_text(
            yaml.safe_dump(config, sort_keys=False, allow_unicode=True, width=120),
            encoding="utf-8",
        )

    action = "Would add" if dry_run else "Added"
    print(f"{action} {sum(added.values())} feeds: {dict(added)}")
    print(f"Removed {removed_existing} existing duplicate entries")
    print(f"Final RSS totals: { {group: len(sources[group]) for group in RSS_GROUPS} }")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--yaml-path", type=Path, default=Path("factory/feeds.yaml"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    merge(args.csv_path, args.yaml_path, args.dry_run)


if __name__ == "__main__":
    main()
