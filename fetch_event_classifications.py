"""Fetch FIA F1 event classification tables and save them as JSON.

This scraper targets the FIA 2025 archive page and pulls the HTML table-based
classification data for each Grand Prix, including sprint weekends.

Outputs:
  - classification/2025/classifications.json
  - classification/2025/<grand-prix-slug>/classifications.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://www.fia.com"
DEFAULT_YEAR = 2025
DEFAULT_SEASON_ID = 2071
DEFAULT_OUTPUT_DIR = Path("classification")
ARCHIVE_URL_TEMPLATE = f"{BASE_URL}/f1-archives?season={{season_id}}"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)
MAX_CONCURRENT_REQUESTS = 6

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


@dataclass(slots=True)
class EventListing:
    name: str
    date: str | None
    output_slug: str
    event_page_slug: str
    championship_url: str
    archive_pages: list[dict[str, str]]
    archive_table: dict


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def slugify(value: str) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[^\w\s-]", "", value)
    value = re.sub(r"[\s-]+", "-", value)
    return value.strip("-")


def to_key(value: str) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[^\w\s]+", "", value)
    value = re.sub(r"\s+", "_", value)
    return value.strip("_") or "column"


def parse_archive_date(value: str | None) -> str | None:
    if not value:
        return None
    return datetime.strptime(value, "%B %d, %Y").date().isoformat()


def classify_archive_link(label: str) -> str | None:
    normalized = clean_text(label).lower()
    if "session classifications" in normalized:
        return "session_classifications"
    if "sprint qualifying" in normalized:
        return "sprint_qualifying"
    if "sprint classification" in normalized:
        return "sprint_classification"
    if "qualifying classification" in normalized:
        return "qualifying_classification"
    if "race classification" in normalized or "race qualification" in normalized:
        return "race_classification"
    return None


def choose_preferred_url(current: str, candidate: str) -> str:
    current_has_numeric_suffix = bool(re.search(r"-\d+$", urlparse(current).path))
    candidate_has_numeric_suffix = bool(re.search(r"-\d+$", urlparse(candidate).path))
    if current_has_numeric_suffix and not candidate_has_numeric_suffix:
        return candidate
    return current


def merge_archive_pages(existing: list[dict[str, str]], candidate: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {page["key"]: dict(page) for page in existing}

    for page in candidate:
        current = merged.get(page["key"])
        if current is None:
            merged[page["key"]] = dict(page)
            continue

        preferred_url = choose_preferred_url(current["url"], page["url"])
        merged[page["key"]] = {
            "key": current["key"],
            "label": current["label"],
            "url": preferred_url,
        }

    return list(merged.values())


def page_slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[-2]


async def fetch_text(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
) -> str:
    async with sem:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
            response.raise_for_status()
            return await response.text()


def discover_events(archive_html: str) -> list[EventListing]:
    soup = BeautifulSoup(archive_html, "html.parser")
    events: dict[str, EventListing] = {}

    for table in soup.find_all("table"):
        caption = table.find("caption")
        if not isinstance(caption, Tag):
            continue

        championship_link = caption.find(
            "a",
            href=re.compile(r"/championship/events/fia-formula-one-world-championship/season-\d+/"),
        )
        if not isinstance(championship_link, Tag):
            continue

        full_caption_text = clean_text(championship_link.get_text(" ", strip=True))
        if "Grand Prix" not in full_caption_text:
            continue

        date_tag = caption.find(class_=re.compile(r"date-display-single"))
        raw_date = clean_text(date_tag.get_text(" ", strip=True)) if isinstance(date_tag, Tag) else None

        event_name = full_caption_text
        if raw_date:
            event_name = re.sub(rf"\s*-\s*{re.escape(raw_date)}$", "", event_name).strip()

        archive_pages: list[dict[str, str]] = []
        for link in table.find_all("a", href=True):
            key = classify_archive_link(link.get_text(" ", strip=True))
            if not key:
                continue
            archive_pages.append(
                {
                    "key": key,
                    "label": clean_text(link.get_text(" ", strip=True)),
                    "url": urljoin(BASE_URL, link["href"]),
                }
            )

        session_page = next((page for page in archive_pages if page["key"] == "session_classifications"), None)
        session_url = session_page["url"] if session_page else None
        if not session_url:
            continue

        archive_table_rows = [
            {"label": page["label"], "url": page["url"]}
            for page in archive_pages
        ]
        archive_table = {
            "name": "Archive Links",
            "slug": "archive-links",
            "columns": [
                {"key": "label", "label": "LABEL"},
                {"key": "url", "label": "URL"},
            ],
            "rows": archive_table_rows,
            "row_count": len(archive_table_rows),
        }

        output_slug = slugify(event_name)
        candidate = EventListing(
            name=event_name,
            date=parse_archive_date(raw_date),
            output_slug=output_slug,
            event_page_slug=page_slug_from_url(session_url),
            championship_url=urljoin(BASE_URL, championship_link["href"]),
            archive_pages=archive_pages,
            archive_table=archive_table,
        )

        existing = events.get(output_slug)
        if existing:
            merged_pages = merge_archive_pages(existing.archive_pages, candidate.archive_pages)
            preferred_session_url = next(
                page["url"] for page in merged_pages if page["key"] == "session_classifications"
            )
            events[output_slug] = EventListing(
                name=existing.name,
                date=existing.date or candidate.date,
                output_slug=output_slug,
                event_page_slug=page_slug_from_url(preferred_session_url),
                championship_url=existing.championship_url,
                archive_pages=merged_pages,
                archive_table={
                    "name": "Archive Links",
                    "slug": "archive-links",
                    "columns": candidate.archive_table["columns"],
                    "rows": [{"label": page["label"], "url": page["url"]} for page in merged_pages],
                    "row_count": len(merged_pages),
                },
            )
            continue

        events[output_slug] = candidate

    return list(events.values())


def build_column_key(label: str, previous_metric: str | None, seen: dict[str, int]) -> tuple[str, str | None]:
    key = to_key(label)

    if key == "laps" and previous_metric in {"q1", "q2", "q3", "sq1", "sq2", "sq3"}:
        key = f"{previous_metric}_laps"

    if seen.get(key, 0):
        seen[key] += 1
        key = f"{key}_{seen[key]}"
    else:
        seen[key] = 1

    next_metric = previous_metric
    if key in {"q1", "q2", "q3", "sq1", "sq2", "sq3"}:
        next_metric = key

    return key, next_metric


def is_header_row(row: Tag) -> bool:
    cells = row.find_all(["th", "td"])
    if not cells:
        return False
    if row.find("th"):
        return True
    row_classes = set(row.get("class", []))
    if {"table-header", "header-color"} & row_classes:
        return True
    return all(any(cls.startswith("header-") for cls in cell.get("class", [])) for cell in cells)


def build_default_columns(cell_count: int) -> list[dict[str, str]]:
    return [{"key": f"col_{index}", "label": f"COL {index}"} for index in range(1, cell_count + 1)]


def parse_table(table: Tag) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    rows = table.find_all("tr")
    if not rows:
        return [], []

    data_start_index = 0
    if is_header_row(rows[0]):
        header_cells = rows[0].find_all(["th", "td"])
        previous_metric: str | None = None
        seen_keys: dict[str, int] = {}
        columns: list[dict[str, str]] = []

        for cell in header_cells:
            label = clean_text(cell.get_text(" ", strip=True))
            key, previous_metric = build_column_key(label, previous_metric, seen_keys)
            columns.append({"key": key, "label": label})

        data_start_index = 1
    else:
        first_row_cells = rows[0].find_all(["th", "td"])
        columns = build_default_columns(len(first_row_cells))

    data_rows: list[dict[str, str]] = []
    for row in rows[data_start_index:]:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue

        record: dict[str, str] = {}
        for column, cell in zip(columns, cells, strict=False):
            record[column["key"]] = clean_text(cell.get_text(" ", strip=True))

        if any(value for value in record.values()):
            data_rows.append(record)

    return columns, data_rows


def parse_table_bundle(
    table: Tag,
    title: str,
    slug: str | None = None,
) -> dict:
    columns, rows = parse_table(table)
    return {
        "name": title,
        "slug": slug or slugify(title),
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }


def parse_page_title(soup: BeautifulSoup) -> str:
    for selector in ("h2", "h1", "title"):
        node = soup.find(selector)
        if isinstance(node, Tag):
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                return text
    return "Untitled Page"


def parse_all_page_tables(root: Tag | BeautifulSoup) -> list[dict]:
    tables: list[dict] = []
    for index, table in enumerate(root.find_all("table"), start=1):
        caption = table.find("caption")
        title = clean_text(caption.get_text(" ", strip=True)) if isinstance(caption, Tag) else f"Table {index}"
        tables.append(parse_table_bundle(table, title, slugify(title)))
    return tables


def parse_standard_tables(soup: BeautifulSoup) -> list[dict]:
    tables = soup.select(".standard-tables-outer .external-xml-data.standard-table")
    sessions: list[dict] = []

    for index, block in enumerate(tables, start=1):
        table = block.find("table")
        if not isinstance(table, Tag):
            continue

        heading = block.find("h3", class_=re.compile(r"token-title"))
        session_name = clean_text(heading.get_text(" ", strip=True)) if isinstance(heading, Tag) else f"TABLE {index}"
        sessions.append(parse_table_bundle(table, session_name))

    return sessions


def parse_source_page(page: dict[str, str], html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title = parse_page_title(soup)
    standard_tables = parse_standard_tables(soup)
    if standard_tables:
        tables = standard_tables
    else:
        content_root = soup.select_one(".content .middle") or soup.select_one(".content") or soup
        tables = parse_all_page_tables(content_root)
    return {
        "key": page["key"],
        "label": page["label"],
        "title": title,
        "url": page["url"],
        "table_count": len(tables),
        "tables": tables,
    }


def write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")


async def fetch_event_payload(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    event: EventListing,
) -> dict:
    async def fetch_page(page: dict[str, str]) -> dict:
        log.info("Fetching %s", page["url"])
        html = await fetch_text(session, sem, page["url"])
        return parse_source_page(page, html)

    fetched_pages = await asyncio.gather(*(fetch_page(page) for page in event.archive_pages))

    archive_listing_page = {
        "key": "archive_listing",
        "label": "Archive Listing",
        "title": "Archive Listing",
        "url": event.championship_url,
        "table_count": 1,
        "tables": [event.archive_table],
    }
    pages = [archive_listing_page, *fetched_pages]

    session_page = next((page for page in pages if page["key"] == "session_classifications"), None)
    sessions = session_page["tables"] if session_page else []
    source_pages = {page["key"]: page["url"] for page in event.archive_pages}
    table_count = sum(page["table_count"] for page in pages)

    return {
        "name": event.name,
        "slug": event.output_slug,
        "event_page_slug": event.event_page_slug,
        "date": event.date,
        "championship_url": event.championship_url,
        "source_pages": source_pages,
        "page_count": len(pages),
        "table_count": table_count,
        "pages": pages,
        "session_page_url": source_pages.get("session_classifications"),
        "session_count": len(sessions),
        "sessions": sessions,
    }


async def run(year: int, season_id: int, output_dir: Path) -> Path:
    headers = {"User-Agent": USER_AGENT}
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    archive_url = ARCHIVE_URL_TEMPLATE.format(season_id=season_id)

    async with aiohttp.ClientSession(headers=headers) as session:
        archive_html = await fetch_text(session, sem, archive_url)
        events = discover_events(archive_html)
        if not events:
            raise RuntimeError(f"No Grand Prix events found on {archive_url}")

        log.info("Discovered %d Grand Prix events", len(events))
        event_payloads = await asyncio.gather(
            *(fetch_event_payload(session, sem, event) for event in events)
        )

    event_payloads.sort(key=lambda item: (item.get("date") or "", item["name"]))

    year_dir = output_dir / str(year)
    for event_payload in event_payloads:
        write_json(year_dir / event_payload["slug"] / "classifications.json", event_payload)

    combined_payload = {
        "season_year": year,
        "season_id": season_id,
        "archive_url": archive_url,
        "fetched_at_utc": datetime.now(UTC).isoformat(),
        "event_count": len(event_payloads),
        "events": event_payloads,
    }
    combined_path = year_dir / "classifications.json"
    write_json(combined_path, combined_payload)
    return combined_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    parser.add_argument("--season-id", type=int, default=DEFAULT_SEASON_ID)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = asyncio.run(run(args.year, args.season_id, args.output_dir))
    log.info("Saved combined classifications to %s", output_path)


if __name__ == "__main__":
    main()
