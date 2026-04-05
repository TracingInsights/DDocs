"""Fetch FIA F1 event classification tables and save them as JSON.

This scraper targets the FIA F1 archive page for a given season and pulls the
HTML table-based classification data for each Grand Prix, including sprint
weekends where the FIA exposes the tables in page source.

Supported season profiles:
  - 2018 -> season id 866
  - 2019 -> season id 971
  - 2020 -> season id 1059
  - 2021 -> season id 1108
  - 2022 -> season id 2005
  - 2023 -> season id 2042
  - 2024 -> season id 2043
  - 2025 -> season id 2071

Outputs by default:
  - classification/2025/classifications.json
  - classification/2025/<grand-prix-slug>/classifications.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://www.fia.com"
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


@dataclass(frozen=True, slots=True)
class SeasonConfig:
    year: int
    season_id: int
    parser_profile: str
    discover_events: Callable[[str], list[EventListing]]
    parse_source_page: Callable[[dict[str, str], str], dict]


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


def classify_archive_link_2018(label: str) -> str | None:
    return classify_archive_link_modern(label)


def classify_archive_link_2019(label: str) -> str | None:
    return classify_archive_link_modern(label)


def classify_archive_link_2020(label: str) -> str | None:
    return classify_archive_link_modern(label)


def classify_archive_link_2021(label: str) -> str | None:
    normalized = clean_text(label).lower()
    if normalized == "sprint qualifying":
        return "sprint_classification"
    return classify_archive_link_modern(label)


def classify_archive_link_2023(label: str) -> str | None:
    normalized = clean_text(label).lower()
    if normalized == "sprint shootout":
        return "sprint_qualifying"
    if normalized == "sprint":
        return "sprint_classification"
    return classify_archive_link_modern(label)


def classify_archive_link_2022(label: str) -> str | None:
    return classify_archive_link_modern(label)


def classify_archive_link_modern(label: str) -> str | None:
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


def championship_event_slug(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[-1]


def derive_event_page_slug(championship_url: str, session_url: str | None) -> str:
    if not session_url:
        return championship_event_slug(championship_url)

    path_parts = urlparse(session_url).path.strip("/").split("/")
    if len(path_parts) < 2:
        return championship_event_slug(championship_url)

    candidate = path_parts[-2]
    if candidate.startswith("season-"):
        return championship_event_slug(championship_url)

    return candidate


async def fetch_text(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    url: str,
) -> str:
    async with sem:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
            response.raise_for_status()
            return await response.text()


def discover_events(
    archive_html: str,
    classify_archive_link: Callable[[str], str | None],
) -> list[EventListing]:
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
        championship_url = urljoin(BASE_URL, championship_link["href"])
        event_page_slug = derive_event_page_slug(championship_url, session_url)

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
            event_page_slug=event_page_slug,
            championship_url=championship_url,
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
                event_page_slug=derive_event_page_slug(existing.championship_url, preferred_session_url),
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


def discover_events_2018(archive_html: str) -> list[EventListing]:
    return discover_events(archive_html, classify_archive_link_2018)


def discover_events_2019(archive_html: str) -> list[EventListing]:
    return discover_events(archive_html, classify_archive_link_2019)


def discover_events_2020(archive_html: str) -> list[EventListing]:
    return discover_events(archive_html, classify_archive_link_2020)


def discover_events_2021(archive_html: str) -> list[EventListing]:
    return discover_events(archive_html, classify_archive_link_2021)


def discover_events_2022(archive_html: str) -> list[EventListing]:
    return discover_events(archive_html, classify_archive_link_2022)


def discover_events_2023(archive_html: str) -> list[EventListing]:
    return discover_events(archive_html, classify_archive_link_2023)


def discover_events_2024(archive_html: str) -> list[EventListing]:
    return discover_events(archive_html, classify_archive_link_modern)


def discover_events_2025(archive_html: str) -> list[EventListing]:
    return discover_events(archive_html, classify_archive_link_modern)


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


def is_title_row(row: Tag) -> bool:
    title_cell = row.find("th", class_=re.compile(r"\btable-head\b"))
    if isinstance(title_cell, Tag):
        return True

    cells = row.find_all(["th", "td"])
    if len(cells) != 1:
        return False

    cell = cells[0]
    colspan = cell.get("colspan")
    if colspan and colspan.isdigit() and int(colspan) > 1:
        return True

    return False


def extract_table_title(table: Tag, fallback: str) -> str:
    caption = table.find("caption")
    if isinstance(caption, Tag):
        text = clean_text(caption.get_text(" ", strip=True))
        if text:
            return text

    title_cell = table.find("th", class_=re.compile(r"\btable-head\b"))
    if isinstance(title_cell, Tag):
        text = clean_text(title_cell.get_text(" ", strip=True))
        if text:
            return text

    return fallback


def build_default_columns(cell_count: int) -> list[dict[str, str]]:
    return [{"key": f"col_{index}", "label": f"COL {index}"} for index in range(1, cell_count + 1)]


def expanded_header_labels(row: Tag, column_count: int) -> list[str]:
    labels: list[str] = []

    for cell in row.find_all(["th", "td"]):
        colspan = cell.get("colspan")
        span = int(colspan) if colspan and colspan.isdigit() else 1
        labels.extend([clean_text(cell.get_text(" ", strip=True))] * span)

    if len(labels) < column_count:
        labels.extend([""] * (column_count - len(labels)))

    return labels[:column_count]


def build_columns_from_header_rows(header_rows: list[Tag]) -> list[dict[str, str]]:
    column_count = max(
        sum(int(cell.get("colspan", 1)) if str(cell.get("colspan", 1)).isdigit() else 1 for cell in row.find_all(["th", "td"]))
        for row in header_rows
    )
    expanded_rows = [expanded_header_labels(row, column_count) for row in header_rows]

    previous_metric: str | None = None
    seen_keys: dict[str, int] = {}
    columns: list[dict[str, str]] = []

    for index in range(column_count):
        label_parts: list[str] = []
        for labels in expanded_rows:
            label = labels[index]
            if label and (not label_parts or label_parts[-1] != label):
                label_parts.append(label)

        label = " ".join(label_parts)
        key, previous_metric = build_column_key(label, previous_metric, seen_keys)
        columns.append({"key": key, "label": label})

    return columns


def parse_table(table: Tag) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    rows = table.find_all("tr")
    if not rows:
        return [], []

    data_start_index = 0
    while data_start_index < len(rows) and is_title_row(rows[data_start_index]):
        data_start_index += 1

    if data_start_index >= len(rows):
        return [], []

    header_rows: list[Tag] = []
    while data_start_index < len(rows) and is_header_row(rows[data_start_index]):
        header_rows.append(rows[data_start_index])
        data_start_index += 1

    if header_rows:
        columns = build_columns_from_header_rows(header_rows)
    else:
        first_row_cells = rows[data_start_index].find_all(["th", "td"])
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
        title = extract_table_title(table, f"Table {index}")
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


def parse_source_page_2018(page: dict[str, str], html: str) -> dict:
    return parse_source_page(page, html)


def parse_source_page_2019(page: dict[str, str], html: str) -> dict:
    return parse_source_page(page, html)


def parse_source_page_2020(page: dict[str, str], html: str) -> dict:
    return parse_source_page(page, html)


def parse_source_page_2021(page: dict[str, str], html: str) -> dict:
    return parse_source_page(page, html)


def parse_source_page_2022(page: dict[str, str], html: str) -> dict:
    return parse_source_page(page, html)


def parse_source_page_2023(page: dict[str, str], html: str) -> dict:
    return parse_source_page(page, html)


def parse_source_page_2024(page: dict[str, str], html: str) -> dict:
    return parse_source_page(page, html)


def parse_source_page_2025(page: dict[str, str], html: str) -> dict:
    return parse_source_page(page, html)


SUPPORTED_SEASONS: dict[int, SeasonConfig] = {
    2018: SeasonConfig(
        year=2018,
        season_id=866,
        parser_profile="fia_2018",
        discover_events=discover_events_2018,
        parse_source_page=parse_source_page_2018,
    ),
    2019: SeasonConfig(
        year=2019,
        season_id=971,
        parser_profile="fia_2019",
        discover_events=discover_events_2019,
        parse_source_page=parse_source_page_2019,
    ),
    2020: SeasonConfig(
        year=2020,
        season_id=1059,
        parser_profile="fia_2020",
        discover_events=discover_events_2020,
        parse_source_page=parse_source_page_2020,
    ),
    2021: SeasonConfig(
        year=2021,
        season_id=1108,
        parser_profile="fia_2021",
        discover_events=discover_events_2021,
        parse_source_page=parse_source_page_2021,
    ),
    2022: SeasonConfig(
        year=2022,
        season_id=2005,
        parser_profile="fia_2022",
        discover_events=discover_events_2022,
        parse_source_page=parse_source_page_2022,
    ),
    2023: SeasonConfig(
        year=2023,
        season_id=2042,
        parser_profile="fia_2023",
        discover_events=discover_events_2023,
        parse_source_page=parse_source_page_2023,
    ),
    2024: SeasonConfig(
        year=2024,
        season_id=2043,
        parser_profile="fia_2024",
        discover_events=discover_events_2024,
        parse_source_page=parse_source_page_2024,
    ),
    2025: SeasonConfig(
        year=2025,
        season_id=2071,
        parser_profile="fia_2025",
        discover_events=discover_events_2025,
        parse_source_page=parse_source_page_2025,
    ),
}
DEFAULT_YEAR = max(SUPPORTED_SEASONS)


def write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")


async def fetch_event_payload(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    event: EventListing,
    season: SeasonConfig,
) -> dict:
    async def fetch_page(page: dict[str, str]) -> dict:
        log.info("Fetching %s", page["url"])
        html = await fetch_text(session, sem, page["url"])
        return season.parse_source_page(page, html)

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
    season = SUPPORTED_SEASONS[year]
    if season_id != season.season_id:
        raise ValueError(
            f"Season ID {season_id} does not match the configured {year} season ID "
            f"{season.season_id} for parser profile {season.parser_profile}."
        )

    headers = {"User-Agent": USER_AGENT}
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    archive_url = ARCHIVE_URL_TEMPLATE.format(season_id=season_id)

    async with aiohttp.ClientSession(headers=headers) as session:
        archive_html = await fetch_text(session, sem, archive_url)
        events = season.discover_events(archive_html)
        if not events:
            raise RuntimeError(f"No Grand Prix events found on {archive_url}")

        log.info(
            "Discovered %d Grand Prix events for %s",
            len(events),
            season.parser_profile,
        )
        event_payloads = await asyncio.gather(
            *(fetch_event_payload(session, sem, event, season) for event in events)
        )

    event_payloads.sort(key=lambda item: (item.get("date") or "", item["name"]))

    year_dir = output_dir / str(year)
    for event_payload in event_payloads:
        write_json(year_dir / event_payload["slug"] / "classifications.json", event_payload)

    combined_payload = {
        "season_year": year,
        "season_id": season_id,
        "parser_profile": season.parser_profile,
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
    parser.add_argument(
        "--year",
        type=int,
        choices=sorted(SUPPORTED_SEASONS),
        default=DEFAULT_YEAR,
        help="Season year to scrape. Supported: %(choices)s",
    )
    parser.add_argument(
        "--season-id",
        type=int,
        help="Override the FIA season ID. Must match the configured ID for the selected year.",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    season_id = args.season_id or SUPPORTED_SEASONS[args.year].season_id
    output_path = asyncio.run(run(args.year, season_id, args.output_dir))
    log.info("Saved combined classifications to %s", output_path)


if __name__ == "__main__":
    main()
