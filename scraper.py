"""FIA F1 Document Scraper

Scrapes the FIA website for Formula 1 decision documents:
  - Decision documents  → year/grand-prix/document-name.pdf

Uses curl_cffi for browser-impersonated requests (bypasses TLS fingerprint checks)
and asyncio for parallel discovery and PDF downloads.

Discovery results are cached to disk (TTL configurable via environment) so repeat runs 
within the same cron window skip all crawling and go straight to downloads.
"""

import asyncio
import datetime
import json
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import aiofiles
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from config import (
    BASE_URL, CHAMPIONSHIP_PATH, DOCUMENTS_URL, AJAX_URL,
    MAX_AJAX_CONCURRENT, MAX_DOWNLOAD_CONCURRENT, AJAX_EXTRA_HEADERS
)
from shared_utils import (
    slugify, load_manifest_with_lock, save_manifest_with_lock,
    load_discovery_cache, save_discovery_cache, validate_cache_structure
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------


def _extract_pdf_links(soup: BeautifulSoup) -> list[dict]:
    documents = []
    for link in soup.find_all("a", href=lambda h: h and h.endswith(".pdf")):
        # The date might be in a sibling or parent container (e.g., inside an <li>)
        raw_text = link.get_text(strip=True, separator=" ")

        # Find the closest parent that contains the "Published on" text
        parent_with_date = link.find_parent(lambda tag: tag and tag.name in ["li", "div", "tr"] and "Published on" in tag.get_text())
        search_text = parent_with_date.get_text(strip=True, separator=" ") if parent_with_date else raw_text

        # Extract published time (e.g., "Published on 12.03.2021 15:45")
        published_time = ""
        # Look for "Published on " followed by digits (the date)
        pub_match = re.search(r"Published\s+on\s+([\d\.:/\sA-Z]+)", search_text, re.IGNORECASE)
        if pub_match:
            after_on = pub_match.group(1).strip()
            # Find the actual date time portion
            time_match = re.search(r"(\d{1,2}[\.:/]\d{1,2}[\.:/]\d{2,4}\s+\d{1,2}:\d{2})", after_on)
            if time_match:
                published_time = time_match.group(1)
            else:
                # Fallback if the time format is slightly different
                time_match2 = re.search(r"([\d\.:/]+(?:\s+[\d:]+)?)", after_on)
                published_time = time_match2.group(1).strip() if time_match2 else after_on

        # print(f"DEBUG: EXTRACTED '{published_time}' from '{search_text[:80]}'")

        title = re.sub(r"^Doc\s+\d+\s*[-–]\s*", "", raw_text)
        title = re.sub(r"Published\s+on.*$", "", title, flags=re.IGNORECASE).strip()
        if not title:
            title = Path(link["href"]).stem

        doc_num_match = re.match(r"Doc\s+(\d+)", raw_text)
        doc_num = int(doc_num_match.group(1)) if doc_num_match else 0

        documents.append({
            "title": title,
            "url": urljoin(BASE_URL, link["href"]),
            "filename": slugify(title) + ".pdf",
            "doc_number": doc_num,
            "published_time": published_time,
        })
    return documents


async def fetch_text(
    session: AsyncSession,
    url: str,
    extra_headers: dict | None = None,
    retries: int = 3,
) -> str | None:
    """GET a URL, returning text or None after retries."""
    for attempt in range(retries):
        try:
            resp = await session.get(url, headers=extra_headers or {}, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            log.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return None


async def download_pdf(
    session: AsyncSession,
    sem: asyncio.Semaphore,
    url: str,
    dest: Path,
) -> bool:
    """Download a single PDF. Returns True if newly written."""
    if dest.exists():
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        async with sem:
            resp = await session.get(url, timeout=120)
            resp.raise_for_status()
            content = resp.content

        async with aiofiles.open(dest, "wb") as f:
            await f.write(content)

        log.info("Downloaded: %s (%d KB)", dest, len(content) // 1024)
        return True
    except Exception as e:
        log.error("Failed to download %s: %s", url, e)
        if dest.exists():
            dest.unlink()
        return False


def load_manifest(path: Path) -> dict:
    """Legacy wrapper for backward compatibility."""
    return load_manifest_with_lock(path)


def save_manifest(path: Path, manifest: dict) -> None:
    """Legacy wrapper for backward compatibility."""
    save_manifest_with_lock(path, manifest)


def _unique_dest(dest: Path, assigned: set[Path]) -> Path:
    if dest not in assigned and not dest.exists():
        return dest
    base, suffix = dest.stem, 1
    while True:
        candidate = dest.with_name(f"{base}-{suffix}.pdf")
        if candidate not in assigned and not candidate.exists():
            return candidate
        suffix += 1


# ---------------------------------------------------------------------------
# Discovery cache
# ---------------------------------------------------------------------------

def load_discovery_cache_legacy(path: Path, force_refresh: bool = False) -> list[dict] | None:
    """Load discovery cache with legacy format support."""
    cache_data = load_discovery_cache(path, "discovery", force_refresh)
    if cache_data is None:
        return None
    
    # Validate required structure for decision documents cache
    if not validate_cache_structure(cache_data, ["events"]):
        return None
    
    return cache_data["events"]


def save_discovery_cache_legacy(path: Path, events: list[dict]) -> None:
    """Save discovery cache with legacy format support."""
    save_discovery_cache(path, {"events": events}, "discovery")


# ---------------------------------------------------------------------------
# Manifest migration
# ---------------------------------------------------------------------------

def migrate_manifest_doc_numbers(manifest: dict, discovery_cache_path: Path) -> bool:
    """
    Backfill doc_number into existing manifest entries that are missing it.
    Only patches decision-source entries. Skips silently if cache is unavailable.
    Returns True if any entries were patched.
    """
    # Only bother if there are actually entries missing doc_number
    needs_patch = [
        url for url, meta in manifest.items()
        if meta.get("source") == "decision" and "doc_number" not in meta
    ]
    if not needs_patch:
        return False

    if not discovery_cache_path.exists():
        log.info("No discovery cache available, skipping doc_number migration (%d entries pending)", len(needs_patch))
        return False

    try:
        data = json.loads(discovery_cache_path.read_text())
        events = data.get("events", [])
    except Exception as e:
        log.warning("Could not read discovery cache for migration: %s", e)
        return False

    # Build URL → doc_number lookup from every document in the cache
    url_to_doc_number: dict[str, int] = {}
    for event in events:
        for doc in event.get("documents", []) + event.get("inline_docs", []):
            url_to_doc_number[doc["url"]] = doc.get("doc_number", 0)

    patched = 0
    for url in needs_patch:
        manifest[url]["doc_number"] = url_to_doc_number.get(url, 0)
        patched += 1

    log.info("Migrated doc_number for %d manifest entries (%d not found in cache, set to 0)",
             patched, sum(1 for u in needs_patch if u not in url_to_doc_number))
    return patched > 0


# ---------------------------------------------------------------------------
# Index writers
# ---------------------------------------------------------------------------

def write_event_index(output: Path, year: int, gp_slug: str, docs: list[dict]) -> None:
    """Write a compact index.json for one event folder."""
    # Sort by number (if exists) then by published time, then by filename
    items = []
    sorted_docs = sorted(docs, key=lambda d: (d.get("n") or 9999, d.get("p", ""), d["f"]))
    for doc in sorted_docs:
        item: dict = {"f": doc["f"], "t": doc["t"]}
        if doc.get("n"):
            item["n"] = doc["n"]
        if doc.get("p"):
            item["p"] = doc["p"]
        items.append(item)

    path = output / str(year) / gp_slug / "index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(items, separators=(",", ":")))
    log.debug("Wrote event index: %s (%d entries)", path, len(items))


def write_year_index(output: Path, year: int, slug_to_name: dict[str, str]) -> None:
    """Write a compact index.json for a year folder listing all events."""
    events = [
        {"s": s, "n": slug_to_name[s]}
        for s in sorted(slug_to_name)
    ]
    path = output / str(year) / "index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(events, separators=(",", ":")))
    log.debug("Wrote year index: %s (%d events)", path, len(events))


# ---------------------------------------------------------------------------
# Decision-documents scraping
# ---------------------------------------------------------------------------

def discover_seasons(soup: BeautifulSoup) -> list[dict]:
    seasons = []
    for option in soup.find_all("option"):
        value = option.get("value", "")
        text = option.get_text(strip=True)
        match = re.match(r"SEASON\s+(\d{4})", text, re.IGNORECASE)
        if match and CHAMPIONSHIP_PATH in value:
            seasons.append({
                "year": int(match.group(1)),
                "url": urljoin(BASE_URL, value),
            })
    return sorted(seasons, key=lambda s: s["year"], reverse=True)


def discover_events_on_page(soup: BeautifulSoup, year: int) -> list[dict]:
    events = []
    for ew in soup.find_all(class_="event-wrapper"):
        title_div = ew.find(class_=lambda c: c and "title" in str(c).lower())
        name = title_div.get_text(strip=True) if title_div else None
        if not name:
            continue

        event_id = None
        ajax_link = ew.find("a", href=lambda h: h and "decision-document-list" in h)
        if ajax_link:
            m = re.search(r"/(\d+)$", ajax_link["href"])
            if m:
                event_id = m.group(1)

        inline_docs = _extract_pdf_links(ew)
        events.append({
            "name": name,
            "id": event_id,
            "year": year,
            "inline_docs": inline_docs,
        })
    return events


async def fetch_event_docs(
    session: AsyncSession,
    sem: asyncio.Semaphore,
    event: dict,
) -> list[dict]:
    if event["inline_docs"]:
        return event["inline_docs"]
    if not event["id"]:
        return []

    async with sem:
        text = await fetch_text(session, f"{AJAX_URL}{event['id']}", extra_headers=AJAX_EXTRA_HEADERS)

    if not text:
        return []

    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return []

    documents = []
    for item in data:
        if item.get("command") == "insert" and "data" in item:
            soup = BeautifulSoup(item["data"], "html.parser")
            documents.extend(_extract_pdf_links(soup))
    return documents


async def discover_all_events(
    session: AsyncSession,
    year_filter: int | None,
) -> list[dict]:
    """Full crawl: main page → season pages → AJAX event docs."""
    log.info("Fetching main documents page...")
    main_html = await fetch_text(session, DOCUMENTS_URL)
    if not main_html:
        log.error("Failed to fetch main documents page")
        return []

    main_soup = BeautifulSoup(main_html, "html.parser")
    seasons = discover_seasons(main_soup)
    log.info("Found seasons: %s", [s["year"] for s in seasons])

    if year_filter:
        seasons = [s for s in seasons if s["year"] == year_filter]
        if not seasons:
            log.error("Season %d not found", year_filter)
            return []

    log.info("Fetching %d season pages in parallel...", len(seasons))
    season_htmls = await asyncio.gather(
        *(fetch_text(session, s["url"]) for s in seasons)
    )

    all_events: list[dict] = []
    for season, html in zip(seasons, season_htmls):
        if not html:
            log.warning("Failed to fetch season %d, skipping", season["year"])
            continue
        soup = BeautifulSoup(html, "html.parser")
        events = discover_events_on_page(soup, season["year"])
        log.info("Season %d: %d decision-doc events", season["year"], len(events))
        all_events.extend(events)

    ajax_sem = asyncio.Semaphore(MAX_AJAX_CONCURRENT)
    events_needing_ajax = [e for e in all_events if not e["inline_docs"] and e["id"]]
    log.info(
        "Loading decision docs for %d events via AJAX (concurrency=%d)...",
        len(events_needing_ajax),
        MAX_AJAX_CONCURRENT,
    )
    ajax_results = await asyncio.gather(
        *(fetch_event_docs(session, ajax_sem, e) for e in events_needing_ajax)
    )

    ajax_idx = 0
    for event in all_events:
        if not event["inline_docs"] and event["id"]:
            event["documents"] = ajax_results[ajax_idx]
            ajax_idx += 1
        else:
            event["documents"] = event["inline_docs"]

    return all_events


# ---------------------------------------------------------------------------
# Main scrape orchestrator
# ---------------------------------------------------------------------------

async def scrape(
    output_dir: str = "documents",
    year_filter: int | None = None,
    force_refresh: bool = False,
) -> int:
    output = Path(output_dir)
    manifest_path = output / "manifest.json"
    discovery_cache_path = output / "discovery_cache.json"
    manifest = load_manifest(manifest_path)

    # Backfill doc_number into any existing manifest entries that predate
    # the field being added. Saves migrated manifest immediately if changed.
    if migrate_manifest_doc_numbers(manifest, discovery_cache_path):
        save_manifest(manifest_path, manifest)

    async with AsyncSession(impersonate="chrome120") as session:

        cached_events = load_discovery_cache_legacy(discovery_cache_path, force_refresh=force_refresh)

        if cached_events is None:
            all_events = await discover_all_events(session, year_filter)
            if all_events:
                save_discovery_cache_legacy(discovery_cache_path, all_events)
        else:
            all_events = cached_events
            if year_filter:
                all_events = [e for e in all_events if e["year"] == year_filter]

        download_queue: list[tuple[str, Path, dict]] = []
        assigned_paths: set[Path] = set()

        manifest_updated = False
        for event in all_events:
            gp_slug = slugify(event["name"])
            year = event["year"]
            for doc in event.get("documents", []):
                if doc["url"] in manifest:
                    entry = manifest[doc["url"]]
                    if not entry.get("published_time") and doc.get("published_time"):
                        entry["published_time"] = doc["published_time"]
                        manifest_updated = True
                    continue
                dest = _unique_dest(
                    output / str(year) / gp_slug / doc["filename"],
                    assigned_paths,
                )
                assigned_paths.add(dest)
                download_queue.append((
                    doc["url"],
                    dest,
                    {
                        "year": year,
                        "event": event["name"],
                        "title": doc["title"],
                        "source": "decision",
                        "doc_number": doc["doc_number"],
                        "published_time": doc["published_time"],
                    },
                ))

        log.info("Download queue: %d new documents", len(download_queue))
        total_new = 0

        if not download_queue:
            log.info("Nothing new to download.")
            # Still rebuild indices — a previous partial run may have left them stale.
            if manifest_updated:
                log.info("Saving updated manifest (backfilled published times).")
                save_manifest(manifest_path, manifest)
        else:
            dl_sem = asyncio.Semaphore(MAX_DOWNLOAD_CONCURRENT)
            results = await asyncio.gather(
                *(download_pdf(session, dl_sem, url, dest) for url, dest, _ in download_queue)
            )

            for (url, dest, meta), downloaded in zip(download_queue, results):
                if downloaded:
                    total_new += 1
                    manifest[url] = {**meta, "path": str(dest)}

            if total_new > 0 or manifest_updated:
                save_manifest(manifest_path, manifest)
            log.info("Done! Downloaded %d new documents.", total_new)

    # -----------------------------------------------------------------------
    # Auto-numbering for 2019-2024 if doc_number is missing (0)
    # -----------------------------------------------------------------------
    # We group all documents in the manifest by (year, event) for the target range.
    groups: dict[tuple[int, str], list[str]] = {}
    for url, meta in manifest.items():
        yr = meta.get("year", 0)
        if 2019 <= yr <= 2024:
            groups.setdefault((yr, meta["event"]), []).append(url)

    any_renumbered = False
    for (yr, event_name), urls in groups.items():
        # Sort by published_time, then by path as fallback
        sorted_urls = sorted(urls, key=lambda u: (manifest[u].get("published_time", ""), manifest[u]["path"]))
        for i, url in enumerate(sorted_urls, 1):
            if manifest[url].get("doc_number", 0) == 0:
                manifest[url]["doc_number"] = i
                any_renumbered = True

    if any_renumbered:
        log.info("Assigned sequence numbers to documents for years 2019-2024.")
        save_manifest(manifest_path, manifest)

    # Rebuild per-event and per-year indices from the full manifest.
    # Uses stored metadata fields directly — never parses year/event from the path.
    event_docs: dict[tuple[int, str], list[dict]] = {}
    year_names: dict[int, dict[str, str]] = {}

    for meta in manifest.values():
        p = Path(meta["path"])
        yr = meta["year"]
        gp_slug = slugify(meta["event"])

        if year_filter and yr != year_filter:
            continue

        event_docs.setdefault((yr, gp_slug), []).append({
            "f": p.name,
            "t": meta["title"],
            "n": meta.get("doc_number", 0),
            "p": meta.get("published_time", ""),
        })
        year_names.setdefault(yr, {})[gp_slug] = meta["event"]

    for (yr, gp_slug), docs in event_docs.items():
        write_event_index(output, yr, gp_slug, docs)

    for yr, slug_to_name in year_names.items():
        write_year_index(output, yr, slug_to_name)

    log.info("Rebuilt indices for %d events across %d seasons.", len(event_docs), len(year_names))

    return total_new


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    output_dir = "documents"
    year_filter = None
    force_refresh = False

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--output-dir" and i + 1 < len(args):
            output_dir = args[i + 1]
            i += 2
        elif args[i] == "--year" and i + 1 < len(args):
            year_filter = int(args[i + 1])
            i += 2
        elif args[i] == "--force-refresh":
            force_refresh = True
            i += 1
        else:
            i += 1

    new_count = asyncio.run(scrape(output_dir=output_dir, year_filter=year_filter, force_refresh=force_refresh))

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"new_documents={new_count}\n")


if __name__ == "__main__":
    main()
