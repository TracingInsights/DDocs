"""FIA F1 Regulations Scraper

Scrapes the FIA website for Formula 1 regulations:
  - https://www.fia.com/regulation/category/110
  - Documents → regs/<year>/<category-slug>/<filename>.pdf

Uses curl_cffi for browser-impersonated requests (bypasses TLS fingerprint
checks) and asyncio for parallel downloads.

The manifest at regs/manifest.json tracks previously-downloaded URLs so
repeat runs are cheap (only new regulations are downloaded).
"""

import asyncio
import logging
import os
import random
import re
from pathlib import Path
from urllib.parse import urljoin

import aiofiles
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from config import MAX_DOWNLOAD_CONCURRENT
from shared_utils import slugify, load_manifest_with_lock, save_manifest_with_lock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

REGULATIONS_URL = "https://www.fia.com/regulation/category/110"
BASE_URL = "https://www.fia.com"

# Standard browser headers to make the request look like a real navigation
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


async def _fetch_text(session: AsyncSession, url: str) -> str | None:
    # First establish a session by visiting the home page if we haven't already
    if not hasattr(session, "_visited_homepage"):
        try:
            log.info("Visiting homepage first to establish session cookies...")
            await session.get("https://www.fia.com", headers=DEFAULT_HEADERS, timeout=20)
            session._visited_homepage = True
            # Let cookies settle
            await asyncio.sleep(1)
        except Exception as exc:
            log.warning("Failed to warm up session via homepage: %s", exc)

    headers = {
        "Referer": "https://www.fia.com/",
        **DEFAULT_HEADERS
    }

    try:
        log.info("Fetching %s...", url)
        resp = await session.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        log.warning("Fetch failed: %s", exc)
        
    return None


# Transient error substrings that warrant a retry
_RETRYABLE_PATTERNS = (
    "connection closed",
    "connection reset",
    "connection aborted",
    "curl: (56)",
    "curl: (18)",   # CURLE_PARTIAL_FILE
    "curl: (28)",   # CURLE_OPERATION_TIMEDOUT
    "curl: (35)",   # CURLE_SSL_CONNECT_ERROR
    "timed out",
    "timeout",
    "temporarily unavailable",
    "503",
    "429",
)

_MAX_RETRIES = 5
_RETRY_BASE_DELAY = 2.0   # seconds


def _is_retryable(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(p in msg for p in _RETRYABLE_PATTERNS)


async def _download_pdf(
    session: AsyncSession,
    sem: asyncio.Semaphore,
    url: str,
    dest: Path,
) -> bool:
    """Download *url* to *dest*. Returns True if a new file was written.

    Retries up to _MAX_RETRIES times with exponential backoff + jitter for
    transient network errors (e.g. curl error 56 – connection closed abruptly).
    """
    if dest.exists():
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)

    headers = {
        "Referer": REGULATIONS_URL,
        **DEFAULT_HEADERS
    }

    # Small random stagger so parallel downloads don't all hit the server at once
    await asyncio.sleep(random.uniform(0.0, 1.5))

    last_exc: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            async with sem:
                resp = await session.get(url, headers=headers, timeout=120)
                resp.raise_for_status()
                content = resp.content

            async with aiofiles.open(dest, "wb") as fh:
                await fh.write(content)

            log.info("Downloaded: %s (%d KB)", dest, len(content) // 1024)
            return True

        except Exception as exc:
            last_exc = exc
            # Remove any partial file before retrying
            if dest.exists():
                dest.unlink()

            if attempt < _MAX_RETRIES and _is_retryable(exc):
                delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
                log.warning(
                    "Failed to download %s (attempt %d/%d): %s — retrying in %.1fs",
                    url, attempt, _MAX_RETRIES, exc, delay,
                )
                await asyncio.sleep(delay)
            else:
                break

    log.error("Failed to download %s: %s", url, last_exc)
    return False


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


def _text(tag) -> str:
    return tag.get_text(separator=" ", strip=True) if tag else ""


def _parse_year(text: str) -> int | None:
    """Extract a 4-digit year from a header string like '2026 Regulations'."""
    m = re.search(r"\b(20\d{2})\b", text)
    return int(m.group(1)) if m else None


def _extract_docs_from_soup(soup: BeautifulSoup) -> list[dict]:
    """Extract PDF links and their metadata (year, category, title, published_time)
    from the regulations page. Handles complex nested DOM structures.
    """
    docs: list[dict] = []
    seen_paths: set[str] = set()

    for li in soup.find_all("li", class_="list-item"):
        a_tag = li.find("a", href=True)
        if a_tag is None:
            continue
        href: str = a_tag["href"]
        if not href.lower().endswith(".pdf"):
            continue

        url = urljoin(BASE_URL, href) if not href.startswith("http") else href
        if url in seen_paths:
            continue
        seen_paths.add(url)

        # Traverse up to find category and year
        category = None
        year = None
        
        for vg in li.find_parents("div", class_="view-grouping"):
            header = vg.find("div", class_="view-grouping-header", recursive=False)
            header_text = _text(header)
            if not header_text:
                continue
                
            candidate_year = _parse_year(header_text)
            if candidate_year is not None:
                if year is None: year = candidate_year
            elif header_text.lower() not in {"archives", ""}:
                if category is None: category = header_text

        # Title: prefer dedicated title div, fall back to link text or URL stem
        title_div = li.find(class_="title") or li.find(class_="field-title")
        title = _text(title_div) or _text(a_tag) or Path(href).stem

        # Published date if present
        date_span = li.find("span", class_="date-display-single")
        published_time = _text(date_span)

        docs.append(
            {
                "url": url,
                "title": title,
                "published_time": published_time,
                "year": year or 0,
                "category": category or "general",
                "filename": slugify(title) + ".pdf",
            }
        )

    return docs


# ---------------------------------------------------------------------------
# Main scrape routine
# ---------------------------------------------------------------------------


async def scrape(output_dir: str = "regs") -> int:
    output = Path(output_dir)
    manifest_path = output / "manifest.json"
    manifest: dict = load_manifest_with_lock(manifest_path)

    async with AsyncSession() as session:
        log.info("Fetching regulations index: %s", REGULATIONS_URL)
        html = await _fetch_text(session, REGULATIONS_URL)
        if not html:
            log.error("Failed to fetch the regulations page — aborting.")
            return 0

        soup = BeautifulSoup(html, "html.parser")
        docs = _extract_docs_from_soup(soup)
        log.info("Found %d regulation PDF link(s) on the page.", len(docs))

        # Build download queue — skip URLs already in the manifest
        assigned: set[Path] = set()
        queue: list[tuple[str, Path, dict]] = []

        for doc in docs:
            url = doc["url"]

            if url in manifest:
                # Update published_time if it was missing before
                if not manifest[url].get("published_time") and doc["published_time"]:
                    manifest[url]["published_time"] = doc["published_time"]
                continue

            cat_slug = slugify(doc["category"])
            dest = output / str(doc["year"]) / cat_slug / doc["filename"]

            # Deduplicate destination paths within this run
            base = dest.stem
            counter = 1
            while dest in assigned or dest.exists():
                dest = dest.with_name(f"{base}-{counter}.pdf")
                counter += 1
            assigned.add(dest)

            queue.append(
                (
                    url,
                    dest,
                    {
                        "year": doc["year"],
                        "category": doc["category"],
                        "title": doc["title"],
                        "source": "regulation",
                        "published_time": doc["published_time"],
                    },
                )
            )

        log.info("%d new regulation(s) to download.", len(queue))

        new_count = 0
        if queue:
            sem = asyncio.Semaphore(MAX_DOWNLOAD_CONCURRENT)
            results = await asyncio.gather(
                *(_download_pdf(session, sem, url, dest) for url, dest, _ in queue)
            )

            for (url, dest, meta), downloaded in zip(queue, results):
                if downloaded:
                    new_count += 1
                    manifest[url] = {**meta, "path": dest.as_posix()}

        # Always persist updated manifest (even just to update published_time)
        save_manifest_with_lock(manifest_path, manifest)

    return new_count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    import sys

    output_dir = "regs"
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--output-dir" and i + 1 < len(args):
            output_dir = args[i + 1]
            i += 2
        else:
            i += 1

    new_count = asyncio.run(scrape(output_dir=output_dir))
    log.info("Done. %d new regulation PDF(s) downloaded.", new_count)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as fh:
            fh.write(f"new_regs={new_count}\n")


if __name__ == "__main__":
    main()
