"""FIA F1 Transcript Scraper

Extracts Friday, Saturday, and Sunday interview transcripts from the FIA website.
Saves them as Markdown files in documents/[year]/[event]/transcripts/.
"""

import asyncio
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

BASE_URL = "https://www.fia.com"
NEWS_URL_BASE = f"{BASE_URL}/news/"

# Concurrency limits
MAX_FETCH_CONCURRENT = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared utilities (mirrored from scraper.py for consistency)
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")

def load_manifest(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}

def save_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True))

# ---------------------------------------------------------------------------
# Transcript Parsing
# ---------------------------------------------------------------------------

def clean_transcript_html(html: str) -> str:
    """Extracts the transcript text from FIA news article HTML and converts to Markdown."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Try common content containers
    content = soup.find(class_="node__content") or \
              soup.find(class_="field-items") or \
              soup.find(class_="field-item even")
              
    if not content:
        # Fallback: find the div that contains the most <p> tags
        divs = soup.find_all("div")
        content = max(divs, key=lambda d: len(d.find_all("p")), default=None)

    if not content:
        return ""

    lines = []
    for p in content.find_all(["p", "h1", "h2", "h3", "h4"]):
        # Special handling for speaker names (often in <strong> or <b>)
        for strong in p.find_all(["strong", "b"]):
            name = strong.get_text(strip=True)
            if name and name.isupper() and (name.endswith(":") or len(name) < 30):
                strong.replace_with(f"**{name}**")
        
        text = p.get_text(strip=True)
        if text:
            # Handle Q: and A: specifically if not already bolded
            text = re.sub(r"^(Q:)", r"**\1**", text)
            text = re.sub(r"^(A:)", r"**\1**", text)
            lines.append(text)

    return "\n\n".join(lines)

async def fetch_transcript(
    session: AsyncSession,
    url: str,
    dest: Path,
) -> bool:
    """Fetch a single transcript and save as Markdown. Returns True if saved."""
    try:
        resp = await session.get(url, timeout=30)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        
        md_content = clean_transcript_html(resp.text)
        if not md_content or len(md_content) < 200: # Sanity check for too-short content
            return False
            
        dest.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(dest, "w", encoding="utf-8") as f:
            await f.write(md_content)
            
        log.info("Saved transcript: %s", dest)
        return True
    except Exception as e:
        log.warning("Failed to fetch %s: %s", url, e)
        return False

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def scrape_transcripts(
    output_dir: str = "documents",
    year_filter: int = 2026,
) -> int:
    output = Path(output_dir)
    manifest_path = output / "manifest.json"
    discovery_path = output / "discovery_cache.json"
    manifest = load_manifest(manifest_path)

    # 1. Get events for the year
    events = []
    if discovery_path.exists():
        try:
            data = json.loads(discovery_path.read_text())
            events = [e for e in data.get("events", []) if e["year"] == year_filter]
            log.info("Loaded %d events from discovery cache for %d", len(events), year_filter)
        except Exception as e:
            log.error("Failed to read discovery cache: %s", e)

    if not events:
        log.warning("No events found for %d. Please run scraper.py first to populate discovery cache.", year_filter)
        return 0

    async with AsyncSession(impersonate="chrome120") as session:
        fetch_tasks = []
        task_metadata = []

        day_types = {
            "thursday": "thursday",
            "friday": "friday",
            "post-qualifying": "saturday",
            "post-race": "sunday"
        }

        total_discovered = 0
        
        for event in events:
            gp_slug = slugify(event["name"])
            
            for fia_type, local_day in day_types.items():
                # Construct hypothesized FIA news URL
                # Pattern: f1-[year]-[gp-slug]-[fia_type]-press-conference-transcript
                url_name = f"f1-{year_filter}-{gp_slug}-{fia_type}-press-conference-transcript"
                url = urljoin(NEWS_URL_BASE, url_name)
                
                if url in manifest and Path(manifest[url]["path"]).exists():
                    continue

                dest_path = output / str(year_filter) / gp_slug / "transcripts" / f"{local_day}.md"
                
                fetch_tasks.append(fetch_transcript(session, url, dest_path))
                task_metadata.append({
                    "url": url,
                    "event": event["name"],
                    "year": year_filter,
                    "type": local_day,
                    "title": f"{local_day.capitalize()} Press Conference Transcript",
                    "path": str(dest_path)
                })

        if not fetch_tasks:
            log.info("No new transcripts to fetch.")
            return 0

        # Execute in chunks to respect concurrency
        log.info("Checking %d hypothesized transcript URLs...", len(fetch_tasks))
        results = []
        for i in range(0, len(fetch_tasks), MAX_FETCH_CONCURRENT):
            chunk = fetch_tasks[i : i + MAX_FETCH_CONCURRENT]
            results.extend(await asyncio.gather(*chunk))

        # Update manifest
        new_count = 0
        for meta, success in zip(task_metadata, results):
            if success:
                manifest[meta["url"]] = {
                    "year": meta["year"],
                    "event": meta["event"],
                    "title": meta["title"],
                    "source": "transcript",
                    "path": meta["path"]
                }
                new_count += 1

        if new_count > 0:
            save_manifest(manifest_path, manifest)
            log.info("Manifest updated with %d new transcripts.", new_count)
            
        return new_count

def main() -> None:
    import sys
    year = 2026
    if "--year" in sys.argv:
        idx = sys.argv.index("--year")
        if idx + 1 < len(sys.argv):
            year = int(sys.argv[idx + 1])

    asyncio.run(scrape_transcripts(year_filter=year))

if __name__ == "__main__":
    main()
