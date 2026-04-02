import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, quote_plus

import aiofiles
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

BASE_URL = "https://www.fia.com"
NEWS_URL_BASE = f"{BASE_URL}/news/"
ARCHIVE_URL_2018 = f"{BASE_URL}/f1-archives?season=866"

# Concurrency limits
MAX_FETCH_CONCURRENT = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared utilities
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
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

# ---------------------------------------------------------------------------
# Discovery & Search
# ---------------------------------------------------------------------------

async def get_discovery_events(session: AsyncSession, year: int, discovery_path: Path) -> list[dict]:
    """Load events from cache or fallback to archives for 2018."""
    events = []
    if discovery_path.exists():
        try:
            data = json.loads(discovery_path.read_text(encoding="utf-8"))
            events = [e for e in data.get("events", []) if e.get("year") == year]
        except Exception as e:
            log.error("Failed to read discovery cache: %s", e)

    if not events and year == 2018:
        log.info("Populating 2018 events from FIA archive...")
        try:
            resp = await session.get(ARCHIVE_URL_2018)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for table in soup.find_all("table", class_="views-table"):
                caption = table.find("caption")
                if not caption: continue
                link = caption.find("a")
                if not link: continue
                name = link.get_text(separator=" ", strip=True).split("-")[0].strip()
                events.append({"name": name, "slug": slugify(name), "year": 2018})
        except Exception as e:
            log.error("Failed to scrape 2018 archive: %s", e)
            
    return events

async def search_fia_transcripts(session: AsyncSession, year: int, event_name: str) -> list[str]:
    """Search the FIA website for press conference transcripts for a specific event."""
    query = f'"{year}" "{event_name}" "Press Conference Transcript"'
    search_url = f"{BASE_URL}/site-search?search_api_views_fulltext={quote_plus(query)}"
    
    try:
        resp = await session.get(search_url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        links = []
        for result in soup.find_all(class_="search-result"):
            link_tag = result.find("a", href=lambda h: h and "/news/" in h and "transcript" in h.lower())
            if link_tag:
                links.append(urljoin(BASE_URL, link_tag["href"]))
        return list(set(links))
    except Exception as e:
        log.warning("Search failed for %s %d: %s", event_name, year, e)
        return []

# ---------------------------------------------------------------------------
# Transcript Parsing
# ---------------------------------------------------------------------------

def clean_transcript_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find(class_="node__content") or \
              soup.find(class_="field-items") or \
              soup.find(class_="field-item even")
              
    if not content:
        divs = soup.find_all("div")
        content = max(divs, key=lambda d: len(d.find_all("p")), default=None)

    if not content:
        return ""

    lines = []
    for p in content.find_all(["p", "h1", "h2", "h3", "h4"]):
        for strong in p.find_all(["strong", "b"]):
            name = strong.get_text(strip=True)
            if name and name.isupper() and (name.endswith(":") or len(name) < 35):
                strong.replace_with(f"**{name}**")
        
        text = p.get_text(strip=True)
        if text:
            text = re.sub(r"^(Q:)", r"**\1**", text)
            text = re.sub(r"^(A:)", r"**\1**", text)
            lines.append(text)

    return "\n\n".join(lines)

async def fetch_transcript(session: AsyncSession, url: str, dest: Path) -> bool:
    try:
        if dest.exists() and dest.stat().st_size > 500:
            return True
            
        resp = await session.get(url, timeout=30)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        
        md_content = clean_transcript_html(resp.text)
        if not md_content or len(md_content) < 500:
            return False
            
        dest.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(dest, "w", encoding="utf-8") as f:
            await f.write(md_content)
        log.info("Saved: %s", dest.name)
        return True
    except Exception as e:
        log.warning("Failed fetch %s: %s", url, e)
        return False

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def scrape_year(session: AsyncSession, year: int, output_dir: Path, manifest: dict, discovery_path: Path):
    events = await get_discovery_events(session, year, discovery_path)
    if not events:
        log.warning("No events found for %d", year)
        return 0

    log.info("Processing %d events for %d", len(events), year)
    
    total_added = 0
    day_types = {"thursday": "thursday", "friday": "friday", "post-qualifying": "saturday", "post-race": "sunday"}

    for event in events:
        gp_name = event["name"]
        gp_slug = event.get("slug", slugify(gp_name)) # Fix: generate slug if missing from cache
        
        local_tasks = []
        local_meta = []

        # 1. Hypothesize URLs
        slug_variants = [gp_slug]
        if "grand-prix" in gp_slug:
            slug_variants.append(gp_slug.replace("-grand-prix", "-gp"))
        
        for variant in slug_variants:
            for fia_type, local_day in day_types.items():
                url_name = f"f1-{year}-{variant}-{fia_type}-press-conference-transcript"
                url = urljoin(NEWS_URL_BASE, url_name)
                
                dest = output_dir / str(year) / gp_slug / "transcripts" / f"{local_day}.md"
                if url in manifest and dest.exists():
                    continue

                local_tasks.append(fetch_transcript(session, url, dest))
                local_meta.append({"url": url, "event": gp_name, "year": year, "title": f"{local_day.capitalize()} Transcript", "path": str(dest)})

        # 2. Search Fallback
        if year <= 2022:
            search_urls = await search_fia_transcripts(session, year, gp_name)
            for i, url in enumerate(search_urls):
                if url in manifest: continue
                
                # Heuristic for day
                day_key = "extra"
                u_lower = url.lower()
                if "thursday" in u_lower: day_key = "thursday"
                elif "friday" in u_lower: day_key = "friday"
                elif "saturday" in u_lower or "qualifying" in u_lower: day_key = "saturday"
                elif "sunday" in u_lower or "race" in u_lower: day_key = "sunday"
                
                dest = output_dir / str(year) / gp_slug / "transcripts" / f"{day_key}_found_{i}.md"
                local_tasks.append(fetch_transcript(session, url, dest))
                local_meta.append({"url": url, "event": gp_name, "year": year, "title": f"{day_key.capitalize()} Transcript (Search)", "path": str(dest)})

        if local_tasks:
            results = []
            for j in range(0, len(local_tasks), MAX_FETCH_CONCURRENT):
                chunk = local_tasks[j:j+MAX_FETCH_CONCURRENT]
                results.extend(await asyncio.gather(*chunk))
                
            for meta, success in zip(local_meta, results):
                if success:
                    manifest[meta["url"]] = {"year": year, "event": meta["event"], "title": meta["title"], "source": "transcript", "path": meta["path"]}
                    total_added += 1

    return total_added

async def main():
    import sys
    years = [2026]
    if "--year" in sys.argv:
        idx = sys.argv.index("--year")
        years = [int(sys.argv[idx + 1])]
    elif "--all-historical" in sys.argv:
        years = [2022, 2021, 2020, 2019, 2018]

    output_dir = Path("documents")
    manifest_path = output_dir / "manifest.json"
    discovery_path = output_dir / "discovery_cache.json"
    manifest = load_manifest(manifest_path)

    async with AsyncSession(impersonate="chrome120") as session:
        for year in years:
            log.info("--- Starting Season %d ---", year)
            count = await scrape_year(session, year, output_dir, manifest, discovery_path)
            if count > 0:
                save_manifest(manifest_path, manifest)
                log.info("Season %d: Added %d new transcripts.", year, count)

if __name__ == "__main__":
    asyncio.run(main())
