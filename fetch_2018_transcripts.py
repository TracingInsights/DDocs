import asyncio
import json
import logging
import os
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from transcript_scraper import fetch_transcript, load_manifest, save_manifest, slugify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.fia.com"
SEASON_2018_URL = f"{BASE_URL}/events/fia-formula-one-world-championship/season-2018/2018-fia-formula-one-world-championship"
NEWS_URL_BASE = f"{BASE_URL}/news/"

# Concurrency limits
MAX_FETCH_CONCURRENT = 5

async def discover_2018_gps(session: AsyncSession) -> list[dict]:
    log.info("Discovering 2018 Grand Prix events from season page...")
    try:
        resp = await session.get(SEASON_2018_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        gps = []
        # Find all GP links (usually in the championship-events-list or similar)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/events/fia-formula-one-world-championship/season-2018/" in href and not href.endswith("2018-fia-formula-one-world-championship"):
                name = a.get_text(strip=True)
                if not name or "Grand Prix" not in name: continue
                # Basic cleaning of the name
                name = name.split("(")[0].strip()
                gps.append({
                    "name": name,
                    "url": urljoin(BASE_URL, href),
                    "slug": slugify(name)
                })
        
        # Dedupe by slug
        seen = set()
        deduped = []
        for g in gps:
            if g["slug"] not in seen:
                deduped.append(g)
                seen.add(g["slug"])
        
        log.info(f"Found {len(deduped)} Grand Prix events.")
        return deduped
    except Exception as e:
        log.error(f"Failed to discover GPs: {e}")
        return []

async def get_transcript_urls_from_gp_page(session: AsyncSession, gp: dict) -> list[dict]:
    log.info(f"Crawling GP page for {gp['name']}: {gp['url']}")
    found_transcripts = []
    try:
        resp = await session.get(gp["url"], timeout=30)
        if resp.status_code != 200: return []
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Look for links containing "transcript" and day names
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"].lower()
            
            if "transcript" in text or "transcript" in href:
                url = urljoin(BASE_URL, a["href"])
                title = a.get_text(strip=True)
                
                day = "extra"
                if "thursday" in text or "thursday" in href: day = "thursday"
                elif "friday" in text or "friday" in href: day = "friday"
                elif "saturday" in text or "saturday" in href or "post-qualifying" in text or "post-qualifying" in href: day = "saturday"
                elif "sunday" in text or "sunday" in href or "post-race" in text or "post-race" in href: day = "sunday"
                
                found_transcripts.append({
                    "url": url,
                    "day": day,
                    "title": title
                })
        
        # Dedupe by URL
        seen_urls = set()
        final_list = []
        for t in found_transcripts:
            if t["url"] not in seen_urls:
                final_list.append(t)
                seen_urls.add(t["url"])
        
        return final_list
    except Exception as e:
        log.warning(f"Error crawling GP page {gp['name']}: {e}")
        return []

async def process_gp(session: AsyncSession, gp: dict, manifest: dict, output_dir: Path, lock: asyncio.Lock):
    # 1. Discovery from page
    extracted = await get_transcript_urls_from_gp_page(session, gp)
    
    # 2. Add hypothesized URLs (pattern-based discovery)
    common_patterns = [
        "thursday-press-conference-transcript",
        "friday-press-conference-transcript",
        "saturday-post-qualifying-press-conference-transcript",
        "saturday-press-conference-transcript",
        "sunday-post-race-press-conference-transcript",
        "sunday-press-conference-transcript"
    ]
    
    hypothesized = []
    for pattern in common_patterns:
        # e.g., f1-australian-grand-prix-thursday-press-conference-transcript
        url = urljoin(NEWS_URL_BASE, f"f1-{gp['slug']}-{pattern}")
        hypothesized.append(url)
        
    # Dedupe and merge
    to_fetch = []
    seen = {e["url"] for e in extracted}
    for e in extracted:
        to_fetch.append(e)
        
    for url in hypothesized:
        if url not in seen:
            day = "extra"
            if "thursday" in url: day = "thursday"
            elif "friday" in url: day = "friday"
            elif "saturday" in url: day = "saturday"
            elif "sunday" in url: day = "sunday"
            to_fetch.append({"url": url, "day": day, "title": f"Hypothesized {day.capitalize()}"})
            seen.add(url)
            
    tasks = []
    meta = []
    for item in to_fetch:
        url = item["url"]
        day = item["day"]
        
        async with lock:
            if url in manifest: continue
        
        dest = output_dir / "2018" / gp["slug"] / "transcripts" / f"{day}.md"
        # If we have multiple for the same day (e.g. from page and hypothesis), avoid overwriting if one already succeeded
        if dest.exists() and dest.stat().st_size > 500: continue
        
        # If 'extra', we might want a better name or suffix
        if day == "extra":
            dest = output_dir / "2018" / gp["slug"] / "transcripts" / f"transcript_{hash(url) % 1000}.md"
            
        tasks.append(fetch_transcript(session, url, dest))
        meta.append({"url": url, "day": day, "dest": dest, "gp": gp["name"]})
        
    if tasks:
        results = await asyncio.gather(*tasks)
        found_count = 0
        async with lock:
            for m, success in zip(meta, results):
                if success:
                    manifest[m["url"]] = {
                        "year": 2018,
                        "event": m["gp"],
                        "title": f"{m['day'].capitalize()} Transcript",
                        "source": "transcript",
                        "path": str(m["dest"]).replace("\\", "/") # Unix style for manifest
                    }
                    found_count += 1
        return found_count
    return 0

async def main():
    import sys
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        limit = int(sys.argv[idx + 1])
        
    output_dir = Path("documents")
    manifest_path = output_dir / "manifest.json"
    manifest = load_manifest(manifest_path)
    lock = asyncio.Lock()
    
    async with AsyncSession(impersonate="chrome120") as session:
        gps = await discover_2018_gps(session)
        if limit:
            gps = gps[:limit]
            
        total_found = 0
        # Process GPs sequentially or with slight concurrency to be polite
        for gp in gps:
            log.info(f"--- Processing {gp['name']} ---")
            found = await process_gp(session, gp, manifest, output_dir, lock)
            total_found += found
            if found > 0:
                save_manifest(manifest_path, manifest)
                
        log.info(f"Finished. Total 2018 transcripts added: {total_found}")

if __name__ == "__main__":
    asyncio.run(main())
