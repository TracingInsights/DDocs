import asyncio
import json
import logging
import os
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from transcript_scraper import clean_transcript_html, load_manifest, save_manifest, slugify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.fia.com"
NEWS_URL_BASE = f"{BASE_URL}/news/"

# 2018 Season GP list with Slugs
GPS_2018 = [
    "australian-grand-prix",
    "bahrain-grand-prix",
    "chinese-grand-prix",
    "azerbaijan-grand-prix",
    "spanish-grand-prix",
    "monaco-grand-prix",
    "canadian-grand-prix",
    "french-grand-prix",
    "austrian-grand-prix",
    "british-grand-prix",
    "german-grand-prix",
    "hungarian-grand-prix",
    "belgian-grand-prix",
    "italian-grand-prix",
    "singapore-grand-prix",
    "russian-grand-prix",
    "japanese-grand-prix",
    "united-states-grand-prix",
    "mexican-grand-prix",
    "brazilian-grand-prix",
    "abu-dhabi-grand-prix"
]

# Patterns found during research
# Pattern A: f1-[gp-slug]-[session]-press-conference-transcript (e.g. Australian GP)
# Pattern B: f1-[year]-[gp-slug]-[session]-press-conference-transcript (e.g. Abu Dhabi GP)
# Suffixes vary for Saturday and Sunday

async def fetch_and_save(session: AsyncSession, url: str, dest: Path) -> bool:
    try:
        resp = await session.get(url, timeout=30)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        
        md_content = clean_transcript_html(resp.text)
        if not md_content or len(md_content) < 500:
            return False
            
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(md_content, encoding="utf-8")
        log.info(f"Saved: {dest}")
        return True
    except Exception as e:
        log.debug(f"Failed fetch {url}: {e}")
        return False

async def process_gp_session(session: AsyncSession, gp_slug: str, session_type: str, manifest: dict, output_dir: Path):
    year = 2018
    gp_name = gp_slug.replace("-", " ").title()
    
    # Define candidates based on session type
    suffixes = []
    if session_type == "thursday":
        suffixes = ["thursday"]
    elif session_type == "friday":
        suffixes = ["friday"]
    elif session_type == "saturday":
        suffixes = ["saturday-post-qualifying", "saturday"]
    elif session_type == "sunday":
        suffixes = ["sunday-post-race", "sunday"]

    day_dest_name = {
        "thursday": "thursday.md",
        "friday": "friday.md",
        "saturday": "saturday.md",
        "sunday": "sunday.md"
    }[session_type]

    dest_path = output_dir / str(year) / gp_slug / "transcripts" / day_dest_name
    
    # Generate all candidate URLs (with and without year)
    candidates = []
    for suffix in suffixes:
        candidates.append(urljoin(NEWS_URL_BASE, f"f1-{year}-{gp_slug}-{suffix}-press-conference-transcript"))
        candidates.append(urljoin(NEWS_URL_BASE, f"f1-{gp_slug}-{suffix}-press-conference-transcript"))

    for url in candidates:
        if url in manifest and dest_path.exists():
            log.info(f"Skipping (already in manifest): {url}")
            return True
        
        if await fetch_and_save(session, url, dest_path):
            manifest[url] = {
                "year": year,
                "event": gp_name,
                "title": f"{session_type.capitalize()} Transcript",
                "source": "transcript",
                "path": str(dest_path).replace("\\", "/") # Unix-style for manifest
            }
            return True
    
    log.warning(f"No transcript found for {gp_name} - {session_type}")
    return False

async def main():
    import sys
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        limit = int(sys.argv[idx + 1])

    output_dir = Path("documents")
    manifest_path = output_dir / "manifest.json"
    manifest = load_manifest(manifest_path)
    
    async with AsyncSession(impersonate="chrome120") as session:
        gps = GPS_2018
        if limit:
            gps = gps[:limit]
            
        for gp_slug in gps:
            log.info(f"--- Processing {gp_slug} ---")
            for session_type in ["thursday", "friday", "saturday", "sunday"]:
                await process_gp_session(session, gp_slug, session_type, manifest, output_dir)
            
            # Save manifest after each GP to avoid loss
            save_manifest(manifest_path, manifest)

    log.info("Finished.")

if __name__ == "__main__":
    asyncio.run(main())
