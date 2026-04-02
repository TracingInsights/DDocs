import asyncio
import json
import logging
from pathlib import Path
from urllib.parse import urljoin

from curl_cffi.requests import AsyncSession
from transcript_scraper import fetch_transcript, load_manifest, save_manifest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BASE_URL = "https://www.fia.com/news/"

async def process_day(session, year, gp, day, manifest, manifest_path, output_dir, lock):
    url_day = day
    if day == "post-qualifying": url_day = "saturday"
    if day == "post-race": url_day = "sunday"
    # Sometimes thursday transcripts are published as wednesday
    day_variants = [url_day]
    if day == "thursday": day_variants.append("wednesday")
    
    variants_gp = [gp, gp.replace("-grand-prix", "-gp"), gp.replace("-grand-prix", "")]
    
    permutations = []
    for variant in variants_gp:
        for d in day_variants:
            # Basic patterns
            base_patterns = [
                f"f1-{year}-{variant}-{d}-press-conference",
                f"f1-{year}-{variant}-{d}-press-conference-transcript",
                f"{year}-{variant}-{d}-press-conference",
                f"{year}-{variant}-{d}-press-conference-transcript",
                f"fia-formula-1-{variant}-{d}-press-conference",
                f"{variant}-{year}-{d}-press-conference",
            ]
            permutations.extend(base_patterns)
            
            # Variants with suffixes
            for i in range(3):
                permutations.append(f"f1-{year}-{variant}-{d}-press-conference-{i}")
            
            # Specific for Sunday
            if day == "sunday":
                permutations.extend([
                    f"f1-{year}-{variant}-sunday-post-race-press-conference-transcript",
                    f"f1-{year}-{variant}-post-race-press-conference-transcript",
                    f"f1-{year}-{variant}-post-race-press-conference",
                    f"{year}-{variant}-post-race-press-conference-transcript",
                    f"{year}-{variant}-sunday-post-race-press-conference-transcript",
                    f"f1-{year}-{variant}-race-press-conference-transcript",
                    f"f1-{year}-{variant}-sunday-race-press-conference-transcript"
                ])
            
            # Specific for Saturday
            if day == "saturday":
                permutations.extend([
                    f"f1-{year}-{variant}-saturday-post-qualifying-press-conference-transcript",
                    f"f1-{year}-{variant}-post-qualifying-press-conference-transcript",
                    f"f1-{year}-{variant}-post-qualifying-press-conference",
                    f"{year}-{variant}-post-qualifying-press-conference-transcript",
                    f"{year}-{variant}-saturday-post-qualifying-press-conference-transcript",
                    f"f1-{year}-{variant}-qualifying-press-conference-transcript",
                    f"f1-{year}-{variant}-saturday-qualifying-press-conference-transcript"
                ])
            
            # General "transcript" suffix
            permutations.append(f"f1-{year}-{variant}-press-conference-transcript")

    for perm in set(permutations):
        url = urljoin(BASE_URL, perm)
        
        async with lock:
            if url in manifest:
                return 0
                
        dest = output_dir / str(year) / gp / "transcripts" / f"{day}.md"
        if dest.exists() and dest.stat().st_size > 500:
            return 0
            
        success = await fetch_transcript(session, url, dest)
        if success:
            log.info(f"FOUND missing transcript: {url}")
            async with lock:
                manifest[url] = {
                    "year": year, 
                    "event": gp, 
                    "title": f"{day.capitalize()} Transcript", 
                    "source": "transcript", 
                    "path": str(dest)
                }
                save_manifest(manifest_path, manifest)
            return 1
    
    log.warning(f"Failed to find {year} {gp} {day}")
    return 0

async def main():
    import sys
    filter_year = None
    if "--year" in sys.argv:
        idx = sys.argv.index("--year")
        filter_year = int(sys.argv[idx + 1])

    missing = json.load(open("missing_transcripts_final.json", "r"))
    manifest_path = Path("documents/manifest.json")
    manifest = load_manifest(manifest_path)
    output_dir = Path("documents")
    lock = asyncio.Lock()
    
    total_found = 0
    tasks = []
    
    async with AsyncSession(impersonate="chrome120") as session:
        for item in missing:
            year = item["year"]
            if filter_year and year != filter_year:
                continue
                
            gp = item["gp"]
            missing_days = item["missing"]
            
            for day in missing_days:
                tasks.append(process_day(session, year, gp, day, manifest, manifest_path, output_dir, lock))
        
        # Limit concurrency
        MAX_CONCURRENT = 5
        for i in range(0, len(tasks), MAX_CONCURRENT):
            chunk = tasks[i:i + MAX_CONCURRENT]
            results = await asyncio.gather(*chunk)
            total_found += sum(results)
                    
    log.info(f"Total found: {total_found}")

if __name__ == "__main__":
    asyncio.run(main())
