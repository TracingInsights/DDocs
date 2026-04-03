import asyncio
import json
import logging
import os
from pathlib import Path
from urllib.parse import urljoin
from curl_cffi.requests import AsyncSession
from transcript_scraper import fetch_transcript, load_manifest, save_manifest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
BASE_URL = "https://www.fia.com/news/"

async def test_fetch(year, gp, day):
    manifest_path = Path("documents/manifest.json")
    manifest = load_manifest(manifest_path)
    output_dir = Path("documents")
    
    async with AsyncSession(impersonate="chrome120") as session:
        url_day = day
        # Sometimes it's wednesday for thursday
        variants_days = [day, "wednesday" if day == "thursday" else day]
        
        variants_gp = [gp, gp.replace("-grand-prix", "-gp"), gp.replace("-grand-prix", "")]
        permutations = []
        for variant_gp in variants_gp:
            for d in variants_days:
                # Basic patterns
                permutations.extend([
                    f"f1-{year}-{variant_gp}-{d}-press-conference",
                    f"f1-{year}-{variant_gp}-{d}-press-conference-transcript",
                    f"{year}-{variant_gp}-{d}-press-conference",
                    f"{year}-{variant_gp}-{d}-press-conference-transcript",
                    f"fia-formula-1-{variant_gp}-{d}-press-conference",
                    f"{variant_gp}-{year}-{d}-press-conference",
                ])
                
                # Variants with suffixes
                for i in range(3):
                    permutations.append(f"f1-{year}-{variant_gp}-{d}-press-conference-{i}")
                
                # Specific for Sunday
                if day == "sunday":
                    permutations.extend([
                        f"f1-{year}-{variant_gp}-sunday-post-race-press-conference-transcript",
                        f"f1-{year}-{variant_gp}-post-race-press-conference-transcript",
                        f"f1-{year}-{variant_gp}-post-race-press-conference",
                        f"{year}-{variant_gp}-post-race-press-conference-transcript",
                        f"{year}-{variant_gp}-sunday-post-race-press-conference-transcript",
                        f"f1-{year}-{variant_gp}-race-press-conference-transcript",
                        f"f1-{year}-{variant_gp}-sunday-race-press-conference-transcript"
                    ])
                
                # Specific for Saturday
                if day == "saturday":
                    permutations.extend([
                        f"f1-{year}-{variant_gp}-saturday-post-qualifying-press-conference-transcript",
                        f"f1-{year}-{variant_gp}-post-qualifying-press-conference-transcript",
                        f"f1-{year}-{variant_gp}-post-qualifying-press-conference",
                        f"{year}-{variant_gp}-post-qualifying-press-conference-transcript",
                        f"{year}-{variant_gp}-saturday-post-qualifying-press-conference-transcript",
                        f"f1-{year}-{variant_gp}-qualifying-press-conference-transcript",
                        f"f1-{year}-{variant_gp}-saturday-qualifying-press-conference-transcript"
                    ])
                
                # General "transcript" suffix
                permutations.append(f"f1-{year}-{variant_gp}-press-conference-transcript")
            
        for perm in set(permutations):
            url = urljoin(BASE_URL, perm)
            dest = output_dir / str(year) / gp / "transcripts" / f"{day}.md"
            if dest.exists() and dest.stat().st_size > 500:
                log.info(f"ALREADY EXISTS: {dest}")
                continue
            
            # Using the fetch_transcript from transcript_scraper.py
            from transcript_scraper import fetch_transcript
            success = await fetch_transcript(session, url, dest)
            if success:
                log.info(f"SUCCESS: {url}")
                return True
    log.warning(f"FAILED: {year} {gp} {day}")
    return False

if __name__ == "__main__":
    # Test for 2018 Abu Dhabi Sunday
    asyncio.run(test_fetch(2018, "abu-dhabi-grand-prix", "sunday"))
