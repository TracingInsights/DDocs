import asyncio
import json
import logging
import os
import re
import io
import time
from pathlib import Path
from urllib.parse import urljoin

import aiofiles
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from pypdf import PdfReader

BASE_URL = "https://www.fia.com"
NEWS_URL_BASE = f"{BASE_URL}/news/"
ARCHIVE_URL_2018 = f"{BASE_URL}/f1-archives?season=866"

# Concurrency limits
MAX_FETCH_CONCURRENT = 5

# Discovery cache TTL (2.5 hours, slightly under 3-hour cron interval)
TRANSCRIPT_DISCOVERY_CACHE_TTL_SECONDS = 2.5 * 60 * 60

# Cache version - increment when discovery logic changes
CACHE_VERSION = 1

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
# Discovery cache
# ---------------------------------------------------------------------------

def load_transcript_discovery_cache(path: Path) -> dict | None:
    """Load cached transcript discovery results if still valid."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        
        # Validate cache version
        if data.get("version") != CACHE_VERSION:
            log.info("Cache version mismatch (expected %d, got %s), re-crawling", CACHE_VERSION, data.get("version"))
            return None
        
        # Validate required keys
        required_keys = ["year", "mode", "events", "hub_articles", "timing_pdfs", "deep_results", "timestamp"]
        if not all(k in data for k in required_keys):
            log.warning("Cache missing required keys, re-crawling")
            return None
        
        # Check TTL
        age = time.time() - data.get("timestamp", 0)
        if age < TRANSCRIPT_DISCOVERY_CACHE_TTL_SECONDS:
            log.info("Using transcript discovery cache (%.0fs old, TTL=%.0fs)", age, TRANSCRIPT_DISCOVERY_CACHE_TTL_SECONDS)
            return data
        log.info("Transcript discovery cache expired (%.0fs old), re-crawling", age)
    except Exception as e:
        log.warning("Could not read transcript discovery cache: %s", e)
    return None

def save_transcript_discovery_cache(path: Path, data: dict) -> None:
    """Save transcript discovery results with timestamp."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        data["version"] = CACHE_VERSION
        data["timestamp"] = time.time()
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        log.info("Transcript discovery cache saved")
    except Exception as e:
        log.warning("Failed to save transcript discovery cache: %s", e)

# ---------------------------------------------------------------------------
# PDF Handling
# ---------------------------------------------------------------------------

def clean_pdf_text(text: str) -> str:
    """Basic cleanup and speaker bolding for PDF text."""
    lines = []
    # Identify speakers (usually SURNAME: or NAME:)
    speaker_pattern = re.compile(r"^([A-Z][A-Z\s.-]+:)")
    
    for line in text.split("\n"):
        line = line.strip()
        if not line: continue
        
        # Bold speakers
        line = speaker_pattern.sub(r"**\1**", line)
        # Bold Q/A
        line = re.sub(r"^(Q:)", r"**\1**", line)
        line = re.sub(r"^(A:)", r"**\1**", line)
        
        lines.append(line)
        
    return "\n\n".join(lines)

async def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text() + "\n"
        return clean_pdf_text(full_text)
    except Exception as e:
        log.error("PDF extraction failed: %s", e)
        return ""

# ---------------------------------------------------------------------------
# Aggressive Discovery (Hub & Timing)
# ---------------------------------------------------------------------------

async def discover_from_hubs(session: AsyncSession, year: int) -> list[dict]:
    hub_url = f"{BASE_URL}/news/f1-press-conference-transcripts-{year}"
    log.info("Aggressively discovering transcripts from hub: %s", hub_url)
    
    found_articles = []
    page = 0
    while True:
        url = f"{hub_url}?page={page}"
        try:
            resp = await session.get(url, timeout=30)
            if resp.status_code != 200: break
            soup = BeautifulSoup(resp.text, "html.parser")
            results = soup.find_all(class_="views-row")
            if not results: break
            for row in results:
                title_tag = row.find(["h2", "h3", "a"], class_=lambda c: c and "title" in c.lower()) or row.find("a")
                if not title_tag: continue
                title = title_tag.get_text(strip=True)
                link = urljoin(BASE_URL, title_tag.get("href") if title_tag.name == "a" else title_tag.find("a")["href"])
                if "transcript" in title.lower() or "transcript" in link.lower():
                    found_articles.append({"title": title, "url": link})
            if not soup.find("li", class_="pager-next"): break
            page += 1
        except Exception as e:
            log.error("Error paginating hub %s page %d: %s", hub_url, page, e)
            break
    return found_articles

async def discover_pdfs_from_timing(session: AsyncSession, year: int, events: list[dict]) -> list[dict]:
    """Scrapes 'Event & Timing Information' pages for transcript PDFs."""
    log.info("Super-aggressively searching for PDFs in timing pages for %d", year)
    found_pdfs = []
    
    # 2018 Fallback if events are empty
    if year == 2018 and not events:
        events = await get_discovery_events(session, year, Path("documents/discovery_cache.json"))

    # Need to find timing URLs first
    # For 2018, we can get them from the archive page
    timing_urls = []
    if year == 2018:
        try:
            resp = await session.get(ARCHIVE_URL_2018)
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=lambda h: h and "eventtiming-information" in h):
                timing_urls.append(urljoin(BASE_URL, a["href"]))
        except Exception: pass
    else:
        # For other years, hypothesize or search timing pages
        # (Simplified for now: focus on 2018 archive logic)
        pass

    for t_url in list(set(timing_urls)):
        try:
            resp = await session.get(t_url, timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=lambda h: h and h.endswith(".pdf")):
                title = a.get_text(strip=True).lower()
                pdf_url = urljoin(BASE_URL, a["href"])
                if "transcript" in title:
                    found_pdfs.append({"title": a.get_text(strip=True), "url": pdf_url})
        except Exception: continue
        
    return found_pdfs

async def discover_from_season_event_pages(session: AsyncSession, year: int) -> list[dict]:
    """Targeted discovery for a specific year using the season master page."""
    log.info("Deep diving into %d season event pages...", year)
    season_url = f"https://www.fia.com/events/fia-formula-one-world-championship/season-{year}/{year}-fia-formula-one-world-championship"
    found = []
    try:
        resp = await session.get(season_url, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")
        # Find all GP links (usually in the championship-events-list or similar)
        event_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if f"/events/fia-formula-one-world-championship/season-{year}/" in href and not href.endswith(f"{year}-fia-formula-one-world-championship"):
                event_links.append(urljoin(BASE_URL, href))
        
        event_links = list(set(event_links)) # Dedupe
        log.info("Found %d %d event links to crawl.", len(event_links), year)
        
        for e_url in event_links:
            try:
                e_resp = await session.get(e_url, timeout=30)
                e_soup = BeautifulSoup(e_resp.text, "html.parser")
                for a in e_soup.find_all("a", href=True):
                    text = a.get_text(strip=True).lower()
                    href = a["href"].lower()
                    if "press conference transcript" in text or "press-conference-transcript" in href:
                        found.append({"title": a.get_text(strip=True), "url": urljoin(BASE_URL, a["href"])})
            except Exception: continue
    except Exception as e:
        log.error("Failed deep discovery for %d: %s", year, e)
    
    return found

def map_article_to_gp(title: str, url: str, events: list[dict]) -> tuple[str, str]:
    text = (title + " " + url).lower()
    for event in events:
        name = event["name"].lower()
        slug = event.get("slug", slugify(name))
        if name in text or slug.replace("-grand-prix", "") in text or slug in text:
            return event["name"], slug
    
    fallbacks = {
        "australia": "Australian Grand Prix", "bahrain": "Bahrain Grand Prix", "china": "Chinese Grand Prix",
        "baku": "Azerbaijan Grand Prix", "azerbaijan": "Azerbaijan Grand Prix", "spain": "Spanish Grand Prix",
        "monaco": "Monaco Grand Prix", "canada": "Canadian Grand Prix", "france": "French Grand Prix",
        "austria": "Austrian Grand Prix", "britain": "British Grand Prix", "silverstone": "British Grand Prix",
        "hungary": "Hungarian Grand Prix", "belgium": "Belgian Grand Prix", "spa": "Belgian Grand Prix",
        "italy": "Italian Grand Prix", "monza": "Italian Grand Prix", "singapore": "Singapore Grand Prix",
        "russia": "Russian Grand Prix", "japan": "Japanese Grand Prix", "suzuka": "Japanese Grand Prix",
        "mexico": "Mexican Grand Prix", "usa": "United States Grand Prix", "brazil": "Brazilian Grand Prix",
        "abu dhabi": "Abu Dhabi Grand Prix", "eifel": "Eifel Grand Prix", "imola": "Emilia Romagna Grand Prix",
        "tuscan": "Tuscan Grand Prix", "styrian": "Styrian Grand Prix", "portugal": "Portuguese Grand Prix",
        "sakhir": "Sakhir Grand Prix", "turkey": "Turkish Grand Prix", "qatar": "Qatar Grand Prix",
        "saudi": "Saudi Arabian Grand Prix", "miami": "Miami Grand Prix", "las vegas": "Las Vegas Grand Prix"
    }
    for key, canonical in fallbacks.items():
        if key in text: return canonical, slugify(canonical)
    return "Unknown Grand Prix", "unknown-gp"

# ---------------------------------------------------------------------------
# Discovery & Search
# ---------------------------------------------------------------------------

async def get_discovery_events(session: AsyncSession, year: int, discovery_path: Path) -> list[dict]:
    events = []
    if discovery_path.exists():
        try:
            data = json.loads(discovery_path.read_text(encoding="utf-8"))
            events = [e for e in data.get("events", []) if e.get("year") == year]
        except Exception: pass
    if not events and year == 2018:
        try:
            resp = await session.get(ARCHIVE_URL_2018)
            soup = BeautifulSoup(resp.text, "html.parser")
            for table in soup.find_all("table", class_="views-table"):
                caption = table.find("caption")
                if not caption: continue
                link = caption.find("a")
                if not link: continue
                name = link.get_text(separator=" ", strip=True).split("-")[0].strip()
                events.append({"name": name, "slug": slugify(name), "year": 2018})
        except Exception: pass
    return events

# ---------------------------------------------------------------------------
# Transcript Parsing
# ---------------------------------------------------------------------------

def clean_transcript_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # Modern FIA news layout
    content = soup.find(class_="node-article") or \
              soup.find(class_="content-body") or \
              soup.find(class_="field-items") or \
              soup.find(class_="field-item even") or \
              soup.find(class_="node__content") or \
              soup.find(class_="field-name-body") or \
              soup.find(class_="description") or \
              soup.find(class_="content")
              
    # Density-based fallback for old pages (find container with most direct text/p children)
    if not (content and content.get_text(strip=True)):
        divs = soup.find_all("div")
        best_div = None
        max_p = 0
        for d in divs:
            # Skip common sidebar/boilerplate containers
            classes = d.get("class", [])
            if any(c in classes for c in ["sidebar", "latest-news", "footer", "header", "menu"]):
                continue
            p_count = len(d.find_all("p", recursive=False))
            if p_count > max_p:
                max_p = p_count
                best_div = d
        if max_p > 2: # heuristic to avoid small text snippets
            content = best_div

    if not content or not content.get_text(strip=True): return ""

    # Before extracting text, apply bolding to <strong> tags in-place
    for strong in content.find_all(["strong", "b"]):
        name = strong.get_text(strip=True)
        if name and name.isupper() and (name.endswith(":") or len(name) < 35):
            strong.replace_with(f"**{name}**")

    # Use separator to preserve line breaks from <p>, <div>, <br>, etc.
    raw_text = content.get_text(separator="\n", strip=True)
    
    lines = []
    for line in raw_text.split("\n"):
        text = line.strip()
        if text:
            # Re-apply bolding for Q/A if not already caught
            text = re.sub(r"^(Q:)", r"**\1**", text)
            text = re.sub(r"^(A:)", r"**\1**", text)
            lines.append(text)
            
    return "\n\n".join(lines)

async def fetch_transcript(session: AsyncSession, url: str, dest: Path) -> bool:
    try:
        if dest.exists() and dest.stat().st_size > 500: return True
        resp = await session.get(url, timeout=30)
        if resp.status_code == 404: return False
        resp.raise_for_status()
        
        if url.endswith(".pdf") or "application/pdf" in resp.headers.get("Content-Type", ""):
            md_content = await extract_text_from_pdf(resp.content)
        else:
            md_content = clean_transcript_html(resp.text)
            
        if not md_content or len(md_content) < 500: return False
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

async def scrape_year(session: AsyncSession, year: int, output_dir: Path, manifest: dict, discovery_path: Path, transcript_cache_path: Path, aggressive: bool = False, super_aggressive: bool = False):
    """Scrape transcripts for a given year with optional discovery cache."""
    
    # Check if we have cached discovery results for this year
    cached_data = load_transcript_discovery_cache(transcript_cache_path)
    use_cache = False
    
    if cached_data and cached_data.get("year") == year:
        if aggressive or super_aggressive:
            # Check if cache matches our discovery mode
            cache_mode = cached_data.get("mode", "standard")
            current_mode = "super_aggressive" if super_aggressive else ("aggressive" if aggressive else "standard")
            if cache_mode == current_mode:
                use_cache = True
                log.info("Using cached discovery for year %d (mode: %s)", year, current_mode)
    
    if use_cache:
        events = cached_data.get("events", [])
        hub_articles = cached_data.get("hub_articles", [])
        timing_pdfs = cached_data.get("timing_pdfs", [])
        deep_results = cached_data.get("deep_results", [])
    else:
        # Perform fresh discovery
        events = await get_discovery_events(session, year, discovery_path)
        hub_articles = []
        timing_pdfs = []
        deep_results = []
        
        if aggressive or super_aggressive:
            hub_articles = await discover_from_hubs(session, year)
            # Sanity check for suspiciously large results
            if len(hub_articles) > 1000:
                log.warning("Suspiciously large hub discovery result (%d articles), possible scraping error", len(hub_articles))
        
        if super_aggressive:
            timing_pdfs = await discover_pdfs_from_timing(session, year, events)
            if len(timing_pdfs) > 500:
                log.warning("Suspiciously large PDF discovery result (%d PDFs), possible scraping error", len(timing_pdfs))
            if year in [2018, 2019, 2020, 2021]:
                deep_results = await discover_from_season_event_pages(session, year)
                if len(deep_results) > 1000:
                    log.warning("Suspiciously large deep discovery result (%d results), possible scraping error", len(deep_results))
        
        # Save discovery cache
        cache_mode = "super_aggressive" if super_aggressive else ("aggressive" if aggressive else "standard")
        save_transcript_discovery_cache(transcript_cache_path, {
            "year": year,
            "mode": cache_mode,
            "events": events,
            "hub_articles": hub_articles,
            "timing_pdfs": timing_pdfs,
            "deep_results": deep_results,
        })
    
    total_added = 0
    day_types = {"thursday": "thursday", "friday": "friday", "post-qualifying": "saturday", "post-race": "sunday"}
    local_tasks, local_meta = [], []

    # 1. Standard Hypothesize
    for event in events:
        gp_name = event["name"]
        gp_slug = event.get("slug", slugify(gp_name))
        slug_variants = [gp_slug]
        if "grand-prix" in gp_slug: slug_variants.append(gp_slug.replace("-grand-prix", "-gp"))
        for variant in slug_variants:
            for fia_type, local_day in day_types.items():
                url = urljoin(NEWS_URL_BASE, f"f1-{year}-{variant}-{fia_type}-press-conference-transcript")
                dest = output_dir / str(year) / gp_slug / "transcripts" / f"{local_day}.md"
                if url in manifest and dest.exists(): continue
                local_tasks.append(fetch_transcript(session, url, dest))
                local_meta.append({"url": url, "event": gp_name, "year": year, "title": f"{local_day.capitalize()} Transcript", "path": str(dest)})

    # 2. Aggressive Hubs
    if hub_articles:
        for i, art in enumerate(hub_articles):
            url = art["url"]
            if url in manifest: continue
            gp_name, gp_slug = map_article_to_gp(art["title"], url, events)
            day_key = "extra"; u_lower = url.lower() + " " + art["title"].lower()
            if "thursday" in u_lower: day_key = "thursday"
            elif "friday" in u_lower: day_key = "friday"
            elif "saturday" in u_lower or "qualifying" in u_lower: day_key = "saturday"
            elif "sunday" in u_lower or "race" in u_lower: day_key = "sunday"
            dest = output_dir / str(year) / gp_slug / "transcripts" / f"{day_key}_agg_{i}.md"
            local_tasks.append(fetch_transcript(session, url, dest))
            local_meta.append({"url": url, "event": gp_name, "year": year, "title": f"{day_key.capitalize()} (Aggressive)", "path": str(dest)})

    # 3. Super Aggressive PDF Timing Pages
    if timing_pdfs:
        for i, pdf in enumerate(timing_pdfs):
            url = pdf["url"]
            if url in manifest: continue
            gp_name, gp_slug = map_article_to_gp(pdf["title"], url, events)
            day_key = "extra"; u_lower = url.lower() + " " + pdf["title"].lower()
            if "thursday" in u_lower: day_key = "thursday"
            elif "friday" in u_lower: day_key = "friday"
            elif "saturday" in u_lower or "qualifying" in u_lower: day_key = "saturday"
            elif "sunday" in u_lower or "race" in u_lower: day_key = "sunday"
            dest = output_dir / str(year) / gp_slug / "transcripts" / f"{day_key}_pdf_{i}.md"
            local_tasks.append(fetch_transcript(session, url, dest))
            local_meta.append({"url": url, "event": gp_name, "year": year, "title": f"{day_key.capitalize()} (PDF)", "path": str(dest)})

    # 4. Deep Discovery
    if deep_results:
        for i, res in enumerate(deep_results):
            url = res["url"]
            if url in manifest: continue
            gp_name, gp_slug = map_article_to_gp(res["title"], url, events)
            day_key = "extra"; u_lower = url.lower() + " " + res["title"].lower()
            if "thursday" in u_lower: day_key = "thursday"
            elif "friday" in u_lower: day_key = "friday"
            elif "saturday" in u_lower or "qualifying" in u_lower: day_key = "saturday"
            elif "sunday" in u_lower or "race" in u_lower: day_key = "sunday"
            dest = output_dir / str(year) / gp_slug / "transcripts" / f"{day_key}_deep_{i}.md"
            local_tasks.append(fetch_transcript(session, url, dest))
            local_meta.append({"url": url, "event": gp_name, "year": year, "title": f"{day_key.capitalize()} (Deep)", "path": str(dest)})

    if local_tasks:
        log.info("Executing %d tasks for %d", len(local_tasks), year)
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
    aggressive = "--aggressive" in sys.argv
    super_aggressive = "--super-aggressive" in sys.argv
    output_dir = Path("documents")
    
    # Parse command line arguments
    if "--year" in sys.argv:
        idx = sys.argv.index("--year")
        try:
            years = [int(sys.argv[idx + 1])]
        except (IndexError, ValueError):
            log.error("Invalid --year argument. Usage: --year YYYY")
            return 0
    elif "--all-historical" in sys.argv:
        years = [2022, 2021, 2020, 2019, 2018]
    
    if "--output-dir" in sys.argv:
        idx = sys.argv.index("--output-dir")
        try:
            output_dir = Path(sys.argv[idx + 1])
        except IndexError:
            log.error("Invalid --output-dir argument. Usage: --output-dir PATH")
            return 0
    
    if "--help" in sys.argv or "-h" in sys.argv:
        print("""
FIA F1 Transcript Scraper

Usage: python transcript_scraper.py [OPTIONS]

Options:
  --year YYYY              Scrape transcripts for a specific year (default: 2026)
  --all-historical         Scrape years 2018-2022
  --output-dir PATH        Output directory (default: documents)
  --aggressive             Enable aggressive discovery (hub pages)
  --super-aggressive       Enable super aggressive discovery (hub + timing + deep)
  -h, --help              Show this help message

Examples:
  python transcript_scraper.py --year 2025 --super-aggressive
  python transcript_scraper.py --all-historical --aggressive
        """)
        return 0
    
    manifest_path = output_dir / "manifest.json"
    discovery_path = output_dir / "discovery_cache.json"
    transcript_cache_path = output_dir / "transcript_discovery_cache.json"
    
    manifest = load_manifest(manifest_path)
    total_added = 0
    
    async with AsyncSession(impersonate="chrome120") as session:
        for year in years:
            log.info("--- Starting Season %d ---", year)
            count = await scrape_year(
                session, 
                year, 
                output_dir, 
                manifest, 
                discovery_path, 
                transcript_cache_path,
                aggressive=aggressive, 
                super_aggressive=super_aggressive
            )
            if count > 0:
                save_manifest(manifest_path, manifest)
                log.info("Season %d: Total Added %d transcripts.", year, count)
            total_added += count
    
    log.info("=== Transcript scraping complete: %d total transcripts added ===", total_added)
    
    # Write output for GitHub Actions
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"new_transcripts={total_added}\n")
    
    return total_added

if __name__ == "__main__":
    asyncio.run(main())
