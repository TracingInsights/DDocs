import asyncio
import os
import json
import re
from urllib.parse import urljoin
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
import logging

# Configuration
BASE_URL = "https://www.fia.com"
ARCHIVE_URL = "https://www.fia.com/f1-archives?season=866"
# Workspace path - using absolute path to be safe
DOCS_DIR = "//wsl.localhost/Ubuntu/home/devcontainers/uGithub/DDocs/documents"
YEAR = 2018
MANIFEST_PATH = os.path.join(DOCS_DIR, "manifest.json")
MAX_CONCURRENT_DOWNLOADS = 5

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# --- Helpers ---

def slugify(text):
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    return text.strip("-")

def load_json(path, default=None):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {path}: {e}")
    return default or {}

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)

# --- Core Logic ---

async def fetch_2018_events_and_timing(session):
    logger.info(f"Fetching 2018 events from {ARCHIVE_URL}")
    try:
        resp = await session.get(ARCHIVE_URL, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch archive: {resp.status_code}")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        
        events = []
        # Each GP is in a <table>
        for table in soup.find_all("table", class_="views-table"):
            caption = table.find("caption")
            if not caption:
                continue
            
            link = caption.find("a")
            if not link:
                continue
            
            # Extract GP name
            full_text = link.get_text(separator=" ", strip=True)
            name = full_text.split("-")[0].strip()
            slug = slugify(name)
            
            # Find the "Event&Timing Information" link in the table body
            timing_url = None
            for row_link in table.find_all("a"):
                text = row_link.get_text(strip=True)
                if "Timing" in text and "Information" in text:
                    timing_url = urljoin(BASE_URL, row_link["href"])
                    break
            
            if timing_url:
                events.append({
                    "name": name,
                    "slug": slug,
                    "timing_url": timing_url
                })
            else:
                logger.warning(f"Could not find timing link for {name} on archive page")
        
        logger.info(f"Found {len(events)} events for 2018")
        return events
    except Exception as e:
        logger.error(f"Error discovering events: {e}")
        return []

async def extract_documents(session, timing_url):
    logger.info(f"Extracting documents from {timing_url}")
    try:
        resp = await session.get(timing_url, timeout=30)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        
        documents = []
        for link in soup.find_all("a", href=lambda h: h and h.endswith(".pdf")):
            url = urljoin(BASE_URL, link["href"])
            title = link.get_text(separator=" ", strip=True)
            
            # Parse doc number (e.g., "Doc 34")
            doc_match = re.search(r"doc[ument]?\s+(\d+)", title, re.IGNORECASE)
            if not doc_match:
                doc_match = re.search(r"doc_(\d+)", url, re.IGNORECASE)
            
            doc_number = int(doc_match.group(1)) if doc_match else 0
            
            # Clean title
            clean_title = title.replace("\n", " ").strip()
            if not clean_title or len(clean_title) < 5:
                clean_title = os.path.basename(url).replace(".pdf", "").replace("_", " ").title()

            documents.append({
                "title": clean_title,
                "url": url,
                "doc_number": doc_number,
                "filename": os.path.basename(url)
            })
            
        return documents
    except Exception as e:
        logger.error(f"Error extracting documents from {timing_url}: {e}")
        return []

async def download_pdf(session, semaphore, url, dest_path):
    async with semaphore:
        if os.path.exists(dest_path):
            return True
        
        logger.info(f"Downloading {os.path.basename(dest_path)}")
        try:
            r = await session.get(url, timeout=60)
            if r.status_code == 200:
                with open(dest_path, "wb") as f:
                    f.write(r.content)
                return True
            else:
                logger.error(f"Failed to download {url}: {r.status_code}")
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
        return False

async def main():
    async with AsyncSession(impersonate="chrome120") as session:
        events = await fetch_2018_events_and_timing(session)
        if not events:
            logger.error("No events discovered.")
            return

        manifest = load_json(MANIFEST_PATH)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        
        # 1. Discover all docs
        all_event_docs = []
        for event in events:
            docs = await extract_documents(session, event["timing_url"])
            if docs:
                event["documents"] = docs
                all_event_docs.append(event)
            else:
                logger.warning(f"No documents found for {event['name']} at {event['timing_url']}")
            
        # 2. Download and process
        download_tasks = []
        for event in all_event_docs:
            event_dir = os.path.join(DOCS_DIR, "2018", event["slug"])
            os.makedirs(event_dir, exist_ok=True)
            
            event_indices = []
            for doc in event["documents"]:
                url = doc["url"]
                filename = doc["filename"]
                dest_path = os.path.join(event_dir, filename)
                
                # Update manifest
                rel_path = f"documents\\2018\\{event['slug']}\\{filename}"
                manifest[url] = {
                    "doc_number": doc["doc_number"],
                    "event": event["name"],
                    "path": rel_path,
                    "published_time": "", # Not available in 2018 archives
                    "source": "decision",
                    "title": doc["title"],
                    "year": YEAR
                }
                
                download_tasks.append(download_pdf(session, semaphore, url, dest_path))
                
                # For event index.json
                idx_entry = {
                    "f": filename,
                    "t": doc["title"]
                }
                if doc["doc_number"] > 0:
                    idx_entry["n"] = doc["doc_number"]
                event_indices.append(idx_entry)
            
            # Save event index.json
            event_indices.sort(key=lambda x: (x.get("n", 999), x["t"]))
            save_json(os.path.join(event_dir, "index.json"), event_indices)
            
        # Execute downloads
        if download_tasks:
            logger.info(f"Starting {len(download_tasks)} downloads")
            await asyncio.gather(*download_tasks)
            
        # 3. Save common indices
        save_json(MANIFEST_PATH, manifest)
        
        # Save 2018 index
        year_index = [{"s": e["slug"], "n": e["name"]} for e in all_event_docs]
        year_index.sort(key=lambda x: x["s"])
        save_json(os.path.join(DOCS_DIR, "2018", "index.json"), year_index)
        
        logger.info("Scraping complete!")

if __name__ == "__main__":
    asyncio.run(main())
