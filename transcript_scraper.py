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

from config import (
    BASE_URL, NEWS_URL_BASE, ARCHIVE_URL_2018, MAX_FETCH_CONCURRENT
)
from shared_utils import (
    slugify, load_manifest_with_lock, save_manifest_with_lock,
    load_discovery_cache, save_discovery_cache, validate_cache_structure
)

# ----------------------------------------------------------------------------
# Transcript validation constants
# ----------------------------------------------------------------------------
# `clean_transcript_html` and `fetch_transcript` cooperate to prevent the FIA
# "Latest News" listing from ever being persisted as a transcript. FIA returns
# a generic landing page (HTTP 200, <title>News | FIA</title>) for not-yet-
# published transcript URLs, and without these guards the landing page's
# news-list div was being saved as the day's transcript.

# Container classes that ship with FIA "Latest News" / sidebar / listing
# regions. They must NEVER be selected as the article body, neither by the
# candidate chain nor by the densest-<p> fallback.
SIDEBAR_CLASS_DENYLIST = frozenset({
    # News listing / landing-page widgets
    "news-list", "news-list-content", "news-title", "news-desc",
    "news-date", "news-image", "news-champ", "news-competition",
    "news-competitionlabel",
    # Drupal-style view / listing wrappers
    "list-view", "list-item",
    "view-fia-press-conference-transcripts",
    "view-content",
    # Related-news / "more like this" modules
    "related-news", "pane-related-news",
    "pane-related-news-related-news-pane",
    # Generic boilerplate
    "latest-news", "latest-news-block",
    "sidebar", "footer", "header", "menu", "breadcrumb",
})

# Real press conferences are 30k+ characters after cleaning; news-listing
# contamination is typically <700 chars. 2000 is a comfortable lower bound
# that, combined with the event-name and press-conference marker checks
# below, reliably rejects sidebar noise across every GP and every year.
MIN_TRANSCRIPT_CHARS = 2000

# ----------------------------------------------------------------------------
# Universal inference helpers (no per-GP hardcoding)
# ----------------------------------------------------------------------------
# These helpers turn the canonical FIA URL pattern
#   /news/f1-{year}-{event-slug}-{day}-press-conference-transcript
# into:
#   - a human-readable event name (with colloquial-alias resolution)
#   - the local file day-key (thursday / friday / saturday / sunday / extra)
# Every input that FIA actually publishes is supported without per-GP tables.

# A small, well-documented map of colloquial FIA URL slugs whose body uses
# the canonical GP name but whose URL slug does not. Every entry below has
# been observed in real FIA transcript URLs (e.g. `imola` for the Emilia
# Romagna GP). Keeping this list small + documented makes it easy to review
# when a new colloquial appears.
_SLUG_ALIASES: dict[str, str] = {
    "imola":          "Emilia Romagna Grand Prix",
    "mugello":        "Tuscan Grand Prix",
    "monza":          "Italian Grand Prix",
    "suzuka":         "Japanese Grand Prix",
    "spa":            "Belgian Grand Prix",
    "silverstone":    "British Grand Prix",
    "great-britain":  "British Grand Prix",
    "sao-paulo":      "São Paulo Grand Prix",
    "interlagos":     "São Paulo Grand Prix",
    "mexico-city":    "Mexico City Grand Prix",
    "madrid":         "Madrid Grand Prix",
    "vegas":          "Las Vegas Grand Prix",
    "portimao":       "Portuguese Grand Prix",
    "sochi":          "Russian Grand Prix",
}


# Order in the tuples below matters: when matching a URL day-token we
# prefer the LONGEST known prefix so `post-sprint-qualifying` wins over
# `sprint` and `team-principals` wins over `principal`. Each pair maps a
# recognized URL token to the local file key.
_DAY_TOKEN_TO_KEY: tuple[tuple[str, str], ...] = (
    ("post-sprint-qualifying", "saturday"),
    ("post-sprint",            "saturday"),
    ("post-qualifying",        "saturday"),
    ("team-principals",        "friday"),
    ("team-principal",         "friday"),
    ("thursday",               "thursday"),
    ("friday",                 "friday"),
    ("saturday",               "saturday"),
    ("sunday",                 "sunday"),
    ("post-race",              "sunday"),
)
# Pre-sorted longest-first for O(n) lookups.
_DAY_TOKEN_ORDERED: tuple[tuple[str, str], ...] = tuple(
    sorted(_DAY_TOKEN_TO_KEY, key=lambda kv: -len(kv[0]))
)


# Day-token alternation used as group 3 in TRANSCRIPT_URL_RE. Anchoring
# group 3 to a known day-token is required — otherwise the regex's
# non-greedy group 2 collapses arbitrarily and `_url_event_slug` returns a
# truncated slug for canonical URLs (e.g. ``austrian`` instead of
# ``austrian-grand-prix`` for ``.../f1-2026-austrian-grand-prix-thursday-...``),
# which would later route hub-discovered files to the wrong GP folder.
_DAY_TOKEN_ALT: str = "|".join(re.escape(t) for t, _ in _DAY_TOKEN_TO_KEY)


# Single regex that captures (year, event-slug, day-token) for canonical
# FIA transcript URLs. Group 2 is non-greedy so the event slug can be any
# length from ``austrian-gp`` to ``mexico-city-grand-prix``, but group 3 is
# anchored to a known day-token alternation so it expands just far enough
# to match a real day token — and never captures into ``-press-conference``.
TRANSCRIPT_URL_RE = re.compile(
    rf"/f1-(\d{{4}})-([a-z0-9-]+?)-({_DAY_TOKEN_ALT})-press-conference(?:-\w+)?/?$",
    re.IGNORECASE,
)


def _canonical_name_from_slug(slug: str) -> str:
    """Derive a human-readable GP name from an FIA URL slug.

    Universal — handles ``austrian-grand-prix``, ``austrian-gp``,
    ``mexico-city-grand-prix``, raw ``imola``, and ordinal-bearing slugs
    like ``70th-anniversary`` without mangling ``70th`` into ``70Th``.
    """
    if not slug:
        return ""
    if slug in _SLUG_ALIASES:
        return _SLUG_ALIASES[slug]
    # Strip the GP suffix if present, remembering whether we stripped one.
    gp_suffix = ""
    stripped = slug.lower()
    for suffix in ("-grand-prix", "-gp"):
        if stripped.endswith(suffix):
            stripped = stripped[: -len(suffix)]
            gp_suffix = " Grand Prix"
            break
    # Title-case each dash-token; leave pure-digit/ordinal tokens alone so
    # ``70th`` stays ``70th`` (not ``70Th``).
    titled: list[str] = []
    for tok in stripped.split("-"):
        if not tok:
            continue
        if tok.isdigit() or any(ch.isdigit() for ch in tok):
            # ordinals like "70th" — leave as-is
            titled.append(tok)
        else:
            titled.append(tok[:1].upper() + tok[1:].lower())
    return " ".join(titled) + gp_suffix


def _url_event_slug(url: str) -> str | None:
    """Extract the ``{event-slug}`` from a canonical transcript URL.

    Returns ``None`` when the URL doesn't match the canonical pattern (e.g.
    PDF, archived HTML page, sitemap entry).
    """
    m = TRANSCRIPT_URL_RE.search(url)
    if not m:
        return None
    return m.group(2).lower()


def _url_day_key(url: str) -> str:
    """Map a canonical transcript URL onto one of
    ``thursday``/``friday``/``saturday``/``sunday``/``extra``.

    Longest-prefix-wins against ``_DAY_TOKEN_ORDERED`` so multi-word tokens
    like ``post-sprint-qualifying`` match before any sub-token.
    """
    m = TRANSCRIPT_URL_RE.search(url)
    if not m:
        return "extra"
    token = m.group(3).lower()
    for day_token, key in _DAY_TOKEN_ORDERED:
        if token == day_token or token.startswith(day_token + "-"):
            return key
    return "extra"


def _url_event_slug_and_day(url: str) -> tuple[str | None, str]:
    """Return ``(event_slug, day_key)`` extracted from a canonical URL.

    Both are ``None`` / ``"extra"`` respectively when the URL does not match.
    """
    m = TRANSCRIPT_URL_RE.search(url)
    if not m:
        return (None, "extra")
    return (m.group(2).lower(), _url_day_key(url))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def load_manifest(path: Path) -> dict:
    """Legacy wrapper for backward compatibility."""
    return load_manifest_with_lock(path)


def save_manifest(path: Path, manifest: dict) -> None:
    """Legacy wrapper for backward compatibility."""
    save_manifest_with_lock(path, manifest)

# ---------------------------------------------------------------------------
# Discovery cache
# ---------------------------------------------------------------------------

def load_transcript_discovery_cache(path: Path, force_refresh: bool = False) -> dict | None:
    """Load cached transcript discovery results if still valid."""
    cache_data = load_discovery_cache(path, "transcript", force_refresh)
    if cache_data is None:
        return None
    
    # Validate required structure for transcript cache
    required_keys = ["year", "mode", "events", "hub_articles", "timing_pdfs", "deep_results"]
    if not validate_cache_structure(cache_data, required_keys):
        return None
    
    return cache_data


def save_transcript_discovery_cache(path: Path, data: dict) -> None:
    """Save transcript discovery results with timestamp."""
    save_discovery_cache(path, data, "transcript")

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


def _archive_url_for_year(year: int) -> str | None:
    """Universal per-year archive URL, read from ``classification/{year}/classifications.json``.

    Returns ``None`` if the classification file is absent or lacks an
    ``archive_url``. This is the canonical universal source for any year's
    archive landing page, with no per-year hardcoding in this script.
    """
    cls_path = Path("classification") / str(year) / "classifications.json"
    if not cls_path.exists():
        return None
    try:
        data = json.loads(cls_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return (data.get("archive_url") or "").strip() or None


async def discover_from_sitemap(session: AsyncSession, year: int) -> list[dict]:
    """Crawl FIA's sitemap (recursively into ``<sitemapindex>`` children) and
    collect every canonical ``press-conference-transcript`` URL for ``year``.

    Universal — no per-year URL patterns. Works against any FIA sitemap
    shape (Drupal-style ``<sitemapindex>`` with a child ``<urlset>`` per
    paginated chunk, or a single ``<urlset>``).
    """
    log.info("Discovering transcripts from FIA sitemap for %d", year)
    found: list[dict] = []
    needle_press_conf = "press-conference"
    year_marker = f"f1-{year}-"
    seen_urls: set[str] = set()

    async def crawl(xml_url: str, depth: int) -> None:
        if depth > 3 or xml_url in seen_urls:
            return
        seen_urls.add(xml_url)
        try:
            resp = await session.get(xml_url, timeout=30)
            if resp.status_code != 200:
                return
            text = resp.text or ""
        except Exception:
            return
        # If it's a sitemap index, recurse into each child sitemap.
        if "<sitemapindex" in text.lower():
            for loc in re.findall(r"<loc>([^<]+)</loc>", text, flags=re.IGNORECASE):
                await crawl(loc.strip(), depth + 1)
            return
        # Otherwise treat as a urlset; extract every <loc> whose URL looks
        # like our year + transcript pattern.
        for loc in re.findall(r"<loc>([^<]+)</loc>", text, flags=re.IGNORECASE):
            u = loc.strip()
            if needle_press_conf not in u.lower():
                continue
            if year_marker not in u.lower():
                continue
            if u in seen_urls:
                continue
            seen_urls.add(u)
            title = u.rsplit("/", 1)[-1].replace("-", " ").strip().title()
            found.append({"title": title, "url": u})

    await crawl(f"{BASE_URL}/sitemap.xml", depth=0)
    log.info("Sitemap crawl for %d found %d transcript URLs", year, len(found))
    return found


async def discover_pdfs_from_timing(session: AsyncSession, year: int, events: list[dict]) -> list[dict]:
    """Discover transcript PDFs linked from Event & Timing Information pages.

    Universal across all years: locates a year-specific archive landing page
    via ``_archive_url_for_year(year)`` (which reads the per-year
    ``classification/{year}/classifications.json``), then crawls all timing
    pages it links to and collects ``.pdf`` links whose anchor text contains
    "transcript". Falls back to ``ARCHIVE_URL_2018`` for legacy years without
    a classification entry.
    """
    log.info("Discovering transcript PDFs for %d", year)
    found_pdfs: list[dict] = []

    archive_url = _archive_url_for_year(year)
    if not archive_url:
        log.info(
            "No archive_url for %d in classification/{year}/classifications.json; "
            "skipping PDF discovery for this year.",
            year,
        )
        return []
    timing_urls: set[str] = set()
    try:
        resp = await session.get(archive_url, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                # Catch-all for FIA timing pages regardless of small wording
                # variations across years (eventtiming-information,
                # event-timing-information, etc.).
                if "eventtiming-information" in href or "event-timing-information" in href:
                    timing_urls.add(urljoin(BASE_URL, a["href"]))
    except Exception:
        pass

    for t_url in timing_urls:
        try:
            resp = await session.get(t_url, timeout=30)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.lower().endswith(".pdf"):
                    continue
                title = a.get_text(strip=True).lower()
                if "transcript" in title:
                    found_pdfs.append({
                        "title": a.get_text(strip=True),
                        "url": urljoin(BASE_URL, href),
                    })
        except Exception:
            continue

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

# Tokens we hypothesize per GP per year. The URL is constructed by joining
# one of these with the GP slug, e.g.
#   f1-{year}-{slug}-thursday-press-conference-transcript
# Every token in this tuple is a known FIA day-variant, universal across all
# GPs and all years. The URL-derived ``_url_day_key`` collapses duplicates
# onto the same local file, so hypothesizing both ``saturday`` and
# ``post-qualifying`` produces one fetch per day instead of two.
HYPOTHESIZED_DAY_TOKENS: tuple[str, ...] = (
    "thursday",
    "friday",
    "saturday",                  # literal Saturday form (newer years)
    "sunday",                    # literal Sunday form (newer years)
    "post-qualifying",
    "post-sprint-qualifying",    # sprint weekends
    "post-sprint",               # sprint weekends
    "post-race",
)


def map_article_to_gp(title: str, url: str, events: list[dict]) -> tuple[str, str]:
    """Universal GP inference. Order of preference:

    1. Classified ``events`` list — matches its canonical name against the
       title+url text. Handles "URL is generic but title says Bahrain", and
       resolves colloquial slugs (``imola`` URL -> the ``Emilia Romagna``
       events-list entry, because the events-list contains the official name).
    2. Classified ``events`` list — matches its slug against the url text.
    3. URL-slug canonicalization (``_canonical_name_from_slug``). Covers
       any new GP added in the current year that hasn't been classified yet.
    4. (``"Unknown Grand Prix"``, ``"unknown-gp"``).

    No per-GP hardcoded tables: every GP comes from the classification
    pipeline (``get_discovery_events``) or from universal slug inference.
    """
    text = (title + " " + url).lower()
    # 1. Canonical event name substring match.
    for event in events:
        name_lc = event["name"].lower()
        if name_lc and name_lc in text:
            slug = event.get("slug", slugify(event["name"]))
            return event["name"], slug
    # 2. events-list slug substring match.
    for event in events:
        event_slug = event.get("slug", slugify(event["name"])).lower()
        if event_slug and event_slug in text:
            return event["name"], event_slug
    # 3. URL-slug canonicalization.
    url_slug = _url_event_slug(url)
    if url_slug:
        canonical = _canonical_name_from_slug(url_slug)
        # Prefer an existing event's slug if names match.
        for event in events:
            if event["name"].lower() == canonical.lower():
                slug = event.get("slug", slugify(event["name"]))
                return event["name"], slug
        return canonical, slugify(canonical)
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
    """Extract clean Markdown body text from an FIA press-conference page.

    Returns "" if the page is not actually a transcript article. FIA returns
    a generic "News | FIA" landing page (HTTP 200) for not-yet-published
    transcript URLs; that page's "Latest News" listing previously leaked
    through this cleaner as a transcript.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Title-based short-circuit. Real FIA transcript pages always include
    # "press conference" or "transcript" in their <title>; landing pages do
    # not. If neither phrase is present, refuse to extract anything.
    title_text = ""
    if soup.title and soup.title.string:
        title_text = soup.title.string.strip().lower()
    if "press conference" not in title_text and "transcript" not in title_text:
        return ""

    def _is_sidebar(tag) -> bool:
        classes = tag.get("class", []) or []
        return any(c in SIDEBAR_CLASS_DENYLIST for c in classes)

    # Modern FIA news layout — first non-sidebar match wins.
    content = None
    for cls in ("node-article", "content-body", "field-items",
                "field-item even", "node__content", "field-name-body",
                "description", "content"):
        el = soup.find(class_=cls)
        if el and not _is_sidebar(el):
            content = el
            break

    # Density-based fallback for old pages — only consider non-sidebar divs.
    if not (content and content.get_text(strip=True)):
        divs = soup.find_all("div")
        best_div = None
        max_p = 0
        for d in divs:
            if _is_sidebar(d):
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

# Bolded speaker line in a cleaned transcript: **NAME:**
_SPEAKER_LINE_RE = re.compile(r"\*\*[A-Z][A-Z\s.'-]{1,40}:\*\*")


_EXISTING_FILE_PEEK_BYTES = 4096


def _existing_file_is_valid_transcript(
    dest: Path, expected_event_name: str | None
) -> bool:
    """Cheap on-disk heuristic: does the file already look like a real transcript?

    Used to relax the previous "skip if file exists with size > 500" guard so
    bogus sidebar-contaminated files (e.g. the FIA "Latest News" snippet
    that ended up in the 2026 Austrian GP transcripts) can still be replaced
    with valid content on a future scrape, without churning every historical
    transcript on every run.

    We only peek at the first _EXISTING_FILE_PEEK_BYTES bytes — for a 30 kB
    transcript that's enough to confirm the event-name substring and a few
    speaker/Q&A markers; for a 569-byte bogus file there's nothing to peek.
    """
    if not dest.exists():
        return False
    try:
        with open(dest, "rb") as f:
            text = f.read(_EXISTING_FILE_PEEK_BYTES).decode(
                encoding="utf-8", errors="ignore"
            )
    except OSError:
        return False
    if len(text.strip()) < MIN_TRANSCRIPT_CHARS // 2:
        return False
    if expected_event_name and expected_event_name.lower() not in text.lower():
        return False
    # When no event name is supplied, fall back to a marker check so a long
    # but content-less file (e.g. a sidebar dump) cannot masquerade as valid.
    if not expected_event_name and not _has_press_conference_markers(text):
        return False
    return True


def _has_press_conference_markers(md_content: str) -> bool:
    """Detect that cleaned content looks like a Q&A transcript rather than a
    news listing / sidebar snippet.

    A real press-conference transcript has bolded NAME: speaker lines and/or
    Q:/A: markers. A news listing has neither.
    """
    if not md_content:
        return False
    speakers = len(_SPEAKER_LINE_RE.findall(md_content))
    qa_markers = (
        md_content.count("**Q:**")
        + md_content.count("**A:**")
        + len(re.findall(r"^(?:Q|A):", md_content, flags=re.MULTILINE))
    )
    return speakers >= 2 or qa_markers >= 1


async def fetch_transcript(
    session: AsyncSession,
    url: str,
    dest: Path,
    expected_event_name: str | None = None,
) -> bool:
    """Fetch and save a press-conference transcript page.

    Returns True only when the file ends up containing a real transcript for
    the given GP. Returns False on 404s, sidebar-only pages, body-mismatch,
    or any other reason not to write. If `dest` already contains a valid
    transcript, no fetch is performed.
    """
    try:
        # Skip the network only if we already have a valid transcript on disk.
        if _existing_file_is_valid_transcript(dest, expected_event_name):
            return True

        resp = await session.get(url, timeout=30)
        if resp.status_code == 404: return False
        resp.raise_for_status()

        if url.endswith(".pdf") or "application/pdf" in resp.headers.get("Content-Type", ""):
            md_content = await extract_text_from_pdf(resp.content)
        else:
            md_content = clean_transcript_html(resp.text)

        if not md_content:
            log.info("Skipping %s: empty content from %s", dest.name, url)
            return False

        # 1. Length gate — real press conferences are well above this.
        if len(md_content) < MIN_TRANSCRIPT_CHARS:
            log.info("Skipping %s: too short (%d chars)", dest.name, len(md_content))
            return False

        # 2. Event-name gate — GP-specific content only.
        if expected_event_name and expected_event_name.lower() not in md_content.lower():
            log.info("Skipping %s: body missing event keyword '%s'", dest.name, expected_event_name)
            return False

        # 3. Press-conference marker gate — speakers or Q:/A: structure.
        if not _has_press_conference_markers(md_content):
            log.info("Skipping %s: no Q&A / speaker markers", dest.name)
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

async def scrape_year(session: AsyncSession, year: int, output_dir: Path, manifest: dict, discovery_path: Path, transcript_cache_path: Path, aggressive: bool = False, super_aggressive: bool = False, force_refresh: bool = False):
    """Scrape transcripts for a given year with optional discovery cache."""
    
    # Check if we have cached discovery results for this year
    cached_data = load_transcript_discovery_cache(transcript_cache_path, force_refresh=force_refresh)
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
        sitemap_articles: list[dict] = []
        timing_pdfs = []
        deep_results = []

        if aggressive or super_aggressive:
            # Universal sitemap crawl first (works for any year); the hub
            # page is a soft supplement whose stale 404s we tolerate.
            sitemap_articles = await discover_from_sitemap(session, year)
            hub_articles = await discover_from_hubs(session, year)
            # Merge, deduping by URL.
            seen: set[str] = set()
            merged: list[dict] = []
            for art in sitemap_articles + hub_articles:
                if art["url"] in seen:
                    continue
                seen.add(art["url"])
                merged.append(art)
            hub_articles = merged
            if len(hub_articles) > 1000:
                log.warning(
                    "Suspiciously large hub discovery result (%d articles), "
                    "possible scraping error",
                    len(hub_articles),
                )

        if super_aggressive:
            timing_pdfs = await discover_pdfs_from_timing(session, year, events)
            if len(timing_pdfs) > 500:
                log.warning(
                    "Suspiciously large PDF discovery result (%d PDFs), "
                    "possible scraping error",
                    len(timing_pdfs),
                )
            # Deep season-page crawl is cheapest per-event; run for every
            # year, not just legacy ones. Universal because season page URL
            # is year-derived.
            deep_results = await discover_from_season_event_pages(session, year)
            if len(deep_results) > 1000:
                log.warning(
                    "Suspiciously large deep discovery result (%d results), "
                    "possible scraping error",
                    len(deep_results),
                )

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
    local_tasks, local_meta = [], []

    # 1. Standard Hypothesize — enumerate every known FIA day-variant URL
    # for every GP's slug (and `-gp` short variant). The local filename is
    # computed from the URL itself so duplicates (e.g. `saturday` and
    # `post-qualifying`) collapse onto the same file naturally.
    for event in events:
        gp_name = event["name"]
        gp_slug = event.get("slug", slugify(gp_name))
        slug_variants = [gp_slug]
        if "grand-prix" in gp_slug:
            slug_variants.append(gp_slug.replace("-grand-prix", "-gp"))
        for variant in slug_variants:
            for day_token in HYPOTHESIZED_DAY_TOKENS:
                url = urljoin(
                    NEWS_URL_BASE,
                    f"f1-{year}-{variant}-{day_token}-press-conference-transcript",
                )
                if url in manifest:
                    continue
                _, day_key = _url_event_slug_and_day(url)
                dest = output_dir / str(year) / gp_slug / "transcripts" / f"{day_key}.md"
                local_tasks.append(fetch_transcript(session, url, dest, gp_name))
                local_meta.append({
                    "url": url, "event": gp_name, "year": year,
                    "title": f"{day_key.capitalize()} Transcript",
                    "path": str(dest),
                })

    # 2. Aggressive Hubs — drive day-key from URL pattern
    if hub_articles:
        for i, art in enumerate(hub_articles):
            url = art["url"]
            if url in manifest:
                continue
            gp_name, gp_slug = map_article_to_gp(art["title"], url, events)
            _, day_key = _url_event_slug_and_day(url)
            dest = output_dir / str(year) / gp_slug / "transcripts" / f"{day_key}_agg_{i}.md"
            local_tasks.append(fetch_transcript(session, url, dest, gp_name))
            local_meta.append({
                "url": url, "event": gp_name, "year": year,
                "title": f"{day_key.capitalize()} (Aggressive)",
                "path": str(dest),
            })

    # 3. Super Aggressive PDF Timing Pages
    if timing_pdfs:
        for i, pdf in enumerate(timing_pdfs):
            url = pdf["url"]
            if url in manifest:
                continue
            gp_name, gp_slug = map_article_to_gp(pdf["title"], url, events)
            _, day_key = _url_event_slug_and_day(url)
            dest = output_dir / str(year) / gp_slug / "transcripts" / f"{day_key}_pdf_{i}.md"
            local_tasks.append(fetch_transcript(session, url, dest, gp_name))
            local_meta.append({
                "url": url, "event": gp_name, "year": year,
                "title": f"{day_key.capitalize()} (PDF)",
                "path": str(dest),
            })

    # 4. Deep Discovery
    if deep_results:
        for i, res in enumerate(deep_results):
            url = res["url"]
            if url in manifest:
                continue
            gp_name, gp_slug = map_article_to_gp(res["title"], url, events)
            _, day_key = _url_event_slug_and_day(url)
            dest = output_dir / str(year) / gp_slug / "transcripts" / f"{day_key}_deep_{i}.md"
            local_tasks.append(fetch_transcript(session, url, dest, gp_name))
            local_meta.append({
                "url": url, "event": gp_name, "year": year,
                "title": f"{day_key.capitalize()} (Deep)",
                "path": str(dest),
            })

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
    force_refresh = "--force-refresh" in sys.argv
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
  --force-refresh          Bypass discovery cache and force fresh crawl
  -h, --help              Show this help message

Examples:
  python transcript_scraper.py --year 2025 --super-aggressive
  python transcript_scraper.py --all-historical --aggressive
  python transcript_scraper.py --year 2026 --force-refresh
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
                super_aggressive=super_aggressive,
                force_refresh=force_refresh
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
