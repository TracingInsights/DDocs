# FIA Document Extraction — Design Decisions & Implementation Plan

> **Purpose**: Extract structured data from ~9,715 FIA F1 decision PDFs so they can power a "Chat with FIA Docs" RAG system.
> This document covers the extraction layer only — chunking, embeddings, vector DB, and the chat API are covered in [PLAN.md](PLAN.md).

---

## Table of Contents

1. [Design Decisions](#1-design-decisions)
2. [Architecture Overview](#2-architecture-overview)
3. [Document Landscape](#3-document-landscape)
4. [Output Format](#4-output-format)
5. [Extraction Library](#5-extraction-library)
6. [Metadata Schema](#6-metadata-schema)
7. [Image Handling](#7-image-handling)
8. [Tracking & Deduplication](#8-tracking--deduplication)
9. [Folder Structure](#9-folder-structure)
10. [Extraction Script (`extract.py`)](#10-extraction-script-extractpy)
11. [GitHub Actions Integration](#11-github-actions-integration)
12. [Initial Backfill Strategy](#12-initial-backfill-strategy)
13. [Future: Category-Specific Parsers](#13-future-category-specific-parsers)
14. [Open Questions](#14-open-questions)

---

## 1. Design Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | **Output format**: Markdown (`.md`) + JSON sidecar (`.json`) per PDF | Clean separation of searchable content and structured metadata; `.md` is RAG-ready and human-readable |
| 2 | **Extract maximum structured fields** via regex | Maximize accuracy for downstream search/filter; FIA docs have consistent machine-generated formatting |
| 3 | **Universal parser first, category-specific parsers later** | Ship fast with coverage of all 9,715 docs; layer richer extraction for high-value categories incrementally |
| 4 | **Kreuzberg** as the extraction library | Rust core (fast), MIT license, text + table + image extraction, batch processing, async, GH Actions friendly |
| 5 | **GH Actions = extraction + storage only** | No chunking or embedding in CI; those strategies belong in the downstream RAG app and will evolve independently |
| 6 | **Output folder**: `extracted/` | Clean break from the old `processed_documents/` HTML output |
| 7 | **Incremental extraction** with one-time local backfill | GH Actions processes only new PDFs per run; existing ~9,715 docs are backfilled locally once |
| 8 | **Single `extracted/manifest.json`** with source content hash | Tracks extraction status, detects modified source PDFs, prevents duplicate work |
| 9 | **Extraction step added to existing `scrape.yml`** | Single atomic commit: scrape → extract → commit; no race conditions between workflows |
| 10 | **Local backfill** for the existing corpus | No GH Actions timeout constraints; fastest path for ~9,715 PDFs |
| 11 | **Commit images to git** | Self-contained `extracted/` folder; images are available for future embedding/vision model pipelines |
| 12 | **Skip FIA header/logo images** (zero RAG value) | Reduces storage by ~80%; the same banner appears on virtually every document |
| 13 | **Position + size heuristic** for header detection | Top ~15% of page height + >70% page width → skip; works across all pages, portrait and landscape |
| 14 | **Regex/pattern parsing** for metadata extraction | Docs are machine-generated with consistent key-value format; no API cost, fast, works offline |
| 15 | **New standalone `extract.py`** | Clean separation from the old HTML-focused `fia_pdf_processor.py` |
| 16 | **Install Kreuzberg via `uv`/`pip`** with system deps (`apt`) | Consistent with existing workflow; Tesseract + Pandoc available via `apt-get` on ubuntu runners |
| 17 | **PDFs only** — transcripts deferred | Transcripts are already clean Markdown in `documents/`; they can be ingested directly by the RAG pipeline later |

### Assumptions

- FIA PDFs are **digitally authored** (searchable text), not scans — OCR is a fallback, not the primary path.
- The FIA header/logo banner is always positioned at the **top of the page spanning full width**, consistent across 2015–2026.
- Regex is sufficient for the **consistent key-value format** in FIA document headers and decision bodies.
- Repo size increase from non-header images is acceptable (estimated <100MB after header filtering).
- `extracted/manifest.json` at ~1–2MB for 9,715 entries is trivially small for git.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                   GitHub Actions (scrape.yml)            │
│                                                         │
│  ┌──────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │ scraper  │───▶│ extract.py   │───▶│  git commit   │  │
│  │ .py      │    │ (kreuzberg)  │    │  & push       │  │
│  └──────────┘    └──────┬───────┘    └───────────────┘  │
│                         │                               │
│                         ▼                               │
│              ┌─────────────────────┐                    │
│              │  extracted/         │                    │
│              │  ├── manifest.json  │                    │
│              │  ├── 2024/          │                    │
│              │  │   └── event/     │                    │
│              │  │       ├── *.md   │                    │
│              │  │       ├── *.json │                    │
│              │  │       └── images/│                    │
│              │  └── ...            │                    │
│              └─────────────────────┘                    │
└─────────────────────────────────────────────────────────┘
         ▲                                    │
         │                                    ▼
  documents/                          Future RAG Pipeline
  ├── 2024/event/*.pdf               (PLAN.md Layers 1-9)
  ├── manifest.json
  └── ...
```

**Data flow:**
1. `scraper.py` / `transcript_scraper.py` fetch new PDFs into `documents/`.
2. `extract.py` reads `extracted/manifest.json`, finds unextracted PDFs, processes them via Kreuzberg.
3. Outputs `.md` + `.json` + images into `extracted/year/event/`.
4. Updates `extracted/manifest.json`.
5. Everything is committed and pushed in a single atomic commit.

---

## 3. Document Landscape

### Corpus Statistics

| Metric | Value |
|--------|-------|
| Total PDFs | **9,715** |
| Total size (raw) | **5.2 GB** |
| Year range | 2015–2026 |
| Transcripts (`.md`, excluded) | 635 |
| Events per year | 20–24 |

### PDFs per Year

| Year | PDFs |
|------|------|
| 2015 | 1 |
| 2018 | 1,364 |
| 2019 | 952 |
| 2020 | 836 |
| 2021 | 1,046 |
| 2022 | 1,207 |
| 2023 | 1,274 |
| 2024 | 1,376 |
| 2025 | 1,366 |
| 2026 | 293 |

### Document Categories (Top 30)

| Category | Count | Structure Type |
|----------|-------|----------------|
| Summons (car-specific) | 756 | Semi-structured: header + hearing reason |
| Offence (car-specific) | 584 | Semi-structured: header + key-value decision fields |
| Infringement (car-specific) | 551 | Semi-structured: header + key-value decision fields |
| Decision (car-specific) | 383 | Semi-structured: header + key-value decision fields |
| Race Director's Event Notes | 338 | Multi-section numbered prose (6+ pages) |
| Final Starting Grid | 155 | Tabular: position, driver, team, time |
| Provisional Starting Grid | 153 | Tabular: position, driver, team, time |
| Classifications (P1/P2/P3/Qual/Race) | ~900+ | Tabular: position, driver, team, time, laps, gap |
| Championship Points | 152 | Tabular: driver/team standings |
| Scrutineering Reports | ~400+ | Long prose: checks performed per car |
| Entry List | 151 | Tabular: all drivers/teams/cars |
| PU Elements Used | 147 | Table: driver × component usage counts |
| Deleted Lap Times | ~250+ | Tabular: car, driver, turn, lap time |
| Parc Fermé / Parts Changed | ~160+ | Lists of parts replaced |
| Circuit Map | 61 | Primarily an image (map) |
| Curfew | 63 | Short structured text |
| Misc (procedures, notes, etc.) | ~500+ | Various |

### Document Format Observations

All FIA documents share a **common header structure** on page 1:

```
[FIA HEADER IMAGE — full-width banner]

{YEAR} {EVENT NAME}
{Event Dates}

From    {sender}                Document    {number}
To      {recipient}             Date        {date}
                                Time        {time}
```

**Stewards decisions / offences / infringements** add structured body fields:

```
No / Driver     {car#} - {driver name}
Competitor      {team name}
Time            {incident time}
Session         {FP1/FP2/FP3/Qualifying/Sprint/Race}

Fact            {description of incident}
Offence         {regulation reference}
Decision        {penalty or "No further action"}
Reason          {explanation}

[Steward names]
```

**Summons** are lighter (no Decision/Reason, just a hearing time and reason).

**Tabular documents** (grids, classifications, PU elements) have the same header but the body is a data table.

**Race Director notes** have the header plus numbered multi-section prose.

---

## 4. Output Format

Each PDF produces **two or three outputs**:

### 4.1 Body Text — `{slug}.md`

Clean extracted text in Markdown format. Tables are rendered as Markdown tables. No CSS, no HTML, no layout markup. This is the primary content for RAG ingestion.

Example for a decision document:
```markdown
# Decision - Car 22 - Alleged Unsafe Release

2023 AUSTRIAN GRAND PRIX
30 June - 02 July 2023

The Stewards, having received a report from the Race Director, have considered
the following matter and determine the following:

**No / Driver:** 22 - Yuki Tsunoda
**Competitor:** Scuderia AlphaTauri
**Time:** 15:32
**Session:** Race

**Fact:** Alleged failure to serve a penalty during a pit stop.
**Infringement:** Alleged breach of Article 54.4(c) of the FIA Formula One
Sporting Regulations.
**Decision:** No further action.
**Reason:** Video evidence from the FIA pit box overhead camera shows the jack
moving towards the front of the car as the car stops...

Competitors are reminded that they have the right to appeal...

**The Stewards:**
Garry Connelly, Mathieu Remmerie, Enrique Bernoldi, Walter Jobst
```

### 4.2 Structured Metadata — `{slug}.json`

All extractable structured fields. The universal parser extracts the common header; category-specific parsers (future) will add richer fields.

```json
{
  "source_pdf": "documents/2023/austrian-grand-prix/decision-car-22-alleged-unsafe-release.pdf",
  "source_hash": "sha256:a1b2c3d4e5f6...",
  "extracted_at": "2025-05-19T14:30:00Z",
  "year": 2023,
  "event_slug": "austrian-grand-prix",
  "event_name": "Austrian Grand Prix",
  "doc_type": "decision",
  "doc_number": "65",
  "date": "2023-07-02",
  "time": "16:22",
  "from": "The Stewards",
  "to": "The Team Manager, Scuderia AlphaTauri",
  "title": "Decision Car 22 Alleged Unsafe Release",
  "pages": 1,
  "images_extracted": 0,
  "tables_extracted": 0,
  "driver": "Yuki Tsunoda",
  "car_number": 22,
  "team": "Scuderia AlphaTauri",
  "session": "Race",
  "incident_time": "15:32",
  "fact": "Alleged failure to serve a penalty during a pit stop.",
  "offence": "Alleged breach of Article 54.4(c) of the FIA Formula One Sporting Regulations.",
  "decision": "No further action.",
  "reason": "Video evidence from the FIA pit box overhead camera shows the jack moving towards the front of the car as the car stops...",
  "penalty": null,
  "penalty_points": null,
  "stewards": ["Garry Connelly", "Mathieu Remmerie", "Enrique Bernoldi", "Walter Jobst"],
  "rules_referenced": ["Article 54.4(c) FIA Formula One Sporting Regulations"]
}
```

Fields will be `null` when not present or not applicable to the document type.

### 4.3 Images — `images/{slug}_p{page}_img{index}.png`

Extracted images excluding FIA header/logo banners. Stored per-event in an `images/` subfolder. See [Section 7](#7-image-handling) for filtering rules.

---

## 5. Extraction Library

### Why Kreuzberg

| Requirement | Kreuzberg v4.x |
|-------------|----------------|
| Text extraction (searchable PDFs) | ✅ pdfium (Chromium's PDF engine), Rust core |
| Table extraction | ✅ `result.tables` with cells + Markdown rendering |
| Image extraction | ✅ `PdfConfig(extract_images=True)` → `result.images` |
| Per-page extraction | ✅ `PageConfig(extract_pages=True)` |
| Batch processing | ✅ `batch_extract_files_sync()` for parallel extraction |
| Async + sync APIs | ✅ Both available |
| Content filtering | ✅ Strips headers/footers/watermarks automatically |
| Quality scoring | ✅ 0.0–1.0 extraction quality score |
| License | ✅ MIT (PyMuPDF is AGPL) |
| GH Actions compatible | ✅ No GPU needed; system deps via `apt-get` |
| Speed | ✅ Rust core, ms-per-PDF for text extraction |

### Dependencies

**Python package:**
```
kreuzberg>=4.9
```

**System dependencies (GH Actions):**
```bash
sudo apt-get install -y tesseract-ocr pandoc
```

### Extraction Configuration

```python
from kreuzberg import ExtractionConfig, PdfConfig, PageConfig

config = ExtractionConfig(
    output_format="markdown",
    pdf_options=PdfConfig(
        extract_images=True,
        extract_metadata=True,
    ),
    pages=PageConfig(
        extract_pages=True,
    ),
)
```

---

## 6. Metadata Schema

### 6.1 Universal Fields (All Documents)

Extracted from the folder path and the common FIA header present in every document:

| Field | Type | Source | Example |
|-------|------|--------|---------|
| `source_pdf` | string | File path | `"documents/2024/austrian-grand-prix/decision-car-22.pdf"` |
| `source_hash` | string | SHA256 of PDF bytes | `"sha256:a1b2c3..."` |
| `extracted_at` | string | ISO 8601 timestamp | `"2025-05-19T14:30:00Z"` |
| `year` | int | Folder path | `2024` |
| `event_slug` | string | Folder path | `"austrian-grand-prix"` |
| `event_name` | string | PDF header text | `"Austrian Grand Prix"` |
| `doc_type` | string | Filename heuristic | `"decision"` |
| `doc_number` | string\|null | PDF header ("Document" field) | `"65"` |
| `date` | string\|null | PDF header, normalized to ISO | `"2024-06-30"` |
| `time` | string\|null | PDF header | `"14:30"` |
| `from` | string\|null | PDF header | `"The Stewards"` |
| `to` | string\|null | PDF header | `"The Team Manager, Red Bull Racing"` |
| `title` | string | Humanized from filename | `"Decision Car 22 Alleged Unsafe Release"` |
| `pages` | int | Kreuzberg result | `1` |
| `images_extracted` | int | Count after header filtering | `0` |
| `tables_extracted` | int | Kreuzberg result | `0` |

### 6.2 Decision/Offence/Infringement Fields

Extracted via regex from the document body for documents where `doc_type` is `decision`, `offence`, `infringement`, or `summons`:

| Field | Type | Example |
|-------|------|---------|
| `driver` | string\|null | `"Max Verstappen"` |
| `car_number` | int\|null | `1` |
| `team` | string\|null | `"Red Bull Racing"` |
| `session` | string\|null | `"Race"`, `"Qualifying"`, `"FP1"`, `"Sprint"` |
| `incident_time` | string\|null | `"15:32"` |
| `fact` | string\|null | Description of the incident |
| `offence` | string\|null | Regulation article cited |
| `decision` | string\|null | `"5 second time penalty"`, `"No further action"` |
| `reason` | string\|null | Stewards' reasoning |
| `penalty` | string\|null | Normalized penalty (e.g., `"€100 fine"`, `"5s time penalty"`) |
| `penalty_points` | int\|null | Penalty points awarded |
| `stewards` | list[string] | List of steward names |
| `rules_referenced` | list[string] | Regulation articles cited |

### 6.3 Doc Type Detection

Determined from the PDF filename using keyword matching:

| Keyword in filename | `doc_type` value |
|---------------------|------------------|
| `decision` | `"decision"` |
| `summons` | `"summons"` |
| `offence` | `"offence"` |
| `infringement` | `"infringement"` |
| `classification` | `"classification"` |
| `starting-grid` | `"starting-grid"` |
| `scrutineering` | `"scrutineering"` |
| `entry-list` | `"entry-list"` |
| `race-directors` | `"race-director-note"` |
| `circuit-map` | `"circuit-map"` |
| `championship-points` | `"championship-points"` |
| `pu-elements` | `"pu-elements"` |
| `deleted-lap-times` | `"deleted-lap-times"` |
| `curfew` | `"curfew"` |
| `parc-fermé` or `parc-ferme` | `"parc-ferme"` |
| *(none of the above)* | `"other"` |

---

## 7. Image Handling

### 7.1 Extraction

Kreuzberg extracts all embedded images from PDFs via `PdfConfig(extract_images=True)`. Each image in `result.images` (or per-page via `page.images`) includes the image bytes, dimensions, and page location.

### 7.2 FIA Header/Logo Filtering

The FIA banner image appears at the top of virtually every page in every document. It has **zero RAG value** and would account for ~80% of all extracted image data.

**Filter rule — skip an image if ALL of the following are true:**
- The image is positioned in the **top 15% of the page height**
- The image spans **>70% of the page width**
- This applies to **any page** (not just page 1), in **both portrait and landscape** orientation

The page dimensions (width × height) determine the thresholds dynamically, so portrait vs. landscape is handled automatically.

### 7.3 Storage

Images that pass the filter are saved to:
```
extracted/{year}/{event-slug}/images/{doc-slug}_p{page}_img{index}.png
```

Example:
```
extracted/2024/austrian-grand-prix/images/decision-car-22_p1_img1.png
```

The corresponding `.json` metadata records `images_extracted` (count of non-header images).

### 7.4 Expected Storage Impact

- Most decision/summons documents: **0 non-header images** (text only + header)
- Circuit maps: **1 image** (the map itself)
- Some scrutineering/classification docs: **0–2 images**
- Estimated total: **<100MB** of non-header images across all 9,715 PDFs

---

## 8. Tracking & Deduplication

### 8.1 Extraction Manifest

**File:** `extracted/manifest.json`

A single JSON file tracking every extracted document, keyed by the relative source PDF path.

```json
{
  "documents/2024/austrian-grand-prix/decision-car-22-alleged-unsafe-release.pdf": {
    "extracted_at": "2025-05-19T14:30:00Z",
    "source_hash": "sha256:a1b2c3d4e5f6...",
    "pages": 1,
    "images_extracted": 0,
    "tables_extracted": 0,
    "doc_type": "decision",
    "success": true,
    "error": null
  },
  "documents/2023/monaco-grand-prix/circuit-map.pdf": {
    "extracted_at": "2025-05-19T14:31:00Z",
    "source_hash": "sha256:f6e5d4c3b2a1...",
    "pages": 1,
    "images_extracted": 1,
    "tables_extracted": 0,
    "doc_type": "circuit-map",
    "success": true,
    "error": null
  }
}
```

### 8.2 Skip Logic

On each run, `extract.py` performs:

1. **Load** `extracted/manifest.json` (or start empty if first run).
2. **Scan** `documents/` for all `.pdf` files.
3. For each PDF:
   - If the path is **not in the manifest** → extract it.
   - If the path **is in the manifest** but the `source_hash` differs → re-extract (PDF was modified).
   - If the path is in the manifest and the hash matches → **skip**.
4. **Update** `extracted/manifest.json` after processing.

### 8.3 Error Tracking

If extraction fails for a PDF, the manifest records `"success": false` with an `"error"` message. Failed documents are **retried on the next run** (they are not considered "done").

---

## 9. Folder Structure

```
extracted/
├── manifest.json                          # Global extraction tracking
├── 2018/
│   └── australian-grand-prix/
│       ├── decision-car-5-unsafe-release.md
│       ├── decision-car-5-unsafe-release.json
│       ├── final-starting-grid.md
│       ├── final-starting-grid.json
│       ├── race-directors-event-notes.md
│       ├── race-directors-event-notes.json
│       ├── images/
│       │   └── circuit-map_p1_img0.png
│       └── ...
├── 2024/
│   └── austrian-grand-prix/
│       ├── decision-car-22-alleged-unsafe-release.md
│       ├── decision-car-22-alleged-unsafe-release.json
│       └── ...
├── 2025/
│   └── ...
└── 2026/
    └── ...
```

**Naming conventions:**
- Folder structure mirrors `documents/` exactly (`year/event-slug/`)
- File stems match the source PDF filename (minus `.pdf`)
- Images go in a per-event `images/` subfolder, prefixed with the doc slug

---

## 10. Extraction Script (`extract.py`)

### 10.1 CLI Interface

```bash
# Extract all unprocessed PDFs (default)
uv run python extract.py

# Extract a specific year
uv run python extract.py --year 2024

# Extract a specific event
uv run python extract.py --year 2024 --event austrian-grand-prix

# Force re-extraction (ignore manifest)
uv run python extract.py --force

# Dry run (show what would be extracted)
uv run python extract.py --dry-run

# Limit batch size (for GH Actions)
uv run python extract.py --limit 100
```

### 10.2 Script Structure

```
extract.py
├── main()                        # CLI entry point (argparse)
├── load_manifest()               # Read extracted/manifest.json
├── save_manifest()               # Write extracted/manifest.json
├── discover_pdfs()               # Scan documents/ for all .pdf files
├── compute_hash()                # SHA256 of file content
├── needs_extraction()            # Check manifest + hash
├── extract_pdf()                 # Single PDF extraction via Kreuzberg
│   ├── extract_text()            # Text + tables → .md
│   ├── extract_metadata()        # Regex parsing → .json
│   ├── extract_images()          # Image extraction + header filtering
│   └── detect_doc_type()         # Filename-based classification
├── parse_header_fields()         # Regex: From, To, Document, Date, Time
├── parse_decision_fields()       # Regex: Driver, Team, Session, Fact, etc.
├── is_header_image()             # Position + size heuristic
└── humanize_title()              # "decision-car-22" → "Decision Car 22"
```

### 10.3 Key Behaviors

- **Atomic writes**: Write `.md`, `.json`, images first, then update manifest last. If the script crashes mid-document, the manifest won't record it as done.
- **Error resilience**: Catch exceptions per-PDF, log the error, record `"success": false` in manifest, continue to next PDF.
- **Progress reporting**: Use `rich` progress bar for local backfill runs.
- **Deterministic output**: Same PDF always produces the same `.md` + `.json` (no random elements).

---

## 11. GitHub Actions Integration

### 11.1 Changes to `scrape.yml`

The extraction step is added **after** both scrapers run and **before** the commit step. The sparse checkout is extended to include the extraction script and manifest.

```yaml
# Added to sparse-checkout list:
#   extract.py
#   extracted/manifest.json

- name: Install system dependencies for extraction
  run: |
    sudo apt-get update
    sudo apt-get install -y tesseract-ocr pandoc

- name: Run extraction on new documents
  id: extract
  run: |
    uv run python extract.py --limit 200
    echo "extracted_count=$(uv run python extract.py --count-new)" >> $GITHUB_OUTPUT

# Modified commit step to also add extracted/
- name: Commit and push
  run: |
    git add documents/ classification/ extracted/
    # ... rest of existing commit logic ...
```

### 11.2 Sparse Checkout Updates

The workflow sparse checkout needs these additions:
```yaml
sparse-checkout: |
  scraper.py
  transcript_scraper.py
  extract.py          # NEW
  config.py
  shared_utils.py
  pyproject.toml
  uv.lock
  documents/manifest.json
  documents/discovery_cache.json
  documents/transcript_discovery_cache.json
  extracted/manifest.json   # NEW
```

**Important**: The sparse checkout includes the source PDFs implicitly because the scraper step writes them. The extraction step operates on whatever PDFs are present in the working tree — both newly scraped ones and any that were part of the checkout.

### 11.3 Timing Budget

| Step | Estimated time |
|------|---------------|
| Scraper | ~5–15 min |
| Transcript scraper | ~2–5 min |
| System deps install | ~30s |
| Extraction (0–10 new PDFs typical) | ~10–30s |
| Commit + push | ~30s |
| **Total** | **~10–20 min** (well within 60 min timeout) |

### 11.4 Commit Message Update

The commit message is updated to include extraction counts:
```
docs: add 5 FIA F1 documents (3 decisions, 2 transcripts), extracted 3 PDFs [2025-05-19 14:30 UTC]
```

---

## 12. Initial Backfill Strategy

### 12.1 Local Execution

Run `extract.py` locally on the full corpus once:

```bash
# Full backfill — processes all ~9,715 PDFs
uv run python extract.py
```

### 12.2 Expected Performance

| Metric | Estimate |
|--------|----------|
| PDFs to process | ~9,715 |
| Extraction speed (Kreuzberg Rust core) | ~50–200 ms/PDF |
| Total extraction time | ~10–30 minutes |
| Output size (text + JSON) | ~50–100 MB |
| Output size (images, after header filtering) | ~50–100 MB |
| Total `extracted/` folder | ~100–200 MB |

### 12.3 Backfill Steps

1. Ensure Kreuzberg + system deps are installed locally.
2. Run `uv run python extract.py` from the repo root.
3. Review a sample of outputs for quality.
4. Commit the full `extracted/` folder.
5. Push to remote.
6. From this point on, GH Actions handles incremental extraction.

---

## 13. Future: Category-Specific Parsers

After the universal parser is shipping, we layer richer extraction for high-value document types. These parsers enhance the `.json` metadata with fields specific to each category.

### 13.1 Priority Order

| Priority | Category | Count | Additional Fields |
|----------|----------|-------|-------------------|
| **P0** | Decisions + Offences + Infringements | ~1,518 | Driver, car#, team, session, fact, offence, decision, reason, penalty, penalty points, rules referenced, stewards |
| **P0** | Summons | 756 | Driver, car#, team, hearing time, reason for summons |
| **P1** | Classifications (starting grids, race/qual results) | ~900+ | Full tabular data: position, driver, team, time, gap, laps |
| **P1** | Deleted Lap Times | ~250+ | Tabular: car, driver, turn, lap time |
| **P2** | PU Elements | 147 | Tabular: driver × component counts (ICE, TC, MGU-H, etc.) |
| **P2** | Championship Points | 152 | Tabular: driver/team standings |
| **P3** | Race Director Notes | 338 | Sections by topic (DRS zones, track limits, procedures) |
| **P3** | Scrutineering Reports | 400+ | Cars checked, checks performed |

### 13.2 Implementation Approach

Each category parser is a function that:
1. Receives the raw extracted text and existing universal metadata.
2. Applies category-specific regex/parsing logic.
3. Returns additional fields to merge into the `.json` sidecar.

```python
# Future structure in extract.py
CATEGORY_PARSERS = {
    "decision": parse_decision_fields,
    "offence": parse_decision_fields,      # Same format as decisions
    "infringement": parse_decision_fields,  # Same format as decisions
    "summons": parse_summons_fields,
    "classification": parse_classification_table,
    # ...
}
```

**Note:** The P0 parsers (decisions/summons) are included in the initial `extract.py` implementation since the regex patterns are well-understood from the document samples. P1+ parsers are deferred.

---

## 14. Open Questions

| # | Question | Impact | Notes |
|---|----------|--------|-------|
| 1 | **Exact Kreuzberg version to pin** | Build reproducibility | v4.x is rapidly evolving (~4.9.5 as of May 2025); need to test which version works on ubuntu GH Actions runner |
| 2 | **Git repo size monitoring** | Long-term viability | If images push the repo past GitHub's soft 1GB limit, may need to revisit images-in-git or add Git LFS |
| 3 | **Category-specific parser priority** | Extraction richness | Which doc types get enriched extraction first after the universal parser ships? (Tentatively: decisions → summons → grids) |
| 4 | **Date format normalization** | Search quality | FIA dates vary: "01 December 2019", "30 June - 02 July 2023", etc. Need robust parsing to ISO 8601 |
| 5 | **Multi-car decisions** | Schema design | Some decisions reference multiple cars/drivers (e.g., "incident between car 99 and car 88"). Should `driver`/`car_number` be arrays? |
| 6 | **2018 document naming** | Filename parsing | 2018 docs use a different naming convention (e.g., `2018_spanish_grand_prix_2018_offence_-_car_2_...`). Doc type detection regex needs to handle both old and new formats |
| 7 | **Kreuzberg image extraction format** | Storage | Need to verify what format Kreuzberg returns images in (PNG? JPEG? original?) and whether we should normalize |
