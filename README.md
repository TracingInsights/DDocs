# FIA F1 Document Scraper

You can browse these FIA F1 Decision Documents at https://tracinginsights.com/ or f1tel.com. Also able to download these as pdf/images on website.


Automatically scrapes Formula 1 decision documents (PDFs) from the [FIA website](https://www.fia.com/documents/championships/fia-formula-one-world-championship-14) and stores them in this repository.

## Directory Structure

```
documents/
├── 2026/
│   ├── chinese-grand-prix/
│   │   ├── championship-points.pdf
│   │   ├── final-starting-grid.pdf
│   │   └── ...
│   ├── australian-grand-prix/
│   │   └── ...
│   └── ...
├── 2025/
│   └── ...
└── manifest.json
```

## How It Works

- A GitHub Action runs every **3 hours** to check for new documents and transcripts
- Uses `asyncio` + `aiohttp` for parallel AJAX discovery and PDF downloads (15 concurrent downloads)
- All events within a season are discovered in parallel via AJAX
- Press conference transcripts are automatically scraped and converted to Markdown
- A `manifest.json` tracks all downloaded URLs to avoid duplicates

## Manual Usage

```bash
# Install dependencies
uv sync

# Scrape all available seasons
uv run python scraper.py

# Scrape a specific year
uv run python scraper.py --year 2025

# Scrape press conference transcripts for 2026 (standard mode)
uv run python transcript_scraper.py --year 2026

# Scrape transcripts with aggressive discovery (checks hub pages)
uv run python transcript_scraper.py --year 2026 --aggressive

# Scrape transcripts with super-aggressive discovery (checks hub + timing pages + PDFs)
uv run python transcript_scraper.py --year 2026 --super-aggressive

# Scrape all historical transcripts (2018-2022)
uv run python transcript_scraper.py --all-historical --super-aggressive

# Scrape 2025 HTML classification tables into classification/ JSON
uv run python fetch_event_classifications.py

# Scrape the dedicated 2018 classification parser/profile
uv run python fetch_event_classifications.py --year 2018

# Scrape the dedicated 2019 classification parser/profile
uv run python fetch_event_classifications.py --year 2019

# Scrape the dedicated 2020 classification parser/profile
uv run python fetch_event_classifications.py --year 2020

# Scrape the dedicated 2021 classification parser/profile
uv run python fetch_event_classifications.py --year 2021

# Scrape the dedicated 2022 classification parser/profile
uv run python fetch_event_classifications.py --year 2022

# Scrape the dedicated 2023 classification parser/profile
uv run python fetch_event_classifications.py --year 2023

# Scrape the dedicated 2024 classification parser/profile
uv run python fetch_event_classifications.py --year 2024

# Scrape the dedicated 2025 classification parser/profile explicitly
uv run python fetch_event_classifications.py --year 2025

# Custom output directory
uv run python scraper.py --output-dir my-docs
```

## Manual Trigger

You can manually trigger the scraper from the **Actions** tab > **Scrape FIA F1 Documents** > **Run workflow**.
