"""Shared configuration for FIA F1 scrapers."""

import os

# Cache configuration
# Default to 2.5 hours (slightly under 3-hour cron interval)
# Can be overridden via environment variable
DISCOVERY_CACHE_TTL_HOURS = float(os.environ.get("DISCOVERY_CACHE_TTL_HOURS", "2.5"))
DISCOVERY_CACHE_TTL_SECONDS = DISCOVERY_CACHE_TTL_HOURS * 60 * 60

# Cache version - increment when discovery logic changes significantly
CACHE_VERSION = 1

# Concurrency limits
MAX_AJAX_CONCURRENT = 10
MAX_DOWNLOAD_CONCURRENT = 15
MAX_FETCH_CONCURRENT = 5

# Base URLs
BASE_URL = "https://www.fia.com"
CHAMPIONSHIP_PATH = "/documents/championships/fia-formula-one-world-championship-14"
DOCUMENTS_URL = f"{BASE_URL}{CHAMPIONSHIP_PATH}"
AJAX_URL = f"{BASE_URL}/decision-document-list/ajax/"
NEWS_URL_BASE = f"{BASE_URL}/news/"
ARCHIVE_URL_2018 = f"{BASE_URL}/f1-archives?season=866"

# HTTP headers
AJAX_EXTRA_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}