# FIA Document Scraper Data Structures

This document outlines the JSON data structures generated and maintained by the FIA F1 Document Scraper. The data is heavily summarized to be lightweight enough for serving over static file hosts (like GitHub Pages or simple CDNs).

---

## 1. `manifest.json`

**Location:** `documents/manifest.json`

The global registry of every document ever downloaded. It is primarily used by the scraping engine to avoid re-downloading PDFs and to backfill or migrate missing metadata.

**Structure:** A dictionary where the keys are the original `fia.com` URLs, and values are metadata objects.

```json
{
  "https://www.fia.com/sites/default/files/example-document.pdf": {
    "doc_number": 2,
    "event": "Chinese Grand Prix",
    "path": "documents\\2019\\chinese-grand-prix\\example-document.pdf",
    "published_time": "11.04.19 11:15",
    "source": "decision",
    "title": "2019 Chinese Grand Prix - Initial Scrutineering",
    "year": 2019
  }
}
```

### Fields:
-   **`key` (URL)**: The source URL of the PDF on fia.com.
-   **`doc_number`** *(int)*: Sequential document number (extracted from the FIA filename/link text). Will be `0` if it cannot be determined.
-   **`event`** *(string)*: The native name of the Grand Prix.
-   **`path`** *(string)*: The relative file path to the downloaded PDF on disk.
-   **`published_time`** *(string)*: Parsed publish time from the FIA website (e.g. `"11.04.19 11:15"`).
-   **`source`** *(string)*: Document categorization type, generally `"decision"`.
-   **`title`** *(string)*: A cleanly parsed title of the document.
-   **`year`** *(int)*: The championship year the event belongs to.

---

## 2. Year Indices (`index.json`)

**Location:** `documents/[year]/index.json` (e.g., `documents/2019/index.json`)

An index of all the Formula 1 events scraped for a specific year. Useful for populating a dropdown or calendar view on the front-end.

**Structure:** An array of compact objects, ordered alphabetically by event slug.

```json
[
  {"s": "abu-dhabi-grand-prix", "n": "Abu Dhabi Grand Prix"},
  {"s": "australian-grand-prix", "n": "Australian Grand Prix"}
]
```

### Fields:
-   **`s`** *(string)*: The URL-friendly slug of the event name. This maps exactly to the folder name containing that event's documents.
-   **`n`** *(string)*: The original, human-readable name of the event.

---

## 3. Event Indices (`index.json`)

**Location:** `documents/[year]/[event-slug]/index.json` (e.g., `documents/2019/australian-grand-prix/index.json`)

Contains the list of documents available for a specific event. This is highly minified to decrease bandwidth when a client fetches the document list for a race.

**Structure:** An array of objects representing individual PDFs. The items are sorted generally by document number (if known), then by published time, then by filename.

```json
[
  {
    "f": "2019-chinese-grand-prix-initial-scrutineering.pdf",
    "t": "2019 Chinese Grand Prix - Initial Scrutineering",
    "n": 2,
    "p": "11.04.19 11:15"
  }
]
```

### Fields:
-   **`f`** *(string)*: The actual filename of the PDF within this event's directory.
-   **`t`** *(string)*: The document title.
-   **`n`** *(int, optional)*: The document sequence number. Excluded if `0` or unknown.
-   **`p`** *(string, optional)*: The published timestamp. Excluded if unknown or empty.

---

## 4. `discovery_cache.json`

**Location:** `documents/discovery_cache.json`

A temporary file generated during the scraper run. Because the fia.com site structure requires both initial crawls and asynchronous AJAX calls to find all documents, this cache stores the full hierarchy of seasons, events, and document links as a snapshot.

**Structure:**

```json
{
  "timestamp": 1699999999.123,
  "events": [
    {
      "name": "Chinese Grand Prix",
      "id": "123",
      "year": 2019,
      "inline_docs": [],
      "documents": [
        {
          "title": "Document Title",
          "url": "https://...",
          "filename": "slugified-title.pdf",
          "doc_number": 2,
          "published_time": "11.04.19 11:15"
        }
      ]
    }
  ]
}
```

### Fields:
-   **`timestamp`** *(float)*: Epoch timestamp of when the file was written. Used to validate the TTL (usually 2.5 hours).
-   **`events`** *(array)*: Deep breakdown of crawled events to bypass re-crawling within the same cron interval.
