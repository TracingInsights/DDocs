# FIA Documents RAG — Full Implementation Plan (Django Edition)
## "Chat with the FIA Docs" — Production on a 4GB VPS

---

## What We Already Have (The DDocs Repo)

This repo is not a blank slate. The heavy lifting of PDF processing is **already done**:

```
DDocs/
├── documents/          # Raw PDFs: documents/{year}/{event-name}/{pdf-name}.pdf
│   ├── 2018/
│   ├── 2019/
│   ...
│   └── 2026/
├── extracted/          # ✅ Already extracted: one .md + .json per PDF
│   ├── manifest.json   # Master index of ALL extracted docs (source_hash, doc_type, etc.)
│   ├── 2018/
│   │   └── abu-dhabi-grand-prix/
│   │       ├── doc_20_-_..._decision_-_haas_protest.md      # Markdown text + tables
│   │       ├── doc_20_-_..._decision_-_haas_protest.json    # Sidecar metadata
│   │       ├── images/
│   │       └── ...
│   ├── 2019/
│   ...
│   └── 2026/
├── extract.py          # The extraction engine (pdfplumber + PyMuPDF → .md + .json)
└── pyproject.toml      # uv-managed Python project
```

### What `extract.py` gives us per PDF

| File | Contents |
|---|---|
| `{stem}.md` | Full document text with tables in Markdown — ready for chunking |
| `{stem}.json` | Sidecar: `source_hash`, page count, table count, visual pages |
| `images/*.png` | High-DPI renders of visual-only pages (circuit maps, etc.) |

### What `extracted/manifest.json` gives us

Each key is `documents/{year}/{event}/{pdf-name}.pdf` and maps to:

```json
{
  "doc_type":          "decision",        // pre-classified: decision, summons, offence,
                                          // scrutineering, classification, starting-grid,
                                          // entry-list, other
  "success":           true,
  "source_hash":       "sha256:...",      // used for dedup — never re-ingest same file
  "pages":             8,
  "tables_extracted":  0,
  "images_extracted":  0,
  "extracted_at":      "2026-05-21T11:09:19Z",
  "error":             null
}
```

**Key insight**: `year` and `event` are parsed directly from the key path. `doc_type` is already
classified. We do **not** need an LLM to infer these. LLM extraction is reserved only for
fine-grained content fields (driver name, car number, penalty type) from the `.md` text.

---

## Architecture Overview

```
User Browser
     │
     ▼
  Nginx (reverse proxy + SSL)
     │
     ▼
  Gunicorn (1 Uvicorn worker to preserve RAM)
     │
     ▼
  Django Project
     ├── Auth & Sessions      (Django ORM + SQLite)
     ├── Ingestion Queue      (Django ORM + SQLite → processed by Systemd Timer)
     ├── API Layer            (Django Ninja - async)
     └── RAG Logic
          ├─── Layer 1: DuckDB (documents + chunks tables)
          ├─── Layer 2: BM25 index (rank_bm25) + LanceDB (chunk-level vectors)
          ├─── Layer 3: Query rewriter + classifier (SQL-first logic)
          └─── Layer 4: Gemini 2.5 Flash (final answer)
                             │
                    [Gemini API rate limiter — in-process]

Data Flow (Ingestion):
  extracted/manifest.json
          │
          ▼
  bulk_ingest.py / management command
          │  reads .md + .json sidecar
          │  parses year/event from path
          │  LLM extracts driver/penalty fields (only for decision/summons/offence)
          ▼
  DuckDB: documents + chunks tables
          │
          ▼
  LanceDB (vectors) + BM25 pickle
```

**Why this stack for 4GB RAM?**
- **No Redis/Celery:** Django ORM queue + systemd timer saves ~300MB.
- **Django Ninja:** Async views + Pydantic validation natively inside Django.
- **SQLite for Django, DuckDB for Analytics:** DuckDB handles analytical SQL
  (`AGGREGATION`) and chunk storage without locking issues.
- **Extraction is offline:** `extract.py` runs on this dev machine. The VPS only
  needs to serve the pre-built DuckDB + LanceDB.

---

## Directory Structure (Django App, separate from DDocs repo)

```
fia_rag/
├── manage.py
├── fia_project/              # Django project config
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
├── core/                     # Main Django app
│   ├── models.py             # User, ChatSession, Message, IngestionQueue
│   ├── api.py                # Django Ninja endpoints
│   ├── schema.py             # Pydantic schemas
│   ├── services/
│   │   ├── duckdb.py         # DuckDB connection singleton
│   │   ├── chunker.py        # Splits .md files into chunks
│   │   ├── extractor.py      # LLM extraction of deep content fields only
│   │   ├── retrieval.py      # SQL-first, BM25, Vector, Context Builder
│   │   ├── router.py         # Query rewriter & classifier
│   │   └── answerer.py       # Gemini Flash answer generation
│   └── management/
│       └── commands/
│           └── process_ingestion_queue.py
├── data/
│   ├── extracted/            # Symlink or copy of DDocs/extracted/
│   │   ├── manifest.json     # Master index
│   │   ├── 2018/
│   │   ├── 2019/
│   │   ...
│   │   └── 2026/
│   ├── fia_analytics.duckdb  # Built by bulk_ingest.py
│   ├── bm25_index.pkl        # Serialized BM25 index
│   └── lancedb/              # LanceDB vector store
├── scripts/
│   └── bulk_ingest.py        # One-time cold-start ingest from extracted/
├── frontend/                 # Static SPA (vanilla typescript + tailwindcss v3)
├── nginx/
│   └── fia_rag.conf
└── systemd/
    ├── fia_rag.service
    └── fia_ingest.timer
```

---

## Layer 0: Path Utilities (Parsing year/event from extracted/ paths)

Since `year` and `event` live in the path, we parse them rather than using an LLM:

```python
# core/services/path_utils.py
import re
from pathlib import Path

def parse_doc_path(manifest_key: str) -> dict:
    """
    Input:  'documents/2024/miami-grand-prix/doc_17_-_..._decision.pdf'
    Output: {'year': 2024, 'event': 'miami-grand-prix', 'pdf_name': 'doc_17...pdf'}
    """
    parts = Path(manifest_key).parts  # ('documents', '2024', 'miami-grand-prix', 'doc_17...pdf')
    return {
        "year": int(parts[1]),
        "event": parts[2],               # e.g. 'miami-grand-prix'
        "pdf_name": parts[3],
        "pdf_stem": Path(parts[3]).stem, # filename without .pdf
    }

def get_extracted_md_path(manifest_key: str, extracted_root: Path) -> Path:
    """Returns path to the .md file for a given manifest key."""
    info = parse_doc_path(manifest_key)
    return extracted_root / str(info["year"]) / info["event"] / (info["pdf_stem"] + ".md")

def get_extracted_json_path(manifest_key: str, extracted_root: Path) -> Path:
    """Returns path to the .json sidecar for a given manifest key."""
    info = parse_doc_path(manifest_key)
    return extracted_root / str(info["year"]) / info["event"] / (info["pdf_stem"] + ".json")
```

---

## Layer 1: Data Models (Django SQLite + DuckDB)

### 1.1 Django Models (SQLite)

```python
# core/models.py
import uuid
from django.db import models
from django.contrib.auth.models import AbstractUser

class User(AbstractUser):
    daily_quota = models.IntegerField(default=20)
    queries_today = models.IntegerField(default=0)
    quota_reset_date = models.DateField(auto_now_add=True)

class ChatSession(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

class Message(models.Model):
    session = models.ForeignKey(ChatSession, on_delete=models.CASCADE, related_name='messages')
    role = models.CharField(max_length=10)  # 'user' or 'assistant'
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

class IngestionQueue(models.Model):
    """Tracks newly extracted .md files waiting to enter DuckDB."""
    STATUS_CHOICES = [
        ('pending', 'Pending'), ('processing', 'Processing'),
        ('done', 'Done'), ('failed', 'Failed')
    ]
    manifest_key = models.CharField(max_length=512, unique=True)  # e.g. 'documents/2024/.../doc.pdf'
    source_hash = models.CharField(max_length=80)                  # sha256 from manifest.json
    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

### 1.2 DuckDB Schema (Analytics & Retrieval)

```sql
-- documents table: one row per extracted PDF
CREATE TABLE IF NOT EXISTS documents (
    id               VARCHAR PRIMARY KEY,  -- sha256 from manifest (dedup key)
    manifest_key     VARCHAR NOT NULL,     -- 'documents/{year}/{event}/{pdf-name}.pdf'
    md_path          VARCHAR NOT NULL,     -- absolute path to .md file on disk
    year             INTEGER,
    event            VARCHAR,              -- 'miami-grand-prix'
    pdf_stem         VARCHAR,              -- filename stem (no extension)

    -- From manifest.json (no LLM needed)
    doc_type         VARCHAR,              -- 'decision','summons','offence','scrutineering',
                                           --  'classification','starting-grid','entry-list','other'
    pages            INTEGER,
    tables_extracted INTEGER,
    extracted_at     TIMESTAMP,

    -- LLM-extracted (only for decision/summons/offence doc_types)
    driver_name      VARCHAR,
    car_number       VARCHAR,
    team             VARCHAR,
    charge_description TEXT,
    charge_normalized  VARCHAR,
    regulation_articles VARCHAR[],
    heard            BOOLEAN,
    decision         VARCHAR,
    penalty_type     VARCHAR,
    penalty_value    VARCHAR,
    penalty_unit     VARCHAR,
    stewards         VARCHAR[],

    ingested_at      TIMESTAMP DEFAULT now()
);

-- chunks table: granular retrieval units (split from .md files)
CREATE TABLE IF NOT EXISTS chunks (
    id           VARCHAR PRIMARY KEY,  -- '{doc_id}_chunk_{index}'
    doc_id       VARCHAR NOT NULL REFERENCES documents(id),
    chunk_index  INTEGER,
    header_path  VARCHAR,              -- markdown header breadcrumb
    chunk_text   TEXT,
    bm25_id      INTEGER               -- row index in the BM25 corpus list
);
```

---

## Layer 2: Ingestion Pipeline

### Philosophy: manifest.json is the source of truth

The `extracted/manifest.json` already has `source_hash` (sha256) for every extracted PDF.
We use this as the **deduplication key** — if `source_hash` is already in DuckDB's `documents.id`,
we skip it. No polling, no watchdog.

### 2.1 Cold-Start Bulk Ingest Script

This runs **once** (on dev machine or VPS during setup) to populate DuckDB from the existing
`extracted/` folder:

```python
# scripts/bulk_ingest.py
"""
Walks extracted/manifest.json, finds all successful extractions not yet in DuckDB,
and ingests them in batches — respecting Gemini API rate limits.

Only decision/summons/offence documents get LLM field extraction.
Everything else is fast (just path parsing + chunking).

Usage: uv run python scripts/bulk_ingest.py [--year 2024] [--event miami-grand-prix]
       uv run python scripts/bulk_ingest.py --dry-run
       uv run python scripts/bulk_ingest.py --skip-llm  # ingest without LLM fields
"""
import json
import time
import asyncio
import argparse
import hashlib
import duckdb
from pathlib import Path

EXTRACTED_ROOT = Path("data/extracted")
MANIFEST_PATH  = EXTRACTED_ROOT / "manifest.json"
DUCKDB_PATH    = Path("data/fia_analytics.duckdb")

# Gemini free tier: 1000 RPD, ~15 RPM
BATCH_SIZE       = 900   # stay under 1000 RPD (LLM calls only for rich doc types)
DELAY_SECONDS    = 1.5   # 60s / 40 RPM = 1.5s

RICH_DOC_TYPES = {"decision", "summons", "offence"}  # Only these get LLM extraction

def parse_doc_path(key: str) -> dict:
    parts = Path(key).parts
    return {"year": int(parts[1]), "event": parts[2],
            "pdf_name": parts[3], "pdf_stem": Path(parts[3]).stem}

def chunk_markdown(doc_id: str, text: str, max_size=1200, overlap=200) -> list[dict]:
    """Simple header-aware chunker for FIA .md files."""
    import re
    sections = re.split(r'\n(?=#{1,4} )', text)
    chunks = []
    idx = 0
    for section in sections:
        header = ""
        m = re.match(r'^(#{1,4} .+)\n', section)
        if m:
            header = m.group(1).strip("# ").strip()
        if len(section) <= max_size:
            if section.strip():
                chunks.append({"doc_id": doc_id, "chunk_index": idx,
                               "header_path": header, "chunk_text": section.strip()})
                idx += 1
        else:
            # Sliding window on oversized sections
            start = 0
            while start < len(section):
                end = start + max_size
                chunks.append({"doc_id": doc_id, "chunk_index": idx,
                               "header_path": header,
                               "chunk_text": section[start:end].strip()})
                idx += 1
                start += max_size - overlap
    return chunks

async def llm_extract_fields(md_text: str) -> dict:
    """Uses Gemini Flash Lite to extract structured fields from rich documents."""
    import google.generativeai as genai
    prompt = f"""You are a FIA Formula 1 document parser.
Extract the following fields from this FIA document as JSON.
Return ONLY valid JSON with these exact keys. Use null if not found.

Fields:
- driver_name (string): Full name of the driver involved
- car_number (string): Car number (e.g. "44")
- team (string): Constructor name
- charge_description (string): The alleged offence or charge
- charge_normalized (string): Short normalized category (e.g. "track limits", "unsafe release")
- regulation_articles (array of strings): FIA regulation articles cited
- heard (boolean): Was a hearing held?
- decision (string): "penalty" | "no further action" | "reprimand" | "disqualified" | null
- penalty_type (string): "time penalty" | "grid penalty" | "fine" | "drive-through" | null
- penalty_value (string): The numeric or textual value of the penalty
- penalty_unit (string): "seconds" | "positions" | "euros" | null
- stewards (array of strings): Names of the stewards

Document:
---
{md_text[:4000]}
---
JSON:"""

    model = genai.GenerativeModel('gemini-2.5-flash-lite')
    response = await model.generate_content_async(
        prompt,
        generation_config={"response_mime_type": "application/json", "max_output_tokens": 512}
    )
    try:
        return json.loads(response.text)
    except Exception:
        return {}

def run(year_filter=None, event_filter=None, dry_run=False, skip_llm=False):
    manifest = json.loads(MANIFEST_PATH.read_text())
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=False)

    # Build set of already-ingested source_hashes
    try:
        ingested_ids = set(r[0] for r in conn.execute("SELECT id FROM documents").fetchall())
    except Exception:
        ingested_ids = set()

    to_ingest = []
    for key, meta in manifest.items():
        if not meta.get("success"):
            continue
        doc_id = meta["source_hash"]
        if doc_id in ingested_ids:
            continue
        info = parse_doc_path(key)
        if year_filter and info["year"] != year_filter:
            continue
        if event_filter and info["event"] != event_filter:
            continue
        to_ingest.append((key, meta, info))

    print(f"Found {len(to_ingest)} documents to ingest. dry_run={dry_run}")
    if dry_run:
        return

    llm_calls_today = 0
    for key, meta, info in to_ingest:
        doc_id   = meta["source_hash"]
        md_path  = EXTRACTED_ROOT / str(info["year"]) / info["event"] / (info["pdf_stem"] + ".md")

        if not md_path.exists():
            print(f"  SKIP (no .md file): {key}")
            continue

        md_text = md_path.read_text(encoding="utf-8")
        doc_type = meta.get("doc_type", "other")

        # LLM extraction only for rich doc types, and only when not skipped
        llm_fields = {}
        if not skip_llm and doc_type in RICH_DOC_TYPES:
            if llm_calls_today >= BATCH_SIZE:
                print("Daily LLM limit reached. Sleep 24h...")
                time.sleep(86400)
                llm_calls_today = 0
            llm_fields = asyncio.run(llm_extract_fields(md_text))
            llm_calls_today += 1
            time.sleep(DELAY_SECONDS)

        # Insert document row
        conn.execute("""
            INSERT OR IGNORE INTO documents
            (id, manifest_key, md_path, year, event, pdf_stem,
             doc_type, pages, tables_extracted, extracted_at,
             driver_name, car_number, team, charge_description, charge_normalized,
             regulation_articles, heard, decision, penalty_type, penalty_value,
             penalty_unit, stewards)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [
            doc_id, key, str(md_path), info["year"], info["event"], info["pdf_stem"],
            doc_type, meta.get("pages"), meta.get("tables_extracted"),
            meta.get("extracted_at"),
            llm_fields.get("driver_name"), llm_fields.get("car_number"),
            llm_fields.get("team"), llm_fields.get("charge_description"),
            llm_fields.get("charge_normalized"), llm_fields.get("regulation_articles"),
            llm_fields.get("heard"), llm_fields.get("decision"),
            llm_fields.get("penalty_type"), llm_fields.get("penalty_value"),
            llm_fields.get("penalty_unit"), llm_fields.get("stewards"),
        ])

        # Insert chunks
        chunks = chunk_markdown(doc_id, md_text)
        for c in chunks:
            conn.execute("""
                INSERT OR IGNORE INTO chunks (id, doc_id, chunk_index, header_path, chunk_text)
                VALUES (?,?,?,?,?)
            """, [f"{doc_id}_c{c['chunk_index']}", c["doc_id"], c["chunk_index"],
                  c["header_path"], c["chunk_text"]])

        print(f"  OK [{doc_type}] {info['year']}/{info['event']}/{info['pdf_stem']} "
              f"({len(chunks)} chunks)")

    conn.close()
    print("Ingest complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--event", type=str)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-llm", action="store_true")
    args = parser.parse_args()
    run(year_filter=args.year, event_filter=args.event,
        dry_run=args.dry_run, skip_llm=args.skip_llm)
```

### 2.2 Incremental Ingest (Management Command for new extractions)

After the cold start, new docs arrive when `extract.py` runs (via GitHub Actions). The management
command scans manifest.json for new `source_hash` values not yet in DuckDB:

```python
# core/management/commands/process_ingestion_queue.py
import json, asyncio
from pathlib import Path
from django.core.management.base import BaseCommand
from core.services.duckdb import get_duckdb_conn

EXTRACTED_ROOT = Path("data/extracted")
MANIFEST_PATH  = EXTRACTED_ROOT / "manifest.json"
RICH_DOC_TYPES = {"decision", "summons", "offence"}

class Command(BaseCommand):
    help = 'Syncs newly extracted documents from extracted/manifest.json into DuckDB'

    def handle(self, *args, **options):
        manifest = json.loads(MANIFEST_PATH.read_text())
        conn = get_duckdb_conn(read_only=False)
        ingested = set(r[0] for r in conn.execute("SELECT id FROM documents").fetchall())

        new_docs = {k: v for k, v in manifest.items()
                    if v.get("success") and v["source_hash"] not in ingested}

        if not new_docs:
            self.stdout.write("No new documents.")
            return

        self.stdout.write(f"Processing {len(new_docs)} new documents...")
        # Import and call the same logic as bulk_ingest (single doc at a time)
        from scripts.bulk_ingest import chunk_markdown, llm_extract_fields
        from core.services.path_utils import parse_doc_path

        for key, meta in new_docs.items():
            info = parse_doc_path(key)
            md_path = EXTRACTED_ROOT / str(info["year"]) / info["event"] / (info["pdf_stem"] + ".md")
            if not md_path.exists():
                continue
            md_text = md_path.read_text(encoding="utf-8")
            doc_id  = meta["source_hash"]
            doc_type = meta.get("doc_type", "other")

            llm_fields = {}
            if doc_type in RICH_DOC_TYPES:
                try:
                    llm_fields = asyncio.run(llm_extract_fields(md_text))
                except Exception as e:
                    self.stderr.write(f"LLM failed for {key}: {e}")

            # Insert document + chunks (same SQL as bulk_ingest)
            # ... (identical insert logic)
            self.stdout.write(f"  Ingested: {key}")
```

---

## Layer 3: Retrieval Logic (SQL-First + Hybrid)

### 3.1 SQL-First Retrieval

Leverages the structured `documents` table. For queries that mention a specific event,
year, driver, or doc type — we can skip vector search entirely.

```python
# core/services/retrieval.py
from core.services.duckdb import get_duckdb_conn

def sql_first_search(entities: dict) -> list[str] | None:
    """Returns doc_ids (source_hash values) when structured entities match."""
    conn = get_duckdb_conn()
    conditions = []
    params = []

    if entities.get("year"):
        conditions.append("year = ?")
        params.append(entities["year"])
    if entities.get("event"):
        conditions.append("event ILIKE ?")
        params.append(f"%{entities['event']}%")
    if entities.get("driver_name"):
        conditions.append("driver_name ILIKE ?")
        params.append(f"%{entities['driver_name']}%")
    if entities.get("doc_type"):
        conditions.append("doc_type = ?")
        params.append(entities["doc_type"])
    if entities.get("car_number"):
        conditions.append("car_number = ?")
        params.append(entities["car_number"])

    if not conditions:
        return None

    sql = f"SELECT id FROM documents WHERE {' AND '.join(conditions)} LIMIT 10"
    results = conn.execute(sql, params).fetchall()
    return [r[0] for r in results] if results else None

def fetch_docs_by_ids(doc_ids: list[str]) -> list[str]:
    """Reads raw .md text directly from disk for given doc IDs."""
    conn = get_duckdb_conn()
    rows = conn.execute(
        f"SELECT md_path FROM documents WHERE id IN ({','.join('?' * len(doc_ids))})",
        doc_ids
    ).fetchall()
    texts = []
    for (path,) in rows:
        from pathlib import Path
        p = Path(path)
        if p.exists():
            texts.append(p.read_text(encoding="utf-8")[:8000])  # cap for context
    return texts

def build_context_from_chunks(chunk_ids: list[str]) -> list[str]:
    """Fetches .md text from disk for the parent docs of given chunk IDs."""
    conn = get_duckdb_conn()
    doc_ids_rows = conn.execute(
        f"SELECT DISTINCT doc_id FROM chunks WHERE id IN ({','.join('?' * len(chunk_ids))})",
        chunk_ids
    ).fetchall()
    doc_ids = [r[0] for r in doc_ids_rows]
    return fetch_docs_by_ids(doc_ids) if doc_ids else []
```

### 3.2 BM25 & Vector Index Singletons

```python
# core/services/retrieval.py  (continued)
import pickle, lancedb
import google.generativeai as genai
from functools import lru_cache
from rank_bm25 import BM25Okapi

class BM25Index:
    """Loaded once at Django startup from pickle."""
    def __init__(self, index_path: str):
        with open(index_path, "rb") as f:
            data = pickle.load(f)
        self.bm25     = data["bm25"]       # BM25Okapi instance
        self.chunk_ids = data["chunk_ids"]  # parallel list of chunk IDs

    def search(self, query: str, top_k: int = 20) -> list[str]:
        tokens = query.lower().split()
        scores = self.bm25.get_scores(tokens)
        top = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
        return [self.chunk_ids[i] for i in top]

class VectorIndex:
    """LanceDB-backed vector search. Loaded once at Django startup."""
    def __init__(self, lancedb_path: str):
        self.db = lancedb.connect(lancedb_path)
        self.table = self.db.open_table("chunks")

    @lru_cache(maxsize=500)
    def embed(self, text: str) -> tuple:
        result = genai.embed_content(
            model="models/gemini-embedding-exp-03-07",
            content=text,
            task_type="RETRIEVAL_QUERY",
        )
        return tuple(result["embedding"])

    def search(self, query: str, top_k: int = 20, filter_doc_ids: list[str] = None) -> list[str]:
        qvec = list(self.embed(query))
        q = self.table.search(qvec).limit(top_k)
        if filter_doc_ids:
            q = q.where(f"doc_id IN {tuple(filter_doc_ids)!r}")
        return [r["chunk_id"] for r in q.to_list()]

def reciprocal_rank_fusion(bm25_ids: list[str], vec_ids: list[str], k=60) -> list[str]:
    scores = {}
    for rank, cid in enumerate(bm25_ids):
        scores[cid] = scores.get(cid, 0) + 1.0 / (k + rank + 1)
    for rank, cid in enumerate(vec_ids):
        scores[cid] = scores.get(cid, 0) + 1.0 / (k + rank + 1)
    return sorted(scores, key=scores.get, reverse=True)[:20]

# ── Singleton instances (loaded once per Gunicorn worker) ──────────────────────
bm25_index   = BM25Index("data/bm25_index.pkl")
vector_index = VectorIndex("data/lancedb")
```

### 3.3 Building the BM25 Index (Offline Script)

```python
# scripts/build_bm25_index.py
"""Run once after bulk_ingest to build the BM25 pickle file."""
import pickle, duckdb
from rank_bm25 import BM25Okapi

conn = duckdb.connect("data/fia_analytics.duckdb", read_only=True)
rows = conn.execute("SELECT id, chunk_text FROM chunks ORDER BY bm25_id").fetchall()

chunk_ids = [r[0] for r in rows]
corpus    = [r[1].lower().split() for r in rows]

bm25 = BM25Okapi(corpus)
with open("data/bm25_index.pkl", "wb") as f:
    pickle.dump({"bm25": bm25, "chunk_ids": chunk_ids}, f)
print(f"BM25 index built: {len(chunk_ids)} chunks.")
```

---

## Layer 4: Query Rewriting & Routing

```python
# core/services/router.py
import google.generativeai as genai

REWRITE_PROMPT = """
Given conversation history and a follow-up question, rewrite the follow-up
as a complete standalone question. Return ONLY the rewritten question.

History:
{history}

Follow-up: {question}
Rewritten:"""

async def rewrite_query(session_id: str, current_query: str) -> str:
    from core.models import Message
    msgs = await Message.objects.filter(
        session_id=session_id).order_by('-created_at').values_list('role', 'content')[:5]
    if not msgs:
        return current_query
    history = "\n".join(f"{role.capitalize()}: {content}" for role, content in reversed(msgs))
    resp = await genai.GenerativeModel('gemini-2.5-flash-lite').generate_content_async(
        REWRITE_PROMPT.format(history=history, question=current_query)
    )
    return resp.text.strip()

def classify_query(query: str) -> str:
    q = query.lower()
    if any(kw in q for kw in ["how many", "count", "total", "most", "least"]): return "AGGREGATION"
    if any(kw in q for kw in ["compare", "vs", "versus", "difference"]): return "COMPARATIVE"
    if any(kw in q for kw in ["similar", "precedent", "find all", "list all"]): return "PRECEDENT"
    return "FACT_LOOKUP"

async def extract_query_entities(query: str) -> dict:
    """Fast entity extraction — extracts year, event, driver, car_number, doc_type."""
    import re
    entities = {}

    # Year: 4-digit number 2018-2026
    year_m = re.search(r'\b(201[89]|202[0-6])\b', query)
    if year_m:
        entities["year"] = int(year_m.group(1))

    # Car number
    car_m = re.search(r'\bcar\s+(\d{1,2})\b', query, re.IGNORECASE)
    if car_m:
        entities["car_number"] = car_m.group(1)

    # Doc type keywords
    if any(w in query.lower() for w in ["decision", "penalty", "penalised"]):
        entities["doc_type"] = "decision"
    elif any(w in query.lower() for w in ["summons", "summoned"]):
        entities["doc_type"] = "summons"
    elif "offence" in query.lower():
        entities["doc_type"] = "offence"

    # Known event names (partial match)
    known_events = [
        "bahrain", "saudi", "australia", "japan", "china", "miami",
        "monaco", "canada", "spain", "austria", "silverstone", "hungary",
        "belgium", "netherlands", "monza", "azerbaijan", "singapore",
        "austin", "mexico", "brazil", "las vegas", "qatar", "abu dhabi",
    ]
    for ev in known_events:
        if ev in query.lower():
            entities["event"] = ev
            break

    return entities
```

---

## Layer 5: Django Ninja API Layer

### 5.1 Schemas

```python
# core/schema.py
from ninja import Schema

class ChatMessageIn(Schema):
    session_id: str
    query: str

class ChatMessageOut(Schema):
    answer: str
    query_type: str
    sources: list[str] = []

class StreamChunk(Schema):
    token: str
    done: bool = False
    sources: list[str] = []
```

### 5.2 Chat Endpoint (Streaming SSE)

```python
# core/api.py
from django.http import HttpRequest
from ninja import NinjaAPI
from ninja.security import HttpBearer
from core.schema import ChatMessageIn, ChatMessageOut, StreamChunk
from core.services.router import rewrite_query, classify_query, extract_query_entities
from core.services.retrieval import (sql_first_search, fetch_docs_by_ids,
    build_context_from_chunks, vector_index, bm25_index, reciprocal_rank_fusion)
from core.models import Message, ChatSession, User
import google.generativeai as genai
import asyncio

api = NinjaAPI(auth=GlobalAuth())

@api.post("/chat/stream")
async def chat_stream(request: HttpRequest, payload: ChatMessageIn):
    enforce_quota(request)

    # 1. Rewrite for multi-turn
    rewritten   = await rewrite_query(payload.session_id, payload.query)
    query_type  = classify_query(rewritten)
    entities    = await extract_query_entities(rewritten)

    # 2. Retrieve context
    context_docs = []
    if query_type == "AGGREGATION":
        # Generate and execute DuckDB SQL
        sql_result = await generate_aggregation_sql(rewritten, entities)
        context_docs = [str(sql_result)]
    else:
        # Try SQL-first (structured filter)
        doc_ids = sql_first_search(entities)
        if doc_ids:
            context_docs = fetch_docs_by_ids(doc_ids)
        else:
            # Hybrid BM25 + Vector with RRF
            bm25_ids = bm25_index.search(rewritten)
            vec_ids  = vector_index.search(rewritten)
            chunk_ids = reciprocal_rank_fusion(bm25_ids, vec_ids)
            context_docs = build_context_from_chunks(chunk_ids)

    # 3. Stream answer from Gemini
    context_str = "\n\n---\n\n".join(context_docs[:5])
    prompt = f"FIA Documents:\n{context_str}\n\nQuestion: {rewritten}\nAnswer:"
    model  = genai.GenerativeModel('gemini-2.5-flash')

    async def event_gen():
        full = ""
        response = await model.generate_content_async(
            prompt, stream=True,
            generation_config={"max_output_tokens": 1500}
        )
        async for chunk in response:
            if chunk.text:
                full += chunk.text
                yield {"token": chunk.text, "done": False}

        # Persist to DB
        await Message.objects.acreate(session_id=payload.session_id, role="user",   content=payload.query)
        await Message.objects.acreate(session_id=payload.session_id, role="assistant", content=full)
        yield {"token": "", "done": True, "sources": []}

    return event_gen()
```

---

## Layer 6: Authentication & Quota Enforcement

```python
# core/api.py
from ninja.security import HttpBearer
from jose import jwt, JWTError
from django.conf import settings
from django.utils import timezone

class GlobalAuth(HttpBearer):
    def authenticate(self, request, token):
        try:
            payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("sub")
            if user_id:
                request.user = User.objects.get(id=user_id)
                return request.user
        except (JWTError, Exception):
            return None

class QuotaExceeded(Exception):
    pass

@api.exception_handler(QuotaExceeded)
def quota_exceeded_handler(request, exc):
    return api.create_response(request, {"detail": "Daily query quota exceeded"}, status=429)

def enforce_quota(request):
    user = request.user
    if user.quota_reset_date < timezone.now().date():
        user.queries_today = 0
        user.quota_reset_date = timezone.now().date()
        user.save(update_fields=["queries_today", "quota_reset_date"])
    if user.queries_today >= user.daily_quota:
        raise QuotaExceeded()
    user.queries_today += 1
    user.save(update_fields=["queries_today"])
```

---

## Layer 7: DuckDB Connection Management

```python
# core/services/duckdb.py
import duckdb, threading

_local = threading.local()

def get_duckdb_conn(read_only=True):
    if not hasattr(_local, 'conn') or _local.conn is None:
        _local.conn = duckdb.connect('data/fia_analytics.duckdb', read_only=read_only)
    return _local.conn

def close_duckdb_conn():
    if hasattr(_local, 'conn') and _local.conn:
        _local.conn.close()
        _local.conn = None

# core/apps.py
from django.apps import AppConfig

class CoreConfig(AppConfig):
    name = 'core'
    def ready(self):
        import atexit
        from core.services.duckdb import close_duckdb_conn
        atexit.register(close_duckdb_conn)
```

---

## Layer 8: Caching

```python
# fia_project/settings.py
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': '/var/www/fia_rag/data/cache/',
        'TIMEOUT': 3600,
    }
}

# core/services/retrieval.py
from django.core.cache import cache

def execute_aggregation(sql: str):
    key = f"agg_{hash(sql)}"
    result = cache.get(key)
    if result is None:
        conn = get_duckdb_conn(read_only=True)
        result = conn.execute(sql).fetchall()
        cache.set(key, result)
    return result
```

---

## VPS Memory Budget

| Component | RAM |
|---|---|
| Ubuntu OS + system | ~400MB |
| Nginx | ~30MB |
| **Gunicorn (1 Uvicorn worker)** | ~250MB |
| Django + SQLite | ~20MB |
| DuckDB (read-only) | ~150MB |
| **BM25 index (~50k chunks)** | ~200MB |
| LanceDB (disk + RAM cache) | ~100MB |
| Python runtime / buffers | ~200MB |
| **Total** | **~1.35GB** |
| **Free headroom** | **~2.65GB** |

**Why 1 Uvicorn worker?** The 200MB BM25 index is only loaded once. Django Ninja's full async
support means a single worker handles hundreds of concurrent Gemini API calls without blocking.

---

## Deployment Configuration

### Systemd Web Service

```ini
# /etc/systemd/system/fia_rag.service
[Unit]
Description=FIA RAG Django Application
After=network.target

[Service]
User=www-data
WorkingDirectory=/var/www/fia_rag
EnvironmentFile=/var/www/fia_rag/.env
ExecStart=/var/www/fia_rag/.venv/bin/gunicorn fia_project.wsgi:application \
    -k uvicorn.workers.UvicornWorker \
    --bind 127.0.0.1:8000 \
    --workers 1
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Systemd Ingestion Timer (polls for new extractions from GitHub)

```ini
# /etc/systemd/system/fia_ingest.service
[Service]
User=www-data
WorkingDirectory=/var/www/fia_rag
ExecStart=/var/www/fia_rag/.venv/bin/python manage.py process_ingestion_queue

# /etc/systemd/system/fia_ingest.timer
[Timer]
OnBootSec=5min
OnUnitActiveSec=30min   # Check every 30 min (GitHub Actions runs every 3h)

[Install]
WantedBy=timers.target
```

### Nginx

```nginx
server {
    listen 443 ssl;
    server_name yourdomain.com;
    ssl_certificate     /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;

    location /static/ { alias /var/www/fia_rag/staticfiles/; }

    location / {
        root /var/www/fia_rag/frontend/dist;
        try_files $uri $uri/ /index.html;
    }

    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 90s;
        # SSE-specific headers
        proxy_buffering off;
        proxy_cache off;
        proxy_set_header Connection '';
        proxy_http_version 1.1;
        chunked_transfer_encoding on;
    }
}
```

---

## Final Build Order

### Phase 0: Data Setup (This Machine) — Week 1

1. **Day 1:** Run `bulk_ingest.py --skip-llm --dry-run` to validate manifest → DuckDB mapping.
2. **Day 2:** Run `bulk_ingest.py --skip-llm` to ingest ALL docs (fast, no API calls). ~50k chunks.
3. **Day 3:** Run `build_bm25_index.py`. Test BM25 search from Python REPL.
4. **Day 4:** Build LanceDB vectors. Run `bulk_ingest.py` in batches **with** LLM for
   `decision/summons/offence` only (~5-10% of all docs). Throttle to 900/day.
5. **Day 5:** Validate DuckDB: query by year/event/driver/doc_type. Spot check 20 decisions.

### Phase 1: Django Core — Week 2

6. **Day 6:** Django project scaffold, models, migrations, SQLite.
7. **Day 7:** DuckDB singleton, BM25/Vector singletons. Test retrieval service in isolation.
8. **Day 8:** `sql_first_search` + `extract_query_entities`. Test on 20 structured queries.
9. **Day 9:** `rewrite_query` + `classify_query`. Full RAG chain (non-streaming).
10. **Day 10:** JWT auth, quota enforcement, Django Ninja setup.

### Phase 2: API & Frontend — Week 3

11. **Day 11:** SSE streaming endpoint `/api/chat/stream`. Test with `curl`.
12. **Day 12:** Ingestion management command (incremental sync from manifest.json).
13. **Day 13:** React frontend (Login, Chat UI, SSE token streaming, Citation drawer).
14. **Day 14:** File-based cache for aggregation queries. LRU cache for embeddings.

### Phase 3: Production — Week 4

15. **Day 15:** VPS provisioning. `rsync` DuckDB + LanceDB + BM25 pickle to VPS.
16. **Day 16:** Nginx + Gunicorn + Systemd services. Let's Encrypt SSL.
17. **Day 17:** Systemd ingestion timer. Test end-to-end with live Gemini API.
18. **Day 18:** Load test with `locust`. Monitor RAM with `htop` during BM25 load.
19. **Day 19:** Security audit (CORS, CSRF, Django security middleware, rate limiting).
20. **Day 20:** Go live. Monitor Gemini API quota and DuckDB RAM usage for 48h.
