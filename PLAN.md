# FIA Documents RAG — Full Implementation Plan (Django Edition)
## "Chat with the FIA Docs" — Production on a 4GB VPS

---

## Architecture Overview

Switching to Django provides a robust, batteries-included framework. We will use **Django Ninja** for async API endpoints, **Django ORM** (with SQLite) for web-state (Users, Sessions, Ingestion Queue), and **DuckDB** purely for the analytical FIA document retrieval.

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
     ├── Ingestion Queue      (Django ORM + SQLite → processed by Cron/Management Command)
     ├── API Layer            (Django Ninja - async)
     └── RAG Logic
          ├─── Layer 1: DuckDB (structured FIA data + chunks)
          ├─── Layer 2: BM25 index (rank_bm25) + LanceDB (chunk-level vectors)
          ├─── Layer 3: Query rewriter + classifier (SQL-first logic)
          └─── Layer 4: Gemini 2.5 Flash (final answer)
                             │
                    [Gemini API rate limiter — in-process]
```

**Why this stack for 4GB RAM?**
*   **No Redis/Celery:** We use Django's native ORM for the queue and a lightweight systemd `cron` trigger for ingestion. This saves ~300MB RAM.
*   **Django Ninja:** Provides FastAPI-like async views and Pydantic validation natively inside Django, crucial for non-blocking LLM calls.
*   **SQLite for Django, DuckDB for Analytics:** Django handles web traffic perfectly with SQLite. DuckDB handles the heavy analytical SQL queries (`AGGREGATION`) and chunk storage without locking issues.

---

## Directory Structure

```
fia_rag/
├── manage.py
├── fia_project/          # Django project config
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
├── core/                 # Main Django app
│   ├── models.py         # User, Session, Message, IngestionQueue
│   ├── api.py            # Django Ninja endpoints
│   ├── schema.py         # Pydantic schemas
│   ├── services/
│   │   ├── duckdb.py     # DuckDB connection & schema
│   │   ├── chunker.py    # Markdown header splitting
│   │   ├── extractor.py  # LLM extraction + JSON validation
│   │   ├── retrieval.py  # SQL-first, BM25, Vector, Context Builder
│   │   ├── router.py     # Query rewriter & classifier
│   │   └── answerer.py   # Gemini Flash answer generation
│   └── management/
│       └── commands/
│           └── process_ingestion_queue.py  # The cron-job replacement for Celery
├── data/
│   ├── fia_docs/         # Raw .md files
│   ├── fia_analytics.duckdb
│   ├── bm25_index/
│   └── lancedb/
├── frontend/             # Static SPA (React/Vite)
├── nginx/
│   └── fia_rag.conf
├── requirements.txt
└── systemd/
    ├── fia_rag.service   # Gunicorn service
    └── fia_ingest.timer  # Systemd timer for ingestion queue
```

---

## Layer 1: Data Models (Django SQLite + DuckDB)

### 1.1 Django Models (SQLite)
Used for web state, auth, and the ingestion queue.

```python
# core/models.py
from django.db import models
from django.contrib.auth.models import AbstractUser

class User(AbstractUser):
    daily_quota = models.IntegerField(default=20)
    queries_today = models.IntegerField(default=0)
    quota_reset_date = models.DateField(auto_now_add=True)

class ChatSession(models.Model):
    id = models.UUIDField(primary_key=True, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

class Message(models.Model):
    session = models.ForeignKey(ChatSession, on_delete=models.CASCADE)
    role = models.CharField(max_length=10) # 'user' or 'assistant'
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

class IngestionQueue(models.Model):
    STATUS_CHOICES = [('pending', 'Pending'), ('processing', 'Processing'), ('done', 'Done'), ('failed', 'Failed')]
    file_path = models.CharField(max_length=255)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

### 1.2 DuckDB Schema (Analytics & Retrieval)
Initialized via a startup script. Django queries this directly for RAG logic.

```sql
-- Documents table: Parent records
CREATE TABLE IF NOT EXISTS documents (
    id               VARCHAR PRIMARY KEY,
    file_path        VARCHAR NOT NULL,
    ingested_at      TIMESTAMP DEFAULT now(),
    raw_text         TEXT,
    event_name       VARCHAR, year INTEGER, round INTEGER, session VARCHAR,
    document_type    VARCHAR, driver_name VARCHAR, car_number VARCHAR, team VARCHAR,
    charge_description TEXT, charge_normalized VARCHAR, regulation_articles VARCHAR[],
    heard            BOOLEAN, decision VARCHAR, penalty_type VARCHAR,
    penalty_value    VARCHAR, penalty_unit VARCHAR, stewards VARCHAR[]
);

-- Chunks table: Granular retrieval units
CREATE TABLE IF NOT EXISTS chunks (
    id               VARCHAR PRIMARY KEY,
    doc_id           VARCHAR NOT NULL REFERENCES documents(id),
    chunk_index      INTEGER,
    header_path      VARCHAR,
    chunk_text       TEXT,
    embedding_id     VARCHAR,
    bm25_id          INTEGER
);
```

---

## Layer 2: Ingestion Pipeline (Cron/Management Command)

To avoid OOM errors and API rate limits on a 4GB VPS, we ditch Watchdog/async loops. Instead, a systemd timer triggers a Django management command every minute to process the queue safely.

### 2.1 Management Command

```python
# core/management/commands/process_ingestion_queue.py
import asyncio
from django.core.management.base import BaseCommand
from core.models import IngestionQueue
from core.services.extractor import extract_fields
from core.services.chunker import chunk_markdown
from core.services.duckdb import get_duckdb_conn
from core.services.retrieval import vector_index

class Command(BaseCommand):
    help = 'Processes pending FIA documents in the ingestion queue'

    def handle(self, *args, **options):
        pending = IngestionQueue.objects.filter(status='pending').first()
        if not pending:
            return

        pending.status = 'processing'
        pending.save()

        try:
            text = open(pending.file_path, 'r').read()

            # 1. Extract fields via LLM
            fields = asyncio.run(extract_fields(text))
            doc_id = insert_into_duckdb(fields, text)

            # 2. Chunk Document
            chunks = chunk_markdown(doc_id, text)
            insert_chunks_into_duckdb(chunks)

            # 3. Vectorize & Index
            for chunk in chunks:
                vector_index.add_chunk(chunk['id'], doc_id, chunk['chunk_index'], chunk['chunk_text'])
                asyncio.run(asyncio.sleep(0.5)) # Throttle Gemini API

            pending.status = 'done'
            pending.save()

        except Exception as e:
            pending.status = 'failed'
            pending.save()
            self.stderr.write(f"Failed: {e}")
```

### 2.2 Contextual Chunking Strategy

```python
# core/services/chunker.py
import re
from markdown_it import MarkdownIt

def chunk_markdown(doc_id: str, text: str, max_chunk_size: int = 1000, overlap: int = 200) -> list[dict]:
    """Splits document by markdown headers. If a section is too long,
    splits by token size with overlap."""
    md = MarkdownIt()
    tokens = md.parse(text)

    chunks = []
    current_header = "Introduction"
    current_text = ""
    chunk_index = 0

    for token in tokens:
        if token.type == 'heading_open':
            if len(current_text.strip()) > 50:
                chunks.append({
                    "doc_id": doc_id, "chunk_index": chunk_index,
                    "header_path": current_header, "chunk_text": current_text.strip()
                })
                chunk_index += 1
            current_header = token.map # Simplification
            current_text = ""
        elif token.type == 'inline':
            current_text += token.content + " "

        if len(current_text) > max_chunk_size:
            chunks.append({
                "doc_id": doc_id, "chunk_index": chunk_index,
                "header_path": current_header, "chunk_text": current_text[:max_chunk_size].strip()
            })
            chunk_index += 1
            current_text = current_text[max_chunk_size-overlap:]

    if len(current_text.strip()) > 50:
        chunks.append({
            "doc_id": doc_id, "chunk_index": chunk_index,
            "header_path": current_header, "chunk_text": current_text.strip()
        })
    return chunks
```

---

## Layer 3: Retrieval Logic (SQL-First + Hybrid)

### 3.1 SQL-First Retrieval

```python
# core/services/retrieval.py
from core.services.duckdb import get_duckdb_conn

def sql_first_search(query_entities: dict) -> list[str] | None:
    """Builds a SQL query based on extracted entities. Returns doc_ids if found."""
    conn = get_duckdb_conn()
    conditions = []
    if query_entities.get("year"): conditions.append(f"year = {query_entities['year']}")
    if query_entities.get("driver_name"): conditions.append(f"driver_name ILIKE '%{query_entities['driver_name']}%'")
    if query_entities.get("event_name"): conditions.append(f"event_name ILIKE '%{query_entities['event_name']}%'")

    if not conditions: return None

    where_clause = " AND ".join(conditions)
    results = conn.execute(f"SELECT id FROM documents WHERE {where_clause} LIMIT 5").fetchall()
    return [r[0] for r in results] if results else None

def build_context(chunk_ids: list[str]) -> list[str]:
    """Takes chunk IDs, fetches their parent documents."""
    if not chunk_ids: return []
    conn = get_duckdb_conn()
    doc_ids = [r[0] for r in conn.execute(f"SELECT DISTINCT doc_id FROM chunks WHERE id IN {tuple(chunk_ids)}").fetchall()]
    docs = [r[0] for r in conn.execute(f"SELECT raw_text FROM documents WHERE id IN {tuple(doc_ids)}").fetchall()]
    return docs
```

### 3.2 BM25 & Vector Indexes (Singletons)

We load these into memory once when Django starts.

```python
# core/services/retrieval.py
import lancedb
import rank_bm25
import google.generativeai as genai

class VectorIndex:
    def __init__(self):
        self.db = lancedb.connect("data/lancedb")
        # ... initialization logic ...

    def embed(self, text: str, task_type: str) -> list[float]:
        result = genai.embed_content(
            model="models/gemini-embedding-2-preview", # User's requested model
            content=text,
            task_type=task_type,
        )
        return result["embedding"]

    def search(self, query: str, top_k: int = 20, filter_doc_ids: list[str] = None):
        query_vec = self.embed(query, "RETRIEVAL_QUERY")
        results = self.db.open_table("chunks").search(query_vec).limit(top_k)
        if filter_doc_ids:
            results = results.where(f"doc_id IN {tuple(filter_doc_ids)}")
        return [(r["doc_id"], r["chunk_id"]) for r in results.to_list()]

# Instantiate once per Django process
vector_index = VectorIndex()
bm25_index = ... # Load from pickle file
```

---

## Layer 4: Query Rewriting & Routing

### 4.1 Multi-Turn Query Rewriting

```python
# core/services/router.py
import google.generativeai as genai

REWRITE_PROMPT = """
Given the following conversation history and a follow-up question,
rewrite the follow-up question to be a standalone question.
Return ONLY the rewritten question.

History:
{history}

Follow-up Question: {question}
"""

async def rewrite_query(session_id: str, current_query: str) -> str:
    from core.models import Message
    msgs = Message.objects.filter(session_id=session_id).order_by('-created_at')[:5]
    if not msgs: return current_query

    history_str = "\n".join([f"{m.role.capitalize()}: {m.content}" for m in reversed(msgs)])
    response = await genai.GenerativeModel('gemini-2.5-flash-lite').generate_content_async(
        REWRITE_PROMPT.format(history=history_str, question=current_query)
    )
    return response.text.strip()

def classify_query(query: str) -> str:
    q = query.lower()
    if any(kw in q for kw in ["how many", "count", "total"]): return "AGGREGATION"
    if any(kw in q for kw in ["compare", "vs", "versus"]): return "COMPARATIVE"
    if any(kw in q for kw in ["similar", "precedent", "find all"]): return "PRECEDENT"
    return "FACT_LOOKUP"
```

---

## Layer 5: Django Ninja API Layer

### 5.1 Schemas & Endpoints

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
```

```python
# core/api.py
from django.http import HttpRequest
from ninja import NinjaAPI
from core.schema import ChatMessageIn, ChatMessageOut
from core.services.router import rewrite_query, classify_query
from core.services.retrieval import sql_first_search, build_context, vector_index, bm25_index
from core.services.answerer import generate_answer
from core.models import Message, ChatSession
import asyncio

api = NinjaAPI()

@api.post("/chat", response=ChatMessageOut)
async def chat_endpoint(request: HttpRequest, payload: ChatMessageIn):
    # 1. Rewrite Query for multi-turn
    rewritten = await rewrite_query(payload.session_id, payload.query)

    # 2. Classify
    query_type = classify_query(rewritten)

    # 3. Retrieve / Process
    context_docs = []
    if query_type == "AGGREGATION":
        # Logic to generate DuckDB SQL and execute
        pass
    elif query_type == "FACT_LOOKUP":
        # Try SQL First
        doc_ids = sql_first_search({"year": 2024, "driver_name": "Verstappen"}) # Simplified entity extraction
        if doc_ids:
            context_docs = fetch_docs_by_ids(doc_ids)
        else:
            # Fallback to Hybrid RRF Search
            bm25_res = bm25_index.search(rewritten)
            vec_res = vector_index.search(rewritten)
            chunk_ids = reciprocal_rank_fusion(bm25_res, vec_res)
            context_docs = build_context(chunk_ids)

    # 4. Generate Answer
    answer = await generate_answer(rewritten, context_docs, query_type)

    # 5. Save to DB
    Message.objects.create(session_id=payload.session_id, role="user", content=payload.query)
    Message.objects.create(session_id=payload.session_id, role="assistant", content=answer)

    return ChatMessageOut(answer=answer, query_type=query_type, sources=context_docs[:2])
```

---

## VPS Memory Budget & Workers (Critical Optimization)

| Component | RAM Usage |
|---|---|
| Ubuntu OS + system | ~400MB |
| Nginx | ~30MB |
| **Gunicorn (1 Uvicorn worker)** | ~250MB |
| Django SQLite (In-memory cache) | ~20MB |
| DuckDB (Read-only analytics connection) | ~100MB |
| **BM25 index (10k chunks ~ 150MB)** | ~150MB |
| LanceDB (disk-based, RAM cache) | ~100MB |
| Python runtime / Buffers | ~200MB |
| **Total** | **~1.25GB** |
| **Free headroom** | **~2.75GB** |

**Why 1 Uvicorn Worker?**
Running `gunicorn fia_project.wsgi:application -k uvicorn.workers.UvicornWorker --workers 1` ensures that the 150MB BM25 index is only loaded into RAM once. Because Django Ninja is fully `async`, a single worker can handle hundreds of concurrent I/O requests (waiting on Gemini API) without blocking.

---

## Deployment Configuration

### 7.1 Systemd Web Service

```ini
# /etc/systemd/system/fia_rag.service
[Unit]
Description=FIA RAG Django Application
After=network.target

[Service]
User=www-data
WorkingDirectory=/var/www/fia_rag
EnvironmentFile=/var/www/fia_rag/.env
# 1 worker is strictly enforced to preserve RAM and duplicate BM25 indexes
ExecStart=/var/www/fia_rag/venv/bin/gunicorn fia_project.wsgi:application \
    -k uvicorn.workers.UvicornWorker \
    --bind 127.0.0.1:8000 \
    --workers 1
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 7.2 Systemd Ingestion Timer (Replaces Celery)

```ini
# /etc/systemd/system/fia_ingest.service
[Unit]
Description=FIA Ingestion Queue Processor

[Service]
User=www-data
WorkingDirectory=/var/www/fia_rag
ExecStart=/var/www/fia_rag/venv/bin/python manage.py process_ingestion_queue
```

```ini
# /etc/systemd/system/fia_ingest.timer
[Unit]
Description=Run FIA Ingestion Processor every minute

[Timer]
OnBootSec=1min
OnUnitActiveSec=1min

[Install]
WantedBy=timers.target
```

Enable the timer: `sudo systemctl enable fia_ingest.timer`

### 7.3 Nginx Configuration

```nginx
server {
    listen 443 ssl;
    server_name yourdomain.com;

    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;

    # Serve Django static files + React Frontend
    location /static/ {
        alias /var/www/fia_rag/staticfiles/;
    }

    location / {
        root /var/www/fia_rag/frontend/dist;
        try_files $uri $uri/ /index.html;
    }

    # Proxy API
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 60s;  # LLM calls can be slow
    }
}
```

---

## Layer 6: Authentication, Quotas & API Security

Since you are exposing this to the public with a login, we need robust quota enforcement to prevent a single user from burning your 250 RPD (Requests Per Day) Gemini free tier limit.

### 6.1 JWT Authentication in Django Ninja

```python
# core/api.py
from ninja.security import HttpBearer
from jose import jwt, JWTError
from django.conf import settings
from django.contrib.auth import authenticate

class GlobalAuth(HttpBearer):
    def authenticate(self, request, token):
        try:
            payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("sub")
            if user_id:
                from core.models import User
                request.user = User.objects.get(id=user_id)
                return request.user
        except (JWTError, User.DoesNotExist):
            return None

# Unprotected endpoints (login/register)
api_unauth = NinjaAPI(urls_prefix="/api/auth")

# Protected endpoints (chat/admin)
api = NinjaAPI(urls_prefix="/api", auth=GlobalAuth())
```

### 6.2 Per-User Quota Enforcement Dependency

```python
# core/api.py
from ninja import Schema
from django.utils import timezone

class QuotaExceeded(Exception):
    pass

@api.exception_handler(QuotaExceeded)
def quota_exceeded(request, exc):
    return api.create_response(request, {"detail": "Daily query quota exceeded"}, status_code=429)

def enforce_quota(request):
    user = request.user
    # Reset quota if it's a new day
    if user.quota_reset_date < timezone.now().date():
        user.queries_today = 0
        user.quota_reset_date = timezone.now().date()
        user.save()

    if user.queries_today >= user.daily_quota:
        raise QuotaExceeded()

    # Increment quota
    user.queries_today += 1
    user.save()

@api.post("/chat")
async def chat_endpoint(request, payload: ChatMessageIn):
    enforce_quota(request) # Check quota before hitting Gemini
    # ... rest of the logic ...
```

---

## Layer 7: DuckDB Concurrency & Connection Management

DuckDB is an embedded database. In Django, the web process (Gunicorn) and the background ingestion process (Management Command) run simultaneously. DuckDB handles concurrent reads perfectly, but only **one process can write at a time**.

### 7.1 Connection Singleton with Read-Only Mode

The web server should only *read* from DuckDB. The ingestion command will *write*.

```python
# core/services/duckdb.py
import duckdb
import threading

_local = threading.local()

def get_duckdb_conn(read_only=True):
    """Provides a thread-local DuckDB connection."""
    if not hasattr(_local, 'duckdb_conn') or _local.duckdb_conn is None:
        # Connect to the shared file
        _local.duckdb_conn = duckdb.connect('data/fia_analytics.duckdb', read_only=read_only)
    return _local.duckdb_conn

# Close connection when Django process finishes
def close_duckdb_conn():
    if hasattr(_local, 'duckdb_conn') and _local.duckdb_conn:
        _local.duckdb_conn.close()
        _local.duckdb_conn = None
```

In Django's `AppConfig.ready()`, register the cleanup:
```python
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

## Layer 8: Streaming API (Server-Sent Events)

Users hate waiting 10 seconds staring at a blank screen while Gemini generates 1,000 tokens. We must stream the response. Django Ninja supports async generators for Server-Sent Events (SSE), which are much lighter on a 4GB VPS than WebSockets (no Redis/Daphne required).

### 8.1 Streaming Schema & Endpoint

```python
# core/schema.py
from ninja import Schema

class StreamChunk(Schema):
    token: str
    done: bool = False
    sources: list[str] = []

# core/api.py
import asyncio
import google.generativeai as genai

@api.post("/chat/stream")
async def chat_stream_endpoint(request, payload: ChatMessageIn):
    enforce_quota(request)

    # 1. Retrieve context (same as before)
    rewritten = await rewrite_query(payload.session_id, payload.query)
    query_type = classify_query(rewritten)
    context_docs = ["Simulated doc 1", "Simulated doc 2"] # Retrieve properly here

    # 2. Setup Gemini Streaming
    model = genai.GenerativeModel('gemini-2.5-flash')
    prompt = f"Documents:\n{chr(10).join(context_docs)}\n\nQuestion: {rewritten}"

    # 3. Async Generator for SSE
    async def event_generator():
        response = await model.generate_content_async(
            prompt,
            stream=True,
            generation_config={"max_output_tokens": 1500}
        )

        full_answer = ""
        async for chunk in response:
            if chunk.text:
                full_answer += chunk.text
                yield {"token": chunk.text, "done": False}

        # Save complete answer to DB after streaming finishes
        Message.objects.create(session_id=payload.session_id, role="assistant", content=full_answer)

        # Send final signal with sources
        yield {"token": "", "done": True, "sources": ["doc1.md", "doc2.md"]}

    return event_generator()
```

*Note: The frontend will consume this using `EventSource` or `fetch` with a readable stream.*

---

## Layer 9: Caching Strategy (RAM vs. Disk)

On a 4GB VPS, we cannot use Redis. We will use Python's in-memory LRU for high-frequency data, and Django's file-based cache for SQL query results.

### 9.1 Embedding LRU Cache (In-Memory)

Cache query embeddings to save API calls if users ask similar questions.

```python
# core/services/retrieval.py
from functools import lru_cache

class VectorIndex:
    # ...

    @lru_cache(maxsize=500) # Cache last 500 query embeddings (~2MB RAM)
    def get_query_vector(self, query: str):
        return self.embed(query, "RETRIEVAL_QUERY")

    def search(self, query: str, top_k: int = 20):
        query_vec = self.get_query_vector(query)
        # ... search logic ...
```

### 9.2 Aggregation SQL Cache (File-Based)

Django supports file-based caching out of the box, which is perfect for 4GB VPS.

```python
# fia_project/settings.py
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': '/var/www/fia_rag/data/cache/',
        'TIMEOUT': 3600, # 1 hour cache for aggregation stats
    }
}

# core/services/router.py
from django.core.cache import cache

def execute_aggregation_query(query: str, sql: str):
    cache_key = f"agg_{hash(sql)}"
    result = cache.get(cache_key)

    if result is None:
        conn = get_duckdb_conn(read_only=True)
        result = conn.execute(sql).fetchall()
        cache.set(cache_key, result)

    return result
```

---

## Initial Bulk Load Strategy (The 500MB Problem)

You have 500MB of existing documents. You cannot drop them all in the folder at once, or the Gemini API will rate-limit you into the ground (1,000 RPD limit).

### The "Cold Start" Script

Create a standalone script that runs locally (or on the VPS during setup) that slowly ingests the files over a week, *before* the website goes live.

```python
# scripts/bulk_ingest.py
import os
import time
import duckdb
import google.generativeai as genai

FIA_DOCS_DIR = "./data/fia_docs"
BATCH_SIZE = 900 # Stay under 1000 RPD
DELAY_BETWEEN_CALLS = 1.5 # 60s / 40 calls = 1.5s delay to stay under 40 RPM

def run():
    # 1. Find all .md files
    files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(FIA_DOCS_DIR) for f in filenames if f.endswith('.md')]

    # 2. Find which ones are already in DuckDB
    conn = duckdb.connect('data/fia_analytics.duckdb', read_only=False)
    ingested = set(r[0] for r in conn.execute("SELECT file_path FROM documents").fetchall())

    to_process = [f for f in files if f not in ingested]
    print(f"Found {len(to_process)} documents left to ingest.")

    # 3. Process in daily batches
    processed_today = 0
    for file_path in to_process:
        if processed_today >= BATCH_SIZE:
            print("Daily limit reached. Sleeping until tomorrow...")
            time.sleep(86400) # Sleep 24 hours
            processed_today = 0

        text = open(file_path, 'r').read()
        # ... Extraction, Chunking, Embedding logic ...

        processed_today += 1
        time.sleep(DELAY_BETWEEN_CALLS)

if __name__ == "__main__":
    run()
```

---

## Final Build Order & Deployment Checklist

### Week 1: Core RAG Engine
1.  **Day 1:** Setup Django project, DuckDB schema, Django models (User, Session, Queue).
2.  **Day 2:** Build `chunker.py` and `extractor.py`. Test LLM extraction with strict JSON forcing.
3.  **Day 3:** Implement `VectorIndex` (LanceDB + Gemini 2) and `BM25Index`. Test chunk-level upserts.
4.  **Day 4:** Build `sql_first_search` and `reciprocal_rank_fusion`. Test retrieval quality.
5.  **Day 5:** Implement `rewrite_query` and `classify_query`.

### Week 2: API & Web Integration
6.  **Day 6:** Setup Django Ninja, JWT Auth, and the `/api/chat/stream` SSE endpoint.
7.  **Day 7:** Build the Ingestion Management Command and test the file-based queue.
8.  **Day 8:** Implement File-based caching for aggregations and LRU cache for embeddings.
9.  **Day 9:** Connect React frontend (Login, Chat UI with SSE consumption, Citation drawer).
10. **Day 10:** Nginx configuration, Gunicorn setup (1 worker), Systemd Timers for ingestion.

### Week 3: Production Polish
11. **Day 11:** Run the `bulk_ingest.py` script. Let it run over the next 5-7 days.
12. **Day 12:** Load testing with `locust`. Ensure 1 Gunicorn worker handles 50 concurrent users waiting on SSE streams.
13. **Day 13:** Security audit (CORS, CSRF, Django security middleware).
14. **Day 14:** Go live. Monitor DuckDB RAM usage and Gemini API quotas closely.
