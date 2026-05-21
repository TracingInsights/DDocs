Here is the continuation of the plan, covering Authentication, DuckDB Concurrency, Streaming Responses, Caching, and the final build order.

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
