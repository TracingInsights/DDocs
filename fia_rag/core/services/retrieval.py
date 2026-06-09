from core.services.duckdb import get_duckdb_conn
import pickle
import lancedb
from google import genai
from google.genai import types
from functools import lru_cache
from django.conf import settings

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

class BM25Index:
    """Loaded once at Django startup from pickle."""
    def __init__(self, index_path: str):
        try:
            with open(index_path, "rb") as f:
                data = pickle.load(f)
            self.bm25     = data["bm25"]       # BM25Okapi instance
            self.chunk_ids = data["chunk_ids"]  # parallel list of chunk IDs
            self.loaded = True
        except FileNotFoundError:
            self.bm25 = None
            self.chunk_ids = []
            self.loaded = False

    def search(self, query: str, top_k: int = 20) -> list[str]:
        if not self.loaded:
            return []
        tokens = query.lower().split()
        scores = self.bm25.get_scores(tokens)
        top = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
        return [self.chunk_ids[i] for i in top]

class VectorIndex:
    """LanceDB-backed vector search. Loaded once at Django startup."""
    def __init__(self, lancedb_path: str):
        try:
            self.db = lancedb.connect(lancedb_path)
            self.table = self.db.open_table("chunks")
            self.loaded = True
        except Exception:
            self.db = None
            self.table = None
            self.loaded = False

    @lru_cache(maxsize=500)
    def embed(self, text: str) -> tuple:
        client = genai.Client()
        result = client.models.embed_content(
            model="gemini-embedding-2-preview",
            contents=text,
            config=types.EmbedContentConfig(task_type="RETRIEVAL_QUERY"),
        )
        return tuple(result.embeddings[0].values)

    def search(self, query: str, top_k: int = 20, filter_doc_ids: list[str] = None) -> list[str]:
        if not self.loaded:
            return []
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

# Singletons (initialized lazily or at module load depending on file existence)
bm25_index   = BM25Index(str(settings.BASE_DIR / "data" / "bm25_index.pkl"))
vector_index = VectorIndex(str(settings.BASE_DIR / "data" / "lancedb"))
