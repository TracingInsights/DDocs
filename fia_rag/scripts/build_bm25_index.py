"""Run once after bulk_ingest to build the BM25 pickle file."""
import pickle, duckdb
from rank_bm25 import BM25Okapi
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DUCKDB_PATH = BASE_DIR / "fia_rag" / "data" / "fia_analytics.duckdb"
PICKLE_PATH = BASE_DIR / "fia_rag" / "data" / "bm25_index.pkl"

if not DUCKDB_PATH.exists():
    print("DuckDB not found. Run bulk_ingest.py first.")
    exit(1)

conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
rows = conn.execute("SELECT id, chunk_text FROM chunks ORDER BY id").fetchall()

chunk_ids = [r[0] for r in rows]
corpus    = [r[1].lower().split() for r in rows]

bm25 = BM25Okapi(corpus)
with open(PICKLE_PATH, "wb") as f:
    pickle.dump({"bm25": bm25, "chunk_ids": chunk_ids}, f)
print(f"BM25 index built: {len(chunk_ids)} chunks saved to {PICKLE_PATH}.")
