import duckdb
import lancedb
from google import genai
from google.genai import types
from pathlib import Path
import time
from tqdm import tqdm
import os

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DUCKDB_PATH = BASE_DIR / "fia_rag" / "data" / "fia_analytics.duckdb"
LANCEDB_PATH = BASE_DIR / "fia_rag" / "data" / "lancedb"

# Gemini API Limits: 30k TPM (Tokens Per Minute), 100 RPM (Requests Per Minute), 1000 RPD (Requests Per Day).
#
# With a batch_size of 100 chunks (~300 tokens each), we consume ~30k tokens per request.
# Therefore, the 30k TPM limit is our strict bottleneck (not the 100 RPM).
# We must set DELAY_SECONDS to 65 to ensure we only do one batch per minute.
# This will take ~727 requests total, which fits within the 1000 RPD limit!
# At 1 batch per 65 seconds, the entire dataset will process in ~13 hours.
DELAY_SECONDS = 60.5

def get_embeddings_with_retry(client, texts, max_retries=10, base_delay=30):
    """
    Embed a list of texts, returning one embedding vector per text.

    IMPORTANT: Must pass list[Content] not list[str] to embed_content.
    Passing list[str] causes the API to treat them as one multi-part document
    and return only 1 embedding. list[Content] returns one embedding per doc.
    """
    # Wrap each text in a Content object so the API treats them as separate documents
    contents = [
        types.Content(parts=[types.Part(text=t)])
        for t in texts
    ]
    delay = base_delay
    for attempt in range(max_retries):
        try:
            result = client.models.embed_content(
                model="gemini-embedding-2-preview",
                contents=contents,
                config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
            )
            embeddings = [emb.values for emb in result.embeddings]
            if len(embeddings) != len(texts):
                raise ValueError(
                    f"API returned {len(embeddings)} embeddings for {len(texts)} texts. "
                    "Batch embedding mismatch — check contents format."
                )
            return embeddings
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "quota" in err_str.lower() or "resource_exhausted" in err_str.lower():
                if attempt == max_retries - 1:
                    raise e
                print(f"Rate limited (TPM/RPM exceeded). Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                raise e

def run():
    if not DUCKDB_PATH.exists():
        print("DuckDB not found. Run bulk_ingest.py first.")
        return

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    rows = conn.execute("SELECT id, doc_id, chunk_text FROM chunks").fetchall()
    conn.close()

    if not rows:
        print("No chunks found in DuckDB.")
        return

    print(f"Found {len(rows)} total chunks in DuckDB.")
    db = lancedb.connect(str(LANCEDB_PATH))
    client = genai.Client(api_key='AIzaSyAUSNquY_UurJSu5fnM89wlhhNEvq8ZZd8')

    import pyarrow as pa
    schema = pa.schema([
        pa.field("vector", pa.list_(pa.float32(), 3072)),
        pa.field("chunk_id", pa.string()),
        pa.field("doc_id", pa.string())
    ])

    existing_ids = set()

    # Step 1: Open or create the table
    try:
        table = db.open_table("chunks")
        table_exists = True
        print("Found existing LanceDB table. Resuming from previous progress...")
    except Exception as e:
        print(f"Creating new LanceDB table... (Info: {e})")
        table = db.create_table("chunks", schema=schema)
        table_exists = False

    # Step 2: If table existed, read existing chunk IDs (column-only scan, no vectors)
    if table_exists:
        try:
            lance_ds = table.to_lance()
            existing_ids = set(lance_ds.to_table(columns=["chunk_id"])["chunk_id"].to_pylist())
        except Exception as e:
            print(f"Warning: efficient ID scan failed, falling back to full scan: {e}")
            existing_ids = set(table.to_arrow().column("chunk_id").to_pylist())

    # Filter out rows that are already embedded
    rows_to_process = [r for r in rows if r[0] not in existing_ids]
    print(f"{len(existing_ids)} chunks already embedded. {len(rows_to_process)} remaining.")

    if not rows_to_process:
        print("All chunks have been embedded successfully!")
        return

    batch_size = 100
    with tqdm(total=len(rows_to_process)) as pbar:
        for i in range(0, len(rows_to_process), batch_size):
            batch = rows_to_process[i:i+batch_size]
            valid_batch = [r for r in batch if r[2] is not None and str(r[2]).strip() != ""]
            if not valid_batch:
                pbar.update(len(batch))
                continue

            texts = [r[2] for r in valid_batch]
            chunk_ids = [r[0] for r in valid_batch]
            doc_ids = [r[1] for r in valid_batch]

            try:
                embeddings = get_embeddings_with_retry(client, texts)

                data = [
                    {"vector": emb, "chunk_id": cid, "doc_id": did}
                    for emb, cid, did in zip(embeddings, chunk_ids, doc_ids)
                ]
                table.add(data)
                pbar.update(len(batch))

            except Exception as e:
                print(f"Embedding failed for batch {i}: {e}")
                print("Stopping index build due to persistent errors.")
                break

            time.sleep(DELAY_SECONDS)

    print("LanceDB index built successfully.")

if __name__ == "__main__":
    run()
