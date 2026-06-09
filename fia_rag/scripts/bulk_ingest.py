"""
Walks extracted/manifest.json, finds all successful extractions not yet in DuckDB,
and ingests them in batches — respecting Gemini API rate limits.

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

BASE_DIR = Path(__file__).resolve().parent.parent.parent
EXTRACTED_ROOT = BASE_DIR / "extracted"
MANIFEST_PATH  = EXTRACTED_ROOT / "manifest.json"
DUCKDB_PATH    = BASE_DIR / "fia_rag" / "data" / "fia_analytics.duckdb"

# Gemini free tier: 1000 RPD, ~15 RPM
BATCH_SIZE       = 900
DELAY_SECONDS    = 1.5

RICH_DOC_TYPES = {"decision", "summons", "offence"}

def parse_doc_path(key: str) -> dict:
    parts = Path(key).parts
    return {"year": int(parts[1]), "event": parts[2],
            "pdf_name": parts[3], "pdf_stem": Path(parts[3]).stem}

def chunk_markdown(doc_id: str, text: str, max_size=1200, overlap=200) -> list[dict]:
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
    from google import genai
    from google.genai import types
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

    client = genai.Client()
    response = await client.aio.models.generate_content(
        model='gemini-2.5-flash-lite',
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json", 
            max_output_tokens=512
        )
    )
    try:
        return json.loads(response.text)
    except Exception:
        return {}

def run(year_filter=None, event_filter=None, dry_run=False, skip_llm=False):
    # Ensure data dir exists
    DUCKDB_PATH.parent.mkdir(exist_ok=True, parents=True)

    if not MANIFEST_PATH.exists():
        print(f"Manifest not found at {MANIFEST_PATH}. Run from fia_rag root.")
        return

    manifest = json.loads(MANIFEST_PATH.read_text())
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=False)

    # Initialize schema if missing
    conn.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id VARCHAR PRIMARY KEY, manifest_key VARCHAR NOT NULL, md_path VARCHAR NOT NULL,
        year INTEGER, event VARCHAR, pdf_stem VARCHAR, doc_type VARCHAR, pages INTEGER,
        tables_extracted INTEGER, extracted_at TIMESTAMP, driver_name VARCHAR, car_number VARCHAR,
        team VARCHAR, charge_description TEXT, charge_normalized VARCHAR, regulation_articles VARCHAR[],
        heard BOOLEAN, decision VARCHAR, penalty_type VARCHAR, penalty_value VARCHAR,
        penalty_unit VARCHAR, stewards VARCHAR[], ingested_at TIMESTAMP DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS chunks (
        id VARCHAR PRIMARY KEY, doc_id VARCHAR NOT NULL REFERENCES documents(id),
        chunk_index INTEGER, header_path VARCHAR, chunk_text TEXT, bm25_id INTEGER
    );
    """)

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

        llm_fields = {}
        if not skip_llm and doc_type in RICH_DOC_TYPES:
            if llm_calls_today >= BATCH_SIZE:
                print("Daily LLM limit reached. Sleep 24h...")
                time.sleep(86400)
                llm_calls_today = 0
            llm_fields = asyncio.run(llm_extract_fields(md_text))
            llm_calls_today += 1
            time.sleep(DELAY_SECONDS)

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
