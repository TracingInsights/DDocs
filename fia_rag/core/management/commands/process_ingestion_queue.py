import json, asyncio
from pathlib import Path
from django.core.management.base import BaseCommand
from core.services.duckdb import get_duckdb_conn

EXTRACTED_ROOT = Path("../extracted")
MANIFEST_PATH  = EXTRACTED_ROOT / "manifest.json"
RICH_DOC_TYPES = {"decision", "summons", "offence"}

class Command(BaseCommand):
    help = 'Syncs newly extracted documents from extracted/manifest.json into DuckDB'

    def handle(self, *args, **options):
        if not MANIFEST_PATH.exists():
            self.stderr.write(f"Manifest not found at {MANIFEST_PATH}.")
            return

        manifest = json.loads(MANIFEST_PATH.read_text())
        conn = get_duckdb_conn(read_only=False)
        
        try:
            ingested = set(r[0] for r in conn.execute("SELECT id FROM documents").fetchall())
        except Exception:
            self.stderr.write("DuckDB schema not initialized. Run bulk_ingest.py first.")
            return

        new_docs = {k: v for k, v in manifest.items()
                    if v.get("success") and v["source_hash"] not in ingested}

        if not new_docs:
            self.stdout.write("No new documents.")
            return

        self.stdout.write(f"Processing {len(new_docs)} new documents...")
        
        import sys
        sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent))
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

            self.stdout.write(f"  Ingested: {key}")
