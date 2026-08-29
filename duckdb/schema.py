"""Shared DDL for the DuckDB build pipeline (single source of truth).

Per PLAN.md §4: markdown tables carry the full document text plus denormalized
metadata; JSON tables carry raw sidecar JSON (``data``) instead of content.
Everything is core SQL types (no FTS / vector / spatial) so the files work
under DuckDB-WASM without custom extensions.
"""

from __future__ import annotations

from config import TABLE  # script mode: duckdb/ is on sys.path[0]

#: (column, type) pairs for the markdown per-year tables (PLAN.md §4.1).
MARKDOWN_COLUMNS: list[tuple[str, str]] = [
    ("manifest_key", "VARCHAR PRIMARY KEY"),
    ("source_hash", "VARCHAR"),
    ("year", "SMALLINT"),
    ("event", "VARCHAR"),
    ("pdf_stem", "VARCHAR"),
    ("doc_type", "VARCHAR"),
    ("title", "VARCHAR"),
    ("date", "DATE"),
    ("doc_number", "VARCHAR"),
    ("time", "VARCHAR"),
    ("event_name", "VARCHAR"),
    ("sender", "VARCHAR"),  # sidecar `from` (SQL keyword, renamed)
    ("recipient", "VARCHAR"),  # sidecar `to`
    ("pages", "INTEGER"),
    ("tables_extracted", "INTEGER"),
    ("images_extracted", "INTEGER"),
    ("extracted_at", "TIMESTAMPTZ"),
    ("content", "TEXT"),
    ("char_len", "INTEGER"),
    ("ingested_at", "TIMESTAMP"),
]

#: (column, type) pairs for the JSON per-year tables + json_all
#: (PLAN.md §4.2): same metadata, raw sidecar in `data`, no content/char_len.
JSON_COLUMNS: list[tuple[str, str]] = [
    (name, typ) for name, typ in MARKDOWN_COLUMNS if name not in ("content", "char_len")
] + [("data", "JSON")]


def _ddl(columns: list[tuple[str, str]]) -> str:
    body = ",\n  ".join(f"  {name} {typ}" for name, typ in columns)
    return f"CREATE TABLE IF NOT EXISTS {TABLE} (\n{body}\n);"


DDL_MARKDOWN = _ddl(MARKDOWN_COLUMNS)
DDL_JSON = _ddl(JSON_COLUMNS)

#: Persisted in the file and used automatically by DuckDB-WASM reads.
#: The source_hash index serves the incremental upsert diff.
INDEXES = f"""
CREATE INDEX IF NOT EXISTS idx_docs_event    ON {TABLE}(event);
CREATE INDEX IF NOT EXISTS idx_docs_doc_type ON {TABLE}(doc_type);
CREATE INDEX IF NOT EXISTS idx_docs_date     ON {TABLE}(date);
CREATE INDEX IF NOT EXISTS idx_docs_hash     ON {TABLE}(source_hash);
"""

#: Columns written by the builders (ingested_at gets its now() default).
MARKDOWN_WRITE_COLUMNS = [name for name, _ in MARKDOWN_COLUMNS if name != "ingested_at"]
JSON_WRITE_COLUMNS = [name for name, _ in JSON_COLUMNS if name != "ingested_at"]
