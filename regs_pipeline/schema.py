"""Shared schema for the regs pipeline (single source of truth).

Per regs_pipeline/PLAN.md §5: three tables per year — ``articles`` (the search
table, one row per level-2 article section), ``documents`` (one row per PDF,
full text fallback) and ``regs_json`` (raw sidecars). Core SQL types only
(VARCHAR/SMALLINT/INTEGER/DATE/TIMESTAMP/JSON/BOOLEAN/TEXT) so the parquet
files work under DuckDB-WASM without extensions.
"""

from __future__ import annotations

#: (column, type) pairs for ``articles.parquet`` (PLAN.md §5.1).
ARTICLES_COLUMNS: list[tuple[str, str]] = [
    ("row_id", "VARCHAR"),          # {doc_id}#{article_number}#{occurrence}
    ("doc_id", "VARCHAR"),          # {year}/{doc-slug}/iss-{NN}
    ("year", "SMALLINT"),
    ("section", "VARCHAR"),         # A-F for 2026+ regs; NULL for historical
    ("regulation_type", "VARCHAR"), # general/sporting/technical/financial-teams/financial-pu/operational (+ -pu variants)
    ("section_name", "VARCHAR"),    # e.g. "Sporting Regulations"
    ("issue", "SMALLINT"),
    ("is_latest", "BOOLEAN"),
    ("article_number", "VARCHAR"),  # e.g. B1.1; FRONT_MATTER; APPENDIX-n
    ("article_title", "VARCHAR"),
    ("parent_article", "VARCHAR"),  # e.g. "B1 — ORGANISATION OF A COMPETITION"
    ("occurrence", "SMALLINT"),     # 0 unless the number repeats within the doc
    ("printed_page_start", "SMALLINT"),
    ("printed_page_end", "SMALLINT"),
    ("pdf_page_start", "SMALLINT"),
    ("pdf_page_end", "SMALLINT"),
    ("content", "TEXT"),            # markdown: tags, [PAGE n] markers, pipe tables
    ("char_len", "INTEGER"),
    ("has_changes", "BOOLEAN"),
    ("has_removals", "BOOLEAN"),
    ("has_governance", "BOOLEAN"),
    ("has_reference", "BOOLEAN"),
    ("has_comment", "BOOLEAN"),
    ("n_changed", "INTEGER"),       # tagged-span counts
    ("n_removed", "INTEGER"),
    ("clauses", "JSON"),            # [{"number","offset","length"}, ...]
    ("source_pdf", "VARCHAR"),
    ("source_hash", "VARCHAR"),
    ("extracted_at", "TIMESTAMPTZ"),
    ("ingested_at", "TIMESTAMP"),
]

#: (column, type) pairs for ``documents.parquet`` (PLAN.md §5.2).
DOCUMENTS_COLUMNS: list[tuple[str, str]] = [
    ("doc_id", "VARCHAR"),
    ("year", "SMALLINT"),
    ("section", "VARCHAR"),
    ("regulation_type", "VARCHAR"),
    ("section_name", "VARCHAR"),
    ("issue", "SMALLINT"),
    ("is_latest", "BOOLEAN"),
    ("title", "VARCHAR"),
    ("status", "VARCHAR"),
    ("issue_date", "DATE"),
    ("wmsc_approval_date", "DATE"),
    ("pages", "INTEGER"),
    ("content", "TEXT"),            # full markdown (all articles, tags, page markers)
    ("char_len", "INTEGER"),
    ("tables_extracted", "INTEGER"),
    ("stats", "JSON"),
    ("source_pdf", "VARCHAR"),
    ("source_hash", "VARCHAR"),
    ("extracted_at", "TIMESTAMPTZ"),
    ("ingested_at", "TIMESTAMP"),
]

#: (column, type) pairs for ``regs_json.parquet`` (PLAN.md §5.3): flattened
#: queryable metadata plus the raw sidecar in ``data`` (full fidelity).
REGS_JSON_COLUMNS: list[tuple[str, str]] = [
    ("doc_id", "VARCHAR"),
    ("year", "SMALLINT"),
    ("section", "VARCHAR"),
    ("regulation_type", "VARCHAR"),
    ("section_name", "VARCHAR"),
    ("issue", "SMALLINT"),
    ("is_latest", "BOOLEAN"),
    ("title", "VARCHAR"),
    ("status", "VARCHAR"),
    ("issue_date", "DATE"),
    ("wmsc_approval_date", "DATE"),
    ("pages", "INTEGER"),
    ("source_pdf", "VARCHAR"),
    ("source_hash", "VARCHAR"),
    ("extracted_at", "TIMESTAMPTZ"),
    ("ingested_at", "TIMESTAMP"),
    ("data", "JSON"),               # raw sidecar incl. toc[], convention, tables[]
]


def ddl(table: str, columns: list[tuple[str, str]]) -> str:
    body = ",\n  ".join(f"  {name} {typ}" for name, typ in columns)
    return f"CREATE TABLE IF NOT EXISTS {table} (\n{body}\n);"


TABLES: dict[str, list[tuple[str, str]]] = {
    "articles": ARTICLES_COLUMNS,
    "documents": DOCUMENTS_COLUMNS,
    "regs_json": REGS_JSON_COLUMNS,
}

#: Columns written by the builder (ingested_at filled explicitly at build time).
WRITE_COLUMNS: dict[str, list[str]] = {
    table: [name for name, _ in cols] for table, cols in TABLES.items()
}
