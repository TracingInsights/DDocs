#!/usr/bin/env python3
"""Build article/document/regs_json rows from extraction sidecars → staging duckdb.

Per PLAN.md §7: scan all sidecars under ``extracted/``, compute ``is_latest``
per ``(year, regulation_type)`` (max issue; ties → variant, then issue_date),
slice each ``.md`` into article rows using the sidecar's offset/length records,
and assemble the three tables. Rebuild is always full (seconds) so
``is_latest`` stays correct — no incremental upsert logic here.

The output is a single staging database ``data/_staging/regs_staging.duckdb``
with tables ``articles`` / ``documents`` / ``regs_json``; convert_to_parquet.py
exports the per-year parquet files from it (with verification).

Usage (from the repo root):

    uv run regs_pipeline/build_articles.py
    uv run regs_pipeline/build_articles.py --dry-run   # report only
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import duckdb

import config as cfg
from schema import TABLES, ddl

STAGING_DB = cfg.WORK_DB_DIR / "regs_staging.duckdb"


def discover_sidecars() -> list[Path]:
    return sorted(
        p for p in cfg.EXTRACTED_ROOT.rglob("*.json")
        if not p.name.startswith("_run_manifest")
    )


def _latest_flags(sidecars: list[dict[str, Any]]) -> dict[str, bool]:
    """doc_id → is_latest. One winner per (year, regulation_type): highest
    issue number, then variant (iss-06-v2 > iss-06), then issue_date."""
    best: dict[tuple[int, str], tuple[tuple, str]] = {}
    for sc in sidecars:
        key = (sc["year"], sc["regulation_type"])
        rank = (
            sc.get("issue") or 0,
            sc.get("variant") or 0,
            sc.get("issue_date") or "",
        )
        if key not in best or rank > best[key][0]:
            best[key] = (rank, sc["doc_id"])
    winners = {doc_id for _, doc_id in best.values()}
    return {sc["doc_id"]: sc["doc_id"] in winners for sc in sidecars}


def build_rows(
    latest: dict[str, bool],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Returns (articles_rows, documents_rows, regs_json_rows, warnings)."""
    articles_rows: list[dict[str, Any]] = []
    documents_rows: list[dict[str, Any]] = []
    regs_json_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    for json_path in discover_sidecars():
        try:
            sc = json.loads(json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            warnings.append(f"SKIP {json_path}: bad sidecar ({type(exc).__name__})")
            continue
        md_path = json_path.with_suffix(".md")
        if not md_path.exists():
            warnings.append(f"SKIP {json_path}: missing sibling .md")
            continue
        md = md_path.read_text(encoding="utf-8")
        doc_id: str = sc["doc_id"]
        is_latest = latest.get(doc_id, False)
        ingested_at = cfg.now_utc()

        def doc_base() -> dict[str, Any]:
            return {
                "doc_id": doc_id,
                "year": sc["year"],
                "section": sc.get("section"),
                "regulation_type": sc["regulation_type"],
                "section_name": sc.get("section_name"),
                "issue": sc["issue"],
                "is_latest": is_latest,
                "title": sc.get("title"),
                "status": sc.get("status"),
                "issue_date": sc.get("issue_date"),
                "wmsc_approval_date": sc.get("wmsc_approval_date"),
                "pages": sc["pages"],
                "source_pdf": sc.get("source_pdf"),
                "source_hash": sc.get("source_hash"),
                "extracted_at": sc.get("extracted_at"),
                "ingested_at": ingested_at,
            }

        # --- articles (slice the md via sidecar offsets; PLAN.md §5.1) ---
        seen_row_ids: set[str] = set()
        for art in sc.get("articles", []):
            off, length = art["offset"], art["length"]
            content = md[off:off + length].strip()
            row_id = f"{doc_id}#{art['article_number']}#{art['occurrence']}"
            if row_id in seen_row_ids:
                warnings.append(f"DUPLICATE row_id {row_id} — extraction bug?")
            seen_row_ids.add(row_id)
            articles_rows.append({
                "row_id": row_id,
                "doc_id": doc_id,
                "year": sc["year"],
                "section": sc.get("section"),
                "regulation_type": sc["regulation_type"],
                "section_name": sc.get("section_name"),
                "issue": sc["issue"],
                "is_latest": is_latest,
                "article_number": art["article_number"],
                "article_title": art.get("article_title"),
                "parent_article": art.get("parent_article"),
                "occurrence": art.get("occurrence", 0),
                "printed_page_start": art.get("printed_page_start"),
                "printed_page_end": art.get("printed_page_end"),
                "pdf_page_start": art.get("pdf_page_start"),
                "pdf_page_end": art.get("pdf_page_end"),
                "content": content,
                "char_len": len(content),
                "has_changes": art.get("has_changes", False),
                "has_removals": art.get("has_removals", False),
                "has_governance": art.get("has_governance", False),
                "has_reference": art.get("has_reference", False),
                "has_comment": art.get("has_comment", False),
                "n_changed": art.get("n_changed", 0),
                "n_removed": art.get("n_removed", 0),
                "clauses": json.dumps(art.get("clauses", []), ensure_ascii=False),
                "source_pdf": sc.get("source_pdf"),
                "source_hash": sc.get("source_hash"),
                "extracted_at": sc.get("extracted_at"),
                "ingested_at": ingested_at,
            })

        # --- documents (full text fallback; PLAN.md §5.2) ---
        documents_rows.append({
            **doc_base(),
            "content": md,
            "char_len": len(md),
            "tables_extracted": len(sc.get("tables", [])),
            "stats": json.dumps(sc.get("stats", {}), ensure_ascii=False),
        })

        # --- regs_json (raw sidecar, full fidelity; PLAN.md §5.3) ---
        regs_json_rows.append({
            **doc_base(),
            "data": json.dumps(sc, ensure_ascii=False),
        })

    return articles_rows, documents_rows, regs_json_rows, warnings


def write_staging(
    articles_rows: list[dict[str, Any]],
    documents_rows: list[dict[str, Any]],
    regs_json_rows: list[dict[str, Any]],
) -> None:
    """Full rebuild via NDJSON + read_json (duckdb's C++ parser ingests the
    35k-row articles table in <0.1s; executemany with per-row CASTs took ~8min)."""
    STAGING_DB.parent.mkdir(parents=True, exist_ok=True)
    if STAGING_DB.exists():
        STAGING_DB.unlink()  # full rebuild (PLAN.md §2 decision 10)
    con = duckdb.connect(str(STAGING_DB))
    try:
        for table, rows in (("articles", articles_rows),
                            ("documents", documents_rows),
                            ("regs_json", regs_json_rows)):
            con.execute(ddl(table, TABLES[table]))
            json_cols = {n for n, t in TABLES[table] if t == "JSON"}
            ndjson = STAGING_DB.parent / f"{table}.ndjson"
            with ndjson.open("w", encoding="utf-8") as f:
                for row in rows:
                    # JSON columns are stored as JSON strings in the row dicts;
                    # decode them so read_json materializes objects, not string literals
                    row = {k: (json.loads(v) if k in json_cols and isinstance(v, str) else v)
                           for k, v in row.items()}
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            cols_spec = "{" + ", ".join(f"'{n}': '{t}'" for n, t in TABLES[table]) + "}"
            con.execute(
                f"INSERT INTO {table} BY NAME SELECT * FROM read_json('{ndjson}',"
                f" format='newline_delimited', columns={cols_spec})"
            )
            ndjson.unlink()
            n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            if n != len(rows):
                raise RuntimeError(f"{table}: inserted {n} of {len(rows)} rows")
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Build regs staging tables from sidecars")
    parser.add_argument("--dry-run", action="store_true", help="report counts without writing")
    args = parser.parse_args()

    sidecars = [json.loads(p.read_text(encoding="utf-8"))
                for p in discover_sidecars()]
    if not sidecars:
        print("no sidecars found — run extract_markdown.py first", file=sys.stderr)
        return 1
    latest = _latest_flags(sidecars)
    art_rows, doc_rows, json_rows, warnings = build_rows(latest)
    for w in warnings:
        print(f"  {w}", file=sys.stderr)

    years = sorted({r["year"] for r in doc_rows})
    print(f"sidecars={len(sidecars)} articles={len(art_rows)} "
          f"documents={len(doc_rows)} regs_json={len(json_rows)} years={years}")
    n_latest = sum(1 for r in doc_rows if r["is_latest"])
    print(f"is_latest: {n_latest} documents (one per year × regulation_type)")

    if args.dry_run:
        return 0
    write_staging(art_rows, doc_rows, json_rows)
    size = STAGING_DB.stat().st_size / 1e6
    print(f"wrote {STAGING_DB} ({size:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
