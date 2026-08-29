#!/usr/bin/env python3
"""Build / refresh the per-year markdown DuckDB files (one row per .md document).

Per PLAN.md §5.2: incremental, idempotent upsert keyed on ``source_hash`` —
new documents are inserted, changed documents are replaced, unchanged
documents are skipped. One transaction per year file.

Usage (from the repo root — the scripts import ``config``/``schema`` from
their own directory, so run them as files, not via ``-m``):

    uv run duckdb/build_markdown.py --year 2020
    uv run duckdb/build_markdown.py --all
    uv run duckdb/build_markdown.py --all --dry-run   # report only
    uv run duckdb/build_markdown.py --all --force     # drop + rebuild
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from schema import DDL_MARKDOWN, MARKDOWN_WRITE_COLUMNS

import config as cfg


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build markdown_{year}.duckdb files")
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--year", type=int, help="single year to build")
    target.add_argument("--all", action="store_true", help="build all years 2018-2026")
    p.add_argument("--dry-run", action="store_true", help="report inserts/updates/skips without writing")
    p.add_argument("--force", action="store_true", help="drop + recreate the table before ingesting")
    return p.parse_args(argv)


def build_year(year: int, manifest: dict[str, dict[str, Any]], *, dry_run: bool, force: bool) -> dict[str, int]:
    """Sync ``markdown_{year}.duckdb`` with the documents on disk."""
    docs = cfg.discover_year(year, manifest)
    db_path = cfg.markdown_db_path(year)
    con, existing = cfg.open_database(db_path, DDL_MARKDOWN, force=force, dry_run=dry_run)

    summary = {"inserted": 0, "updated": 0, "unchanged": 0, "skipped": 0}
    pending: list[dict[str, Any]] = []

    if not dry_run:
        con.execute("BEGIN TRANSACTION")
    try:
        for doc in docs:
            # Sidecar metadata (malformed sidecar -> log + skip, batch continues).
            try:
                with open(doc["json_path"], encoding="utf-8") as fh:
                    sidecar = json.load(fh)
            except (OSError, json.JSONDecodeError) as exc:
                print(f"  SKIP {doc['manifest_key']}: bad sidecar ({type(exc).__name__})")
                summary["skipped"] += 1
                continue

            meta = cfg.flatten_sidecar(sidecar)
            # Manifest is authoritative for the fields it duplicates.
            if doc["manifest"]:
                for key in cfg.MANIFEST_AUTHORITATIVE:
                    if doc["manifest"].get(key) is not None:
                        meta[key] = doc["manifest"][key]

            # Full markdown text (missing/unreadable file -> log + skip).
            try:
                content = doc["md_path"].read_text(encoding="utf-8")
            except OSError as exc:
                print(f"  SKIP {doc['manifest_key']}: unreadable markdown ({exc})")
                summary["skipped"] += 1
                continue

            row = {
                "manifest_key": doc["manifest_key"],
                "year": year,
                "event": doc["event"],
                "pdf_stem": doc["pdf_stem"],
                **meta,
                "content": content,
                "char_len": len(content),
            }

            old_hash = existing.get(row["manifest_key"])
            if row["source_hash"] is not None and old_hash == row["source_hash"]:
                summary["unchanged"] += 1
                continue
            if old_hash is None:
                summary["inserted"] += 1
            else:
                summary["updated"] += 1
            if not dry_run:
                pending.append(row)
    finally:
        if not dry_run:
            cfg.upsert(con, pending, MARKDOWN_WRITE_COLUMNS)
            cfg.close(con, commit=True)

    summary["total"] = len(docs)
    return summary


def main() -> int:
    args = parse_args(sys.argv[1:])
    years = cfg.YEARS if args.all else [args.year]
    bad = [y for y in years if y not in cfg.YEARS]
    if bad:
        print(f"error: year(s) {bad} out of scope (expected {cfg.YEARS})", file=sys.stderr)
        return 2

    manifest = cfg.load_manifest()
    print(f"manifest: {len(manifest)} entries loaded")
    for year in years:
        summary = build_year(year, manifest, dry_run=args.dry_run, force=args.force)
        db_path = cfg.markdown_db_path(year)
        size = f"{db_path.stat().st_size / 1e6:.1f} MB" if db_path.exists() else "n/a"
        print(
            f"markdown_{year}.duckdb: "
            f"inserted={summary['inserted']} updated={summary['updated']} "
            f"unchanged={summary['unchanged']} skipped={summary['skipped']} "
            f"total={summary['total']} ({size})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
