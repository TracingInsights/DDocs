#!/usr/bin/env python3
"""Build / refresh the per-year JSON DuckDB files plus the combined json_all.

Per PLAN.md §5.3: same incremental, idempotent upsert as the markdown build,
driven by the ``.json`` sidecar files. Each year is written to
``json_{year}.duckdb`` and the same rows are then upserted into
``json_all.duckdb`` (a superset of all years; ``year`` disambiguates).

Usage (from the repo root — the scripts import ``config``/``schema`` from
their own directory, so run them as files, not via ``-m``):

    uv run duckdb/build_json.py --year 2020
    uv run duckdb/build_json.py --all
    uv run duckdb/build_json.py --all --dry-run   # report only
    uv run duckdb/build_json.py --all --force     # drop + rebuild
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from schema import DDL_JSON, JSON_WRITE_COLUMNS

import config as cfg
import duckdb


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build json_{year}.duckdb files + json_all.duckdb")
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--year", type=int, help="single year to build")
    target.add_argument("--all", action="store_true", help="build all years 2018-2026 (+ combined)")
    p.add_argument("--dry-run", action="store_true", help="report inserts/updates/skips without writing")
    p.add_argument("--force", action="store_true", help="drop + recreate the table before ingesting")
    return p.parse_args(argv)


def _rows_for_year(year: int, manifest: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Read sidecars for a year into full JSON-table rows.

    Returns ``(rows, skipped_count)``. Rows carry the flattened metadata plus
    ``data`` (the raw sidecar JSON). Malformed sidecars are logged and skipped.
    """
    rows: list[dict[str, Any]] = []
    skipped = 0
    for doc in cfg.discover_year(year, manifest):
        try:
            with open(doc["json_path"], encoding="utf-8") as fh:
                sidecar = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"  SKIP {doc['manifest_key']}: bad sidecar ({type(exc).__name__})")
            skipped += 1
            continue

        meta = cfg.flatten_sidecar(sidecar)
        if doc["manifest"]:
            for key in cfg.MANIFEST_AUTHORITATIVE:
                if doc["manifest"].get(key) is not None:
                    meta[key] = doc["manifest"][key]

        rows.append(
            {
                "manifest_key": doc["manifest_key"],
                "year": year,
                "event": doc["event"],
                "pdf_stem": doc["pdf_stem"],
                **meta,
                "data": json.dumps(sidecar, ensure_ascii=False),
            }
        )
    return rows, skipped


def _sync(db_path, rows: list[dict[str, Any]], *, dry_run: bool, force: bool) -> dict[str, int]:
    """Diff rows against an existing DB and upsert the changed ones."""
    con, existing = cfg.open_database(db_path, DDL_JSON, force=force, dry_run=dry_run)
    summary = {"inserted": 0, "updated": 0, "unchanged": 0}
    pending: list[dict[str, Any]] = []

    if not dry_run:
        con.execute("BEGIN TRANSACTION")
    try:
        for row in rows:
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
            cfg.upsert(con, pending, JSON_WRITE_COLUMNS)
            cfg.close(con, commit=True)

    summary["total"] = len(rows)
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

    # --force rebuilds the combined DB deterministically: drop the table once,
    # then every target year upserts into a fresh table (a per-year drop would
    # wipe previously synced years).
    combined_path = cfg.json_all_db_path()
    if args.force and not args.dry_run and combined_path.exists():
        con = duckdb.connect(str(combined_path))
        try:
            con.execute("DROP TABLE IF EXISTS documents")
        finally:
            con.close()

    for year in years:
        rows, skipped = _rows_for_year(year, manifest)
        summary = _sync(cfg.json_db_path(year), rows, dry_run=args.dry_run, force=args.force)
        summary["skipped"] = skipped
        db_path = cfg.json_db_path(year)
        size = f"{db_path.stat().st_size / 1e6:.1f} MB" if db_path.exists() else "n/a"
        print(
            f"json_{year}.duckdb: "
            f"inserted={summary['inserted']} updated={summary['updated']} "
            f"unchanged={summary['unchanged']} skipped={summary['skipped']} "
            f"total={summary['total']} ({size})"
        )

        # Maintain the combined DB as a superset of the same rows (never force-
        # dropped per year — see above).
        combined = _sync(combined_path, rows, dry_run=args.dry_run, force=False)
        combined_size = f"{combined_path.stat().st_size / 1e6:.1f} MB" if combined_path.exists() else "n/a"
        print(
            f"json_all.duckdb  : "
            f"inserted={combined['inserted']} updated={combined['updated']} "
            f"unchanged={combined['unchanged']} total={combined['total']} ({combined_size})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
