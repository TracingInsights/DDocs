#!/usr/bin/env python3
"""Export per-year parquet files from the staging duckdb (PLAN.md §7).

Reads ``data/_work/regs_staging.duckdb`` (built by build_articles.py) and
writes ``data/<year>/{articles,documents,regs_json}.parquet``, zstd, core
types only. No combined ``_all`` files (PLAN.md §2 decision 7) — DuckDB globs
``data/*/articles.parquet`` fine when a cross-year query is needed.

Every export is **verified before the staging row batch is trusted**: the
parquet must have the same row count and contain exactly the same rows (full
row equality, both directions) as the staging slice, or the run fails.

Usage:
  uv run regs_pipeline/convert_to_parquet.py
  uv run regs_pipeline/convert_to_parquet.py --year 2026
  uv run regs_pipeline/convert_to_parquet.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

import config as cfg
from build_articles import STAGING_DB
from schema import TABLES

COMPRESSION = "zstd"

# JSON columns are cast to VARCHAR for row-equality verification (JSON values
# do not reliably support set operations in DuckDB).
JSON_COLS = {"articles": {"clauses"}, "documents": {"stats"}, "regs_json": {"data"}}


def verify_select(table: str) -> str:
    cols = []
    for name, _ in TABLES[table]:
        cols.append(f"{name}::VARCHAR AS {name}" if name in JSON_COLS.get(table, set()) else name)
    return ", ".join(cols)


def export_year(con: duckdb.DuckDBPyConnection, year: int, *, dry_run: bool) -> list[str]:
    out_dir = cfg.DATA_DIR / str(year)
    lines: list[str] = []
    for table in TABLES:
        dst = out_dir / f"{table}.parquet"
        if dry_run:
            lines.append(f"would export {table} year={year} -> {dst}")
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        query = f"SELECT * FROM staging.{table} WHERE year = {int(year)}"
        con.execute(
            f"COPY ({query}) TO '{dst}' "
            f"(FORMAT PARQUET, COMPRESSION '{COMPRESSION}')"
        )
        n_src = con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
        n_dst = con.execute(f"SELECT count(*) FROM '{dst}'").fetchone()[0]
        if n_src != n_dst:
            raise RuntimeError(f"{table}/{year}: row count mismatch {n_src} vs {n_dst}")
        vcols = verify_select(table)
        src_v = f"SELECT {vcols} FROM ({query})"
        dst_v = f"SELECT {vcols} FROM '{dst}'"
        for direction, q in (
            ("src-not-dst", f"SELECT count(*) FROM ({src_v} EXCEPT {dst_v})"),
            ("dst-not-src", f"SELECT count(*) FROM ({dst_v} EXCEPT {src_v})"),
        ):
            n = con.execute(q).fetchone()[0]
            if n:
                raise RuntimeError(f"{table}/{year}: {direction} {n} rows differ after export")
        lines.append(f"ok {table:10s} year={year} rows={n_src:5d} "
                     f"({dst.stat().st_size / 1e6:.2f} MB) -> {dst}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description="Export per-year regs parquet from staging")
    parser.add_argument("--year", type=int, help="only this year")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not STAGING_DB.exists():
        print(f"missing staging db {STAGING_DB} — run build_articles.py first",
              file=sys.stderr)
        return 1

    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{STAGING_DB}' AS staging (READ_ONLY)")
        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM staging.documents ORDER BY year").fetchall()]
        if args.year is not None:
            if args.year not in years:
                print(f"year {args.year} not in staging ({years})", file=sys.stderr)
                return 1
            years = [args.year]
        for year in years:
            for line in export_year(con, year, dry_run=args.dry_run):
                print(line)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
