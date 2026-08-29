#!/usr/bin/env python3
"""Convert built ``.duckdb`` files into compressed ``.parquet`` files (zstd).

The ``.duckdb`` files are the builders' writable working format — incremental
upserts need an updatable table. The ``.parquet`` files are the shipped,
read-only format agents query: DuckDB reads parquet natively (same SQL surface,
same core types, and the ``data`` JSON column round-trips), and zstd shrinks
the corpus ~10-30x (149 MB -> ~8 MB).

Conversion is **verified before the source is removed**: the parquet must have
the same row count and contain exactly the same rows (full row equality, all
columns, both directions) or the ``.duckdb`` file is kept and the run fails.

Usage:
  uv run duckdb/convert_to_parquet.py --all
  uv run duckdb/convert_to_parquet.py --year 2020
  uv run duckdb/convert_to_parquet.py --all --keep-duckdb  # convert, keep sources
  uv run duckdb/convert_to_parquet.py --all --dry-run      # report only

Run after the builders (``build_markdown.py`` / ``build_json.py``) so the
parquet files track the latest extraction.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import config as cfg
import duckdb

COMPRESSION = "zstd"


def parquet_targets(year: int) -> list[tuple[Path, Path]]:
    """``(duckdb_src, parquet_dst)`` pairs for one year (or json_all only)."""
    return [
        (cfg.markdown_db_path(year), cfg.markdown_parquet_path(year)),
        (cfg.json_db_path(year), cfg.json_parquet_path(year)),
    ]


def convert_pair(con: duckdb.DuckDBPyConnection, src: Path, dst: Path, *, keep_duckdb: bool) -> str:
    """Convert one file. Returns a short status line."""
    if not src.exists():
        if dst.exists():
            return f"ok    {dst.name} (already converted, no .duckdb source)"
        raise FileNotFoundError(f"missing source: {src}")
    if dst.exists() and dst.stat().st_mtime > src.stat().st_mtime:
        return f"SKIP  {dst.name} (parquet is newer than the .duckdb — nothing to do)"

    dst.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM '{src}') TO '{dst}' (FORMAT PARQUET, COMPRESSION '{COMPRESSION}')")

    # Verified round-trip: same row count + identical rows in both directions.
    n_src = con.execute(f"SELECT count(*) FROM '{src}'").fetchone()[0]
    n_dst = con.execute(f"SELECT count(*) FROM '{dst}'").fetchone()[0]
    if n_src != n_dst:
        raise RuntimeError(f"{src.name}: row count mismatch {n_src} vs {n_dst}")
    in_src_not_dst = con.execute(
        f"SELECT count(*) FROM (SELECT * FROM '{src}' EXCEPT SELECT * FROM '{dst}')"
    ).fetchone()[0]
    in_dst_not_src = con.execute(
        f"SELECT count(*) FROM (SELECT * FROM '{dst}' EXCEPT SELECT * FROM '{src}')"
    ).fetchone()[0]
    if in_src_not_dst or in_dst_not_src:
        raise RuntimeError(f"{src.name}: content differs after conversion ({in_src_not_dst}/{in_dst_not_src} rows)")
    if not keep_duckdb:
        os.remove(src)
    return f"ok    {src.name} -> {dst.name} ({n_src} rows, {dst.stat().st_size / 1e6:.2f} MB)"


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert built .duckdb files to compressed .parquet")
    parser.add_argument("--year", type=int, help="only this year (markdown + json); with --all also json_all")
    parser.add_argument("--all", action="store_true", help="every year file plus json_all.parquet")
    parser.add_argument("--keep-duckdb", action="store_true", help="convert to parquet but keep the .duckdb sources")
    parser.add_argument("--dry-run", action="store_true", help="report what would happen without writing")
    args = parser.parse_args()

    if not args.year and not args.all:
        parser.error("pass --year <n> or --all")

    targets: list[tuple[Path, Path]] = []
    years = [args.year] if args.year else cfg.YEARS
    for year in years:
        targets.extend(parquet_targets(year))
    if args.all:
        targets.append((cfg.json_all_db_path(), cfg.json_all_parquet_path()))

    con = duckdb.connect()
    try:
        for src, dst in targets:
            if args.dry_run:
                state = "convert" if src.exists() else ("skip" if dst.exists() else "missing source")
                print(f"{state:7s} {src.name} -> {dst.name}")
                continue
            try:
                print(convert_pair(con, src, dst, keep_duckdb=args.keep_duckdb))
            except FileNotFoundError as exc:
                print(f"SKIP  {exc}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())