#!/usr/bin/env python3
"""QA checks for the built DuckDB files (PLAN.md §6).

Checks (exit code 1 on any failure):

1. Row count per ``markdown_{year}``   == .md count on disk for that year
2. Row count per ``json_{year}``        == sidecar count; json_all == sum
3. No row with NULL ``content``         in any markdown table
4. ``source_hash`` integrity            duplicated hashes (same PDF re-uploaded
                                        under a second filename) are expected in
                                        the source corpus — the check is that all
                                        rows sharing a hash carry identical
                                        structural content (the filenames/titles/
                                        extraction timestamps naturally differ)
5. Spot check 2020 Belgian GP decision  title / date / doc_number / content
6. Sample agent queries                 the §7 cookbook queries return the same
                                        rows as scanning the extracted files
                                        directly (data-driven, no hardcoded facts)

The shipped files are ``.parquet`` (zstd); the ``.duckdb`` files are the
builders' writable working format and are converted by ``convert_to_parquet.py``.
DuckDB reads parquet natively with the same SQL surface, so every check below
runs against the parquet files.

Usage: uv run duckdb/verify.py [--year 2020]
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import config as cfg
import duckdb

FAILURES: list[str] = []
CHECKS = 0


def check(ok: bool, label: str, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    mark = "ok " if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    if not ok:
        FAILURES.append(label)


def info(label: str, detail: str = "") -> None:
    print(f"  [..] {label}" + (f" — {detail}" if detail else ""))


def open_ro(path) -> Any:
    """In-memory connection with the parquet file attached as `documents`.

    Parquet files are read via DuckDB's native parquet reader (same SQL
    surface the WASM build uses); `read_only=True` doesn't apply to them.
    """
    con = duckdb.connect()
    con.execute(f"CREATE VIEW documents AS SELECT * FROM read_parquet('{path}')")
    return con


def disk_content_matches(year: int, manifest: dict, terms: list[str], *, event: str | None = None) -> int:
    """Count extracted .md files for a year whose lowercase text contains all terms.

    ``event`` restricts the scan to one event (mirroring the SQL predicate).
    """
    n = 0
    for doc in cfg.discover_year(year, manifest):
        if event is not None and doc["event"] != event:
            continue
        try:
            content = doc["md_path"].read_text(encoding="utf-8").lower()
        except OSError:
            continue
        if all(t in content for t in terms):
            n += 1
    return n


def _structural(data: dict) -> dict:
    """Sidecar with self-referential filename-derived fields removed.

    Duplicated PDFs (``foo.pdf`` vs ``foo_1.pdf``) share a source_hash but
    their sidecars differ in ``source_pdf``, ``title`` (carries the filename),
    ``extracted_at`` (per-run timestamp) and ``images[].deduplicated`` (flag
    depends on extraction order) — those are not document content.
    """
    out = {k: v for k, v in data.items() if k not in ("source_pdf", "title", "extracted_at")}
    if isinstance(out.get("images"), list):
        out["images"] = [{k: v for k, v in img.items() if k != "deduplicated"} for img in out["images"]]
    return out


def dup_hash_consistency(con, column: str, *, structural: bool) -> tuple[int, bool]:
    """(dup groups, all-consistent?) — rows sharing a source_hash must agree on `column`."""
    rows = con.execute(f"SELECT source_hash, {column} FROM documents WHERE source_hash IS NOT NULL").fetchall()
    by_hash: dict[str, list] = {}
    for h, value in rows:
        if structural:
            try:
                value = json.dumps(_structural(json.loads(value)), sort_keys=True)
            except (TypeError, ValueError):
                value = json.dumps(value, sort_keys=True, default=str)
        by_hash.setdefault(h, []).append(value)
    dup_groups = {h: vs for h, vs in by_hash.items() if len(vs) > 1}
    consistent = all(len(set(vs)) == 1 for vs in dup_groups.values())
    return len(dup_groups), consistent


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify built DuckDB files")
    parser.add_argument("--year", type=int, help="only verify this year")
    args = parser.parse_args()
    years = [args.year] if args.year else cfg.YEARS

    manifest = cfg.load_manifest()

    for year in years:
        print(f"\n== markdown_{year}.parquet ==")
        md_path = cfg.markdown_parquet_path(year)
        if not md_path.exists():
            check(False, "file exists", str(md_path))
            continue
        con = open_ro(md_path)
        n = con.execute("SELECT count(*) FROM documents").fetchone()[0]
        disk_count = len(cfg.discover_year(year, manifest))
        check(n == disk_count, "row count == .md on disk", f"{n} vs {disk_count}")

        nulls = con.execute("SELECT count(*) FROM documents WHERE content IS NULL").fetchone()[0]
        check(nulls == 0, "no NULL content", f"{nulls} nulls")

        dup = con.execute(
            "SELECT count(*) FROM (SELECT manifest_key FROM documents GROUP BY manifest_key HAVING count(*) > 1)"
        ).fetchone()[0]
        check(dup == 0, "no duplicate manifest_key", f"{dup} dups")

        dup_groups, consistent = dup_hash_consistency(con, "content", structural=False)
        if dup_groups:
            info(
                "duplicate source_hash",
                f"{dup_groups} hash(es) shared by 2+ docs (same PDF re-uploaded) — identical content: {consistent}",
            )
        check(consistent, "duplicate-hash rows have identical content")

        # Spot check: 2020 Belgian GP decision (plan §6)
        if year == 2020:
            row = con.execute(
                "SELECT title, date, doc_number, substring(content, 1, 40) FROM documents "
                "WHERE event = 'belgian-grand-prix' AND doc_type = 'decision' "
                "AND title ILIKE '%driving%slowly%'"
            ).fetchone()
            if row is None:
                check(False, "spot check 2020 Belgian GP decision", "no row found")
            else:
                title, date, doc_number, content_head = row
                check(
                    title == "Decision Car 16 Alleged Driving Unnecessarily Slowly"
                    and str(date) == "2020-08-30"
                    and doc_number == "41"
                    and content_head.startswith("# From The"),
                    "spot check 2020 Belgian GP decision",
                    f"title={title!r} date={date} doc_number={doc_number} head={content_head!r}",
                )

        # §7 sample query (only meaningful for 2020): DB == direct disk scan.
        if year == 2020:
            db_hits = con.execute(
                "SELECT count(*) FROM documents "
                "WHERE event = 'belgian-grand-prix' "
                "AND content ILIKE '%leclerc%' AND content ILIKE '%impeding%'"
            ).fetchone()[0]
            disk_hits = disk_content_matches(2020, manifest, ["leclerc", "impeding"], event="belgian-grand-prix")
            # NB: no 2020 Belgian GP document contains both terms (verstappen
            # impeding/leclerc are separate documents) — 0 == 0 is the expected,
            # data-driven equivalent of the plan's cookbook example.
            check(
                db_hits == disk_hits,
                "sample query: Leclerc impeding Spa 2020 == disk scan",
                f"{db_hits} vs {disk_hits}",
            )  # noqa: E501

        # §7 analytical query (decisions per event)
        rows = con.execute(
            "SELECT event, count(*) FROM documents WHERE doc_type = 'decision' GROUP BY event ORDER BY 2 DESC"
        ).fetchall()
        expected = con.execute("SELECT count(*) FROM documents WHERE doc_type = 'decision'").fetchone()[0]
        top_event, top_count = rows[0] if rows else (None, 0)
        check(
            sum(c for _, c in rows) == expected and top_count > 0,
            "sample query: decisions per event",
            f"top={top_event}:{top_count}, total={expected}",
        )
        con.close()

        print(f"\n== json_{year}.parquet ==")
        json_path = cfg.json_parquet_path(year)
        if not json_path.exists():
            check(False, "file exists", str(json_path))
            continue
        con = open_ro(json_path)
        n = con.execute("SELECT count(*) FROM documents").fetchone()[0]
        check(n == disk_count, "row count == sidecars on disk", f"{n} vs {disk_count}")
        dup_groups, consistent = dup_hash_consistency(con, "data", structural=True)
        if dup_groups:
            info(
                "duplicate source_hash",
                f"{dup_groups} hash(es) shared by 2+ docs — structurally identical data: {consistent}",
            )
        check(consistent, "duplicate-hash rows have structurally identical data")
        con.close()

    print("\n== json_all.parquet ==")
    all_path = cfg.json_all_parquet_path()
    if not all_path.exists():
        check(False, "file exists", str(all_path))
    else:
        con = open_ro(all_path)
        n = con.execute("SELECT count(*) FROM documents").fetchone()[0]
        expected_total = sum(len(cfg.discover_year(y, manifest)) for y in cfg.YEARS)
        check(n == expected_total, "row count == all sidecars on disk", f"{n} vs {expected_total}")

        top = con.execute(
            "SELECT doc_type, count(*) FROM documents GROUP BY doc_type ORDER BY 2 DESC LIMIT 1"
        ).fetchone()
        check(
            top is not None and top[1] > 0,
            "sample query: doc_type distribution",
            f"top={top[0]}:{top[1]}" if top else "",
        )

        row = con.execute(
            "SELECT title, json_extract_string(data, '$.tables') FROM documents "
            "WHERE event = 'bahrain-grand-prix' AND title ILIKE '%final race classification%' LIMIT 1"
        ).fetchone()
        check(
            row is not None and row[1] is not None and row[1].startswith("["),
            "sample query: json_extract on raw sidecar",
            f"title={row[0] if row else None}",
        )
        con.close()

    print(f"\n{CHECKS} checks, {len(FAILURES)} failures")
    if FAILURES:
        for f in FAILURES:
            print(f"  FAILED: {f}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())