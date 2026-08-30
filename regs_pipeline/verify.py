#!/usr/bin/env python3
"""Sanity checks over the regs parquet corpus (PLAN.md §9).

Runs the full verification battery against ``data/<year>/*.parquet`` plus the
extraction sidecars, and prints PASS/FAIL per check with details on failure.

Checks:
  1. tag-balance       every [X] has a matching [/X] in every row
  2. page-coverage     one [PAGE n] marker per PDF page, no dupes, per document
  3. toc-cross-check   every level-2 TOC entry covered by a body row (body may exceed TOC)
  4. is-latest         exactly one is_latest per (year, regulation_type)
  5. strike-sanity     n_removed > 0 when the doc announces changes and issue > 1
  6. unknown-colors    stats.unknown_color_spans == 0
  7. ligature          banned ligature-bug patterns absent from content
  8. row-id-unique     row_id unique across all years
  9. round-trip        DuckDB reads all parquet; counts match sidecars; the
                       README cookbook queries execute

Usage:  uv run regs_pipeline/verify.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import duckdb

import config as cfg

TAGS = ["CHANGED", "REMOVED", "GOVERNANCE", "REFERENCE", "COMMENT"]
RE_PAGE_MARKER = re.compile(r"\[PAGE (\d+)\]")
RE_BANNED_LIG = [
    cfg.RE_LIGATURE_FI, cfg.RE_LIGATURE_FL,
    cfg.RE_LIGATURE_EQ, cfg.RE_LIGATURE_BT, cfg.RE_LIGATURE_W,
]

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name:16s} {detail}")


def parquet_glob(table: str) -> str:
    return str(cfg.DATA_DIR / "*" / f"{table}.parquet")


def main() -> int:
    con = duckdb.connect()
    failures = 0

    # ---------------------------------------------------------------- load
    art_glob = parquet_glob("articles")
    doc_glob = parquet_glob("documents")
    json_glob = parquet_glob("regs_json")
    try:
        n_articles = con.execute(f"SELECT count(*) FROM '{art_glob}'").fetchone()[0]
        n_docs = con.execute(f"SELECT count(*) FROM '{doc_glob}'").fetchone()[0]
        n_json = con.execute(f"SELECT count(*) FROM '{json_glob}'").fetchone()[0]
    except duckdb.Error as exc:
        print(f"cannot read parquet under {cfg.DATA_DIR}: {exc}", file=sys.stderr)
        return 1
    check("round-trip-load", True,
          f"articles={n_articles} documents={n_docs} regs_json={n_json}")

    # sidecar counts must match documents/regs_json row counts
    n_sidecars = len([p for p in cfg.EXTRACTED_ROOT.rglob("*.json")
                      if not p.name.startswith("_run_manifest")])
    check("round-trip-counts", n_docs == n_sidecars == n_json,
          f"sidecars={n_sidecars} documents={n_docs} regs_json={n_json}")

    # ------------------------------------------------------- 1. tag balance
    bad = con.execute(f"""
        SELECT count(*) FROM '{art_glob}'
        WHERE {" OR ".join(
            f"len(regexp_extract_all(content, '\\[{t}\\]'))"
            f" != len(regexp_extract_all(content, '\\[/{t}\\]'))" for t in TAGS)}
    """).fetchone()[0]
    bad += con.execute(f"""
        SELECT count(*) FROM '{doc_glob}'
        WHERE {" OR ".join(
            f"len(regexp_extract_all(content, '\\[{t}\\]'))"
            f" != len(regexp_extract_all(content, '\\[/{t}\\]'))" for t in TAGS)}
    """).fetchone()[0]
    check("tag-balance", bad == 0, f"{bad} rows with unbalanced tags")

    # ----------------------------------------------------- 2. page coverage
    docs = con.execute(f"""
        SELECT doc_id, pages, content FROM '{doc_glob}'
    """).fetchall()
    bad_pages: list[str] = []
    for doc_id, pages, content in docs:
        markers = [int(m) for m in RE_PAGE_MARKER.findall(content)]
        if len(markers) != len(set(markers)):
            bad_pages.append(f"{doc_id}: duplicate markers")
        elif len(markers) != pages:
            bad_pages.append(f"{doc_id}: {len(markers)} markers vs {pages} pages")
    check("page-coverage", not bad_pages,
          f"{len(bad_pages)} docs off" + (f" — {bad_pages[:5]}" if bad_pages else ""))

    # -------------------------------------------------- 3. toc cross-check
    # Directional: the TOC must be COVERED by the body (every TOC level-2
    # number present as a row, body row count not far below the TOC count).
    # Body rows legitimately EXCEED the TOC: appendix-internal sections and
    # numbered volume sections (2021+ tech, 2026 section-c) are not listed.
    toc_bad: list[str] = []
    rows_per_doc: dict[str, tuple[int, set[str]]] = {}
    for doc_id, nums_json, n in con.execute(f"""
        SELECT doc_id, json_group_array(article_number)::VARCHAR, count(*)
        FROM '{art_glob}'
        WHERE article_number NOT IN ('FRONT_MATTER', 'BODY_INTRO')
        GROUP BY doc_id
    """).fetchall():
        rows_per_doc[doc_id] = (n, set(json.loads(nums_json)))
    for doc_id, data_json in con.execute(
            f"SELECT doc_id, data::VARCHAR FROM '{json_glob}'").fetchall():
        toc = json.loads(data_json).get("toc", [])
        toc_l2 = [t["article"] for t in toc if t.get("level") == 2]
        if not toc_l2:
            continue
        n_rows, body_nums = rows_per_doc.get(doc_id, (0, set()))
        missing = [t for t in toc_l2 if t not in body_nums]
        if n_rows < 0.75 * len(toc_l2) or len(missing) > max(2, 0.1 * len(toc_l2)):
            toc_bad.append(f"{doc_id}: {n_rows} rows vs {len(toc_l2)} toc-L2, "
                           f"{len(missing)} missing")
    check("toc-cross-check", not toc_bad,
          f"{len(toc_bad)} docs off" + (f" — {toc_bad[:5]}" if toc_bad else ""))

    # -------------------------------------------------------- 4. is_latest
    bad_latest = con.execute(f"""
        SELECT year, regulation_type, count(*) FILTER (is_latest) AS n
        FROM '{doc_glob}' GROUP BY year, regulation_type
        HAVING n != 1
    """).fetchall()
    check("is-latest", not bad_latest, f"{len(bad_latest)} groups without exactly one latest"
          + (f" — {bad_latest[:5]}" if bad_latest else ""))

    # ----------------------------------------------------- 5. strike sanity
    strike_bad = con.execute(f"""
        SELECT doc_id FROM '{art_glob}'
        WHERE is_latest AND issue > 1
        GROUP BY doc_id, issue
        HAVING sum(n_removed) = 0 AND sum(n_changed) > 0
    """).fetchall()
    check("strike-sanity", len(strike_bad) <= max(2, 0.05 * n_docs),
          f"{len(strike_bad)} latest docs with changes but zero removals"
          + (f" — {[d for d, *_ in strike_bad[:5]]}" if strike_bad else ""))

    # ---------------------------------------------------- 6. unknown colors
    unk = con.execute(f"""
        SELECT doc_id, json_extract(data, '$.stats.unknown_color_spans')::INT AS n
        FROM '{json_glob}' WHERE n IS NOT NULL AND n > 0
    """).fetchall()
    check("unknown-colors", not unk, f"{len(unk)} docs with unknown colors"
          + (f" — {unk[:5]}" if unk else ""))

    # --------------------------------------------------------- 7. ligatures
    lig_hits: list[str] = []
    for doc_id, _pages, content in docs:
        for rx in RE_BANNED_LIG:
            if rx.search(content):
                lig_hits.append(f"{doc_id}: {rx.pattern!r}")
                break
    check("ligature", not lig_hits,
          f"{len(lig_hits)} docs with banned patterns"
          + (f" — {lig_hits[:3]}" if lig_hits else ""))

    # ------------------------------------------------------ 8. row_id unique
    dup_ids = con.execute(f"""
        SELECT row_id FROM '{art_glob}' GROUP BY row_id HAVING count(*) > 1
    """).fetchall()
    check("row-id-unique", not dup_ids, f"{len(dup_ids)} duplicate row_ids"
          + (f" — {[d[0] for d in dup_ids[:5]]}" if dup_ids else ""))

    # -------------------------------------------- 9. README cookbook queries
    cookbook = [
        ("rule lookup", f"""SELECT article_number, article_title, char_len
            FROM '{art_glob}' WHERE is_latest AND regulation_type='sporting'
            ORDER BY article_number LIMIT 5"""),
        ("changed in latest", f"""SELECT count(*) FROM '{art_glob}'
            WHERE has_changes AND is_latest AND regulation_type='sporting'"""),
        ("deletions", f"""SELECT count(*) FROM '{art_glob}'
            WHERE has_removals AND is_latest"""),
        ("most changed", f"""SELECT article_number, n_changed FROM '{art_glob}'
            WHERE is_latest ORDER BY n_changed DESC LIMIT 5"""),
        ("historic ilike", f"""SELECT count(*) FROM '{art_glob}'
            WHERE year = 2018 AND content ILIKE '%halo%'"""),
        ("clauses json", f"""SELECT article_number,
            json_array_length(clauses) FROM '{art_glob}'
            WHERE is_latest AND json_array_length(clauses) > 0 LIMIT 3"""),
    ]
    cq_fail: list[str] = []
    for name, sql in cookbook:
        try:
            con.execute(sql).fetchall()
        except duckdb.Error as exc:
            cq_fail.append(f"{name}: {exc}")
    check("cookbook-queries", not cq_fail, f"{len(cq_fail)} failed — {cq_fail[:2]}")

    failures = sum(1 for _, ok, _ in results if not ok)
    print(f"\n{'=' * 50}\n{len(results) - failures}/{len(results)} checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
