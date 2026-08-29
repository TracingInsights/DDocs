"""Shared paths, constants, and helpers for the DuckDB build pipeline.

Per PLAN.md §5.1: paths, year list, schema column mapping, sidecar flattening.
Imported by ``build_markdown.py``, ``build_json.py`` and ``verify.py``.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import duckdb  # the driver lives in site-packages; this module is config.py

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

DUCKDB_DIR = Path(__file__).resolve().parent
REPO_ROOT = DUCKDB_DIR.parent
EXTRACTED_ROOT = REPO_ROOT / "extracted"
MANIFEST_PATH = EXTRACTED_ROOT / "manifest.json"
DATA_DIR = DUCKDB_DIR / "data"

#: Years in scope — 2018-2026 only (per PLAN.md §1).
YEARS = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]

TABLE = "documents"

#: Sidecar keys that the manifest duplicates; the manifest is authoritative
#: for these when a manifest entry exists (per PLAN.md §5.3 cross-check).
MANIFEST_AUTHORITATIVE = ("source_hash", "doc_type", "pages", "tables_extracted", "images_extracted", "extracted_at")


def markdown_db_path(year: int) -> Path:
    return DATA_DIR / f"markdown_{year}.duckdb"


def json_db_path(year: int) -> Path:
    return DATA_DIR / f"json_{year}.duckdb"


def json_all_db_path() -> Path:
    return DATA_DIR / "json_all.duckdb"


def markdown_parquet_path(year: int) -> Path:
    return DATA_DIR / f"markdown_{year}.parquet"


def json_parquet_path(year: int) -> Path:
    return DATA_DIR / f"json_{year}.parquet"


def json_all_parquet_path() -> Path:
    return DATA_DIR / "json_all.parquet"


def manifest_key(year: int, event: str, pdf_stem: str) -> str:
    """Manifest key for a document, e.g. ``documents/2020/belgian-grand-prix/foo.pdf``."""
    return f"documents/{year}/{event}/{pdf_stem}.pdf"


# ---------------------------------------------------------------------------
# Manifest loading
# ---------------------------------------------------------------------------


def load_manifest() -> dict[str, dict[str, Any]]:
    """Load ``extracted/manifest.json`` as ``{manifest_key: meta}``.

    Returns an empty dict if the file is missing or unreadable so callers can
    degrade gracefully (disk-driven discovery still works).
    """
    try:
        with open(MANIFEST_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}
    return {k: v for k, v in data.items() if isinstance(v, dict)}


# ---------------------------------------------------------------------------
# Sidecar flattening (schema metadata columns)
# ---------------------------------------------------------------------------


def _parse_date(value: Any) -> datetime.date | None:
    if isinstance(value, str):
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            return None
    return None


def flatten_sidecar(sidecar: dict[str, Any]) -> dict[str, Any]:
    """Map a sidecar JSON object to the schema's metadata columns (null-safe).

    ``from``/``to`` are renamed to ``sender``/``recipient`` (``from`` is a SQL
    keyword). ``date`` is parsed to a ``datetime.date``; anything unparsable
    becomes ``None`` rather than crashing the batch (per PLAN.md §3).
    """
    return {
        "source_hash": sidecar.get("source_hash"),
        "doc_type": sidecar.get("doc_type"),
        "title": sidecar.get("title"),
        "date": _parse_date(sidecar.get("date")),
        "doc_number": sidecar.get("doc_number"),
        "time": sidecar.get("time"),
        "event_name": sidecar.get("event_name"),
        "sender": sidecar.get("from"),
        "recipient": sidecar.get("to"),
        "pages": sidecar.get("pages"),
        "tables_extracted": sidecar.get("tables_extracted"),
        "images_extracted": sidecar.get("images_extracted"),
        "extracted_at": sidecar.get("extracted_at"),
    }


# ---------------------------------------------------------------------------
# Document discovery (disk-driven, manifest-gated)
# ---------------------------------------------------------------------------


def discover_year(year: int, manifest: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """List every document of ``year`` present on disk as ``{...}`` row parts.

    Walks ``extracted/{year}/{event}/*.md`` (skipping ``images/``), pairs each
    with its sibling ``.json`` sidecar, and resolves the manifest entry by
    key. Documents whose manifest entry has ``success: false`` are excluded;
    documents with no manifest entry at all (sidecars produced by extraction
    runs whose manifest snapshot is stale) are still included — the sidecar
    carries the full metadata. Malformed sidecars are skipped at build time.

    Each returned dict has:
        manifest_key, year, event, pdf_stem, md_path, json_path, manifest
    """
    rows: list[dict[str, Any]] = []
    year_root = EXTRACTED_ROOT / str(year)
    if not year_root.is_dir():
        return rows
    for md_path in sorted(year_root.rglob("*.md")):
        if "images" in md_path.parts:
            continue
        rel = md_path.relative_to(year_root)
        if len(rel.parts) != 2:  # requires {event}/{stem}.md
            continue
        event, filename = rel.parts
        pdf_stem = filename[: -len(".md")]
        key = manifest_key(year, event, pdf_stem)
        meta = manifest.get(key)
        if meta is not None and not meta.get("success"):
            continue  # manifest says extraction failed — do not ingest
        rows.append(
            {
                "manifest_key": key,
                "year": year,
                "event": event,
                "pdf_stem": pdf_stem,
                "md_path": md_path,
                "json_path": md_path.with_suffix(".json"),
                "manifest": meta,
            }
        )
    return rows


def count_markdown_on_disk(year: int) -> int:
    """Count ``.md`` files on disk for a year (matches the verify expectations)."""
    return len(discover_year(year, load_manifest()))


# ---------------------------------------------------------------------------
# DuckDB helpers (shared by both builders)
# ---------------------------------------------------------------------------


def load_existing_hashes(con: Any) -> dict[str, str | None]:
    """Load ``{manifest_key: source_hash}`` from the DB's documents table."""
    try:
        rows = con.execute(f"SELECT manifest_key, source_hash FROM {TABLE}").fetchall()
    except Exception:  # table does not exist yet (fresh DB)
        return {}
    return {key: value for key, value in rows}


def open_database(
    db_path: Path, ddl: str, *, force: bool = False, dry_run: bool = False
) -> tuple[Any | None, dict[str, str | None]]:
    """Open a year DB for writing (or peek read-only for ``--dry-run``).

    Returns ``(con, existing_hashes)``. ``con`` is ``None`` in dry-run mode —
    nothing is created or written. With ``--force`` the documents table is
    dropped and recreated so the build is deterministic.
    """
    if dry_run:
        existing: dict[str, str | None] = {}
        if not force and db_path.exists():
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                existing = load_existing_hashes(con)
            finally:
                con.close()
        return None, existing

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    if force:
        con.execute(f"DROP TABLE IF EXISTS {TABLE}")
    con.execute(ddl)
    import schema  # function-level: schema imports config at module level

    for stmt in [s.strip() for s in schema.INDEXES.split(";") if s.strip()]:
        con.execute(stmt)
    return con, load_existing_hashes(con)


def upsert(con: Any, rows: list[dict[str, Any]], columns: list[str]) -> None:
    """``INSERT OR REPLACE`` rows into the documents table (keyed on manifest_key)."""
    if not rows:
        return
    cols = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT OR REPLACE INTO {TABLE} ({cols}) VALUES ({placeholders})"
    con.executemany(sql, [tuple(row[c] for c in columns) for row in rows])


def close(con: Any | None, *, commit: bool) -> None:
    """Commit (unless dry-run) and close a build connection."""
    if con is None:
        return
    try:
        con.execute("COMMIT" if commit else "ROLLBACK")
    finally:
        con.close()
