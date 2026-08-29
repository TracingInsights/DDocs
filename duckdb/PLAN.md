# Implementation Plan — Per-Year Markdown & JSON DuckDB Files for Agentic Search

**Date:** 2026-08-29
**Status:** ✅ Implemented 2026-08-29 (see notes below)

---

## 1. Goal

Build a set of **DuckDB database files** from the extracted data so an LLM agent can run
fast, structured searches over all FIA F1 documents and synthesize answers.

- **Markdown DuckDBs — one per year** (`markdown_2020.duckdb` … `markdown_2026.duckdb`):
  every `.md` document for that year, one row per document, with the **full markdown text**
  and **denormalized metadata columns** on the same row (no joins needed by the agent).
- **JSON DuckDBs — one per year *and* one combined** (`json_2020.duckdb` … `json_2026.duckdb`
  plus `json_all.duckdb`): every `.json` sidecar (raw JSON preserved) with the same
  flattened queryable columns.

### Decisions locked in during clarification

| Question | Decision |
|---|---|
| Search model | **SQL-first hybrid**: LLM uses SQL (ILIKE on full text + filters on metadata columns) for both structured and open-ended questions; it can also read the original `.md` files from disk. **No vector DB, no chunking, no RAG infra.** |
| JSON layout | **Both** per-year JSON DBs **and** one combined JSON DB. |
| Relation to `fia_rag/` | **New, self-contained pipeline** at repo root. `fia_rag/` stays untouched. |
| Embeddings | **None.** Search optimized via thoughtful columns + indexes. |
| Markdown row shape | **One row per document**, full markdown text + all metadata **denormalized** on the row. |
| Refresh | **Incremental upsert** keyed on `source_hash` (idempotent; safe to run after every scrape). |
| Agent access | **DuckDB-WASM** — schema must avoid non-WASM extensions (no FTS, no vector, keep to core SQL types). |
| File layout | New `duckdb/` folder at repo root; DB files under `duckdb/data/`. |
| Scope | **Years 2018–2026 only.** Excluded: `extracted/regs/`, `extracted/*/images/`, `processed_documents/`. |

---

## 2. Data inventory (verified)

| Year | Events | Markdown files | JSON sidecars |
|------|-------|---------------|---------------|
| 2018 | 21 | 1,364 | 1,364 |
| 2019 | 21 | 951 | 951 |
| 2020 | 18 | 836 | 836 |
| 2021 | 21 | 1,046 | 1,046 |
| 2022 | 22 | 1,207 | 1,207 |
| 2023 | 22 | 1,274 | 1,274 |
| 2024 | 25 | 1,376 | 1,376 |
| 2025 | 25 | 1,366 | 1,366 |
| 2026 | 12 | 880 | 880 |
| **Total** | | **10,311** | **10,311** |

- Source of truth for "what exists": `extracted/manifest.json` (10,116 entries; only
  `success: true` entries are ingested). Each entry carries `source_hash`, `doc_type`,
  `pages`, `tables_extracted`, `extracted_at`.
- Files live at `extracted/{year}/{event}/{pdf_stem}.md` and `.json` (get sibling JSON).
- `doc_type` distribution (30 types): classification 1,306 · other 1,263 · summons 1,013 ·
  infringement 812 · race-director-note 771 · offence 769 · scrutineering 763 · decision 611 ·
  procedure 447 · starting-grid 419 · pu-elements 375 · deleted-lap-times 324 · parc-ferme 269 ·
  circuit-map 191 · … (full list in `extracted/manifest.json`).
- Estimated size of markdown-only data per year: **~5–15 MB** (total extracted incl. images is
  ~800 MB; images dominate). JSON per year: **~1–3 MB**. Trivial for DuckDB and for
  DuckDB-WASM to fetch over HTTP.

---

## 3. File layout

```
duckdb/
├── PLAN.md                 # this document
├── README.md               # schema reference + LLM query cookbook (for the agent)
├── config.py               # paths, year list, table/column constants
├── schema.py               # shared DDL (CREATE TABLE / CREATE INDEX statements)
├── build_markdown.py       # builds/updates markdown_{year}.duckdb (CLI)
├── build_json.py           # builds/updates json_{year}.duckdb + json_all.duckdb (CLI)
├── verify.py               # QA: row counts vs disk, idempotency, sample queries
├── data/
│   ├── .gitignore          # ignores *.duckdb (binary, regenerable)
│   ├── markdown_2018.duckdb … markdown_2026.duckdb
│   ├── json_2018.duckdb … json_2026.duckdb
│   └── json_all.duckdb
└── tests/                  # pytest tests (later milestone)
```

Dependency: add `duckdb>=1.2` to root `pyproject.toml` dependencies (currently absent).
Build with `uv run duckdb/build_markdown.py --all`.

---

## 4. Schemas

### 4.1 `markdown_{year}.duckdb` — table `documents` (one row per `.md`)

| Column | Type | Source | Notes |
|---|---|---|---|
| `manifest_key` | `VARCHAR` | manifest key | PRIMARY KEY, e.g. `documents/2020/belgian-grand-prix/decision-...pdf` |
| `source_hash` | `VARCHAR` | manifest | upsert key: `sha256:...` |
| `year` | `SMALLINT` | path | equals the DB's year |
| `event` | `VARCHAR` | path | event slug, e.g. `belgian-grand-prix` |
| `pdf_stem` | `VARCHAR` | path | filename without `.pdf` |
| `doc_type` | `VARCHAR` | manifest | one of 30 types |
| `title` | `VARCHAR` | sidecar | may be null |
| `date` | `DATE` | sidecar | parsed from `"2020-08-30"`; null-safe |
| `doc_number` | `VARCHAR` | sidecar | kept VARCHAR ("41", "16a", …) |
| `time` | `VARCHAR` | sidecar | `HH:MM` |
| `event_name` | `VARCHAR` | sidecar | free-text, sometimes noisy |
| `sender` | `VARCHAR` | sidecar `from` | renamed to avoid SQL keyword |
| `recipient` | `VARCHAR` | sidecar `to` | |
| `pages` | `INTEGER` | manifest | |
| `tables_extracted` | `INTEGER` | manifest | |
| `images_extracted` | `INTEGER` | manifest | |
| `extracted_at` | `TIMESTAMPTZ` | manifest | ISO 8601 |
| `content` | `TEXT` | `.md` file | **full markdown text** (no images) |
| `char_len` | `INTEGER` | computed | `length(content)` — lets the agent size documents |
| `ingested_at` | `TIMESTAMP` | computed | `now()` default |

**Indexes** (persisted in the file, honored by WASM):

```sql
CREATE INDEX IF NOT EXISTS idx_docs_event    ON documents(event);
CREATE INDEX IF NOT EXISTS idx_docs_doc_type ON documents(doc_type);
CREATE INDEX IF NOT EXISTS idx_docs_date     ON documents(date);
CREATE INDEX IF NOT EXISTS idx_docs_hash     ON documents(source_hash); -- upsert lookups
```

**Why this is search-optimal without vectors/FTS:**

- Equality/range filters (`event`, `doc_type`, `date`, `year`) use the B-tree indexes.
- Open-ended text search = `content ILIKE '%verstappen%unsafe%release%'` — a columnar scan
  over ~5–15 MB/year is sub-second in DuckDB; no extension needed → WASM-safe.
- Everything the agent needs is on one row: filter → skim `content` → cite `source_hash` /
  `manifest_key` / `event` in the answer.
- `char_len` lets the agent pre-size context (skip/award large documents).

### 4.2 `json_{year}.duckdb` + `json_all.duckdb` — table `documents`

Same shape as 4.1 **minus `content`/`char_len`**, plus the raw sidecar:

| Column | Type | Notes |
|---|---|---|
| (all 4.1 metadata columns, source = sidecar) | | `title, date, doc_number, time, event_name, sender, recipient, ...` |
| `data` | `JSON` | **raw sidecar JSON** (full fidelity, incl. `tables[]` bbox info) |
| `ingested_at` | `TIMESTAMP` | |

Same indexes (`event`, `doc_type`, `date`, `source_hash`).

- **JSON type caveat:** DuckDB-WASM bundles the `json` extension in its default builds, so
  `data` is queryable with `json_extract_*`. If a WASM build without JSON support is ever
  used, query the flattened columns instead — which exist precisely for this reason. (If it
  becomes a real problem, `data` can be `VARCHAR`; the build script keeps the DDL in one place
  in `schema.py` to make that a one-line change.)
- `json_all.duckdb` has the same single `documents` table with all years; `year` column
  disambiguates.

---

## 5. Build pipeline

### 5.1 Shared logic (`config.py`, `schema.py`)

- `YEARS = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]`
- `EXTRACTED_ROOT = ../extracted`, `MANIFEST = ../extracted/manifest.json`, `DATA_DIR = data/`
- `DDL_MARKDOWN`, `DDL_JSON`, `INDEXES` — single source of truth for both build scripts.
- `flatten_sidecar(json_obj) -> dict` — maps sidecar fields to schema columns, parses
  `date` safely (`datetime.date.fromisoformat` with `None` on failure).

### 5.2 `build_markdown.py`

```
usage: uv run duckdb/build_markdown.py [--year 2020] [--all] [--dry-run] [--force]
```

1. Load `extracted/manifest.json` once; keep entries with `success == true` that have a `.md`
   file on disk.
2. For each target year file `markdown_{year}.duckdb`:
   - Open DuckDB (read-write), `CREATE TABLE IF NOT EXISTS` (+ indexes).
   - Load existing `(manifest_key, source_hash)` into memory (~1–2k rows/year — trivial).
   - **Diff per document:**
     - hash unknown → `INSERT`
     - hash differs → `UPDATE` (row replaced with fresh md/metadata)
     - hash unchanged → skip
   - `INSERT OR REPLACE` on `manifest_key`; one transaction per year file, then close.
3. `--dry-run`: report inserts/updates/skips without writing. `--force`: drop + recreate
   before ingesting (deterministic rebuild for schema changes).
4. Print per-year summary: `inserted=N updated=M unchanged=K total=T`.

**Idempotency:** running twice changes nothing (hash diff is empty). Safe to run after every
scrape/CI extraction run; only new/changed docs are written.

### 5.3 `build_json.py`

```
usage: uv run duckdb/build_json.py [--year 2020] [--all] [--dry-run] [--force]
```

1. Same flow as markdown, driven by the `.json` sidecar files (indexed by manifest key in
   the manifest). `source_hash` read from the sidecar itself and cross-checked against the
   manifest (`extracted_at`, doc_type, etc.).
2. Writes `json_{year}.duckdb` per year, then maintains `json_all.duckdb` as a superset:
   after per-year sync, upserts the same rows into the combined file (same diff, so it's
   cheap).

### 5.4 Failure handling

- Missing `.md` (manifest says success but file gone) → log `SKIP`, never crash the batch;
  counts still reported. `--prune` flag (future) can remove rows whose files disappeared.
- Malformed sidecar JSON → log + skip that row; batch continues.
- Every year file is written in one transaction → a crash mid-run leaves either the old or
  the new state, never a torn file.

---

## 6. Verification (`verify.py` + pytest)

| Check | Expectation |
|---|---|
| Row count per `markdown_{year}` | == `.md` count on disk for that year |
| Row count per `json_{year}` / `json_all` | == sidecar count (all: 10,311) |
| No row with NULL `content` | every markdown row has text |
| `source_hash` uniqueness | no duplicate hashes |
| Spot check 2020 Belgian GP decision | `title`, `date=2020-08-30`, `doc_number`, `content` starts with `"# From The Stewards"` |
| Idempotency | second build run reports 0 inserts / 0 updates |
| Sample agent queries (see §7) | correct results on known facts |
| WASM sanity | open file with `duckdb` CLI, run the sample queries (same SQL surface WASM uses) |

---

## 7. Query patterns the agent will use (cookbook → `duckdb/README.md`)

Presented to the LLM in `duckdb/README.md` so it writes correct SQL immediately:

```sql
-- Open-ended: "what happened with Leclerc's impeding at Spa 2020?"
SELECT event, title, doc_type, date, substring(content, 1, 1200) AS excerpt
FROM documents
WHERE event = 'belgian-grand-prix' AND content ILIKE '%leclerc%impeding%';

-- Analytical: "how many decisions per event in 2020?"
SELECT event, count(*) FROM documents WHERE doc_type = 'decision' GROUP BY event ORDER BY 2 DESC;

-- Analytical across years: "most common offences 2018-2026"
SELECT doc_type, count(*) FROM documents GROUP BY doc_type ORDER BY 2 DESC;   -- json_all

-- Filter + full text: "summons for Hamilton in 2021 with tyre temps"
SELECT date, title, substring(content,1,800)
FROM documents WHERE doc_type = 'summons'
  AND content ILIKE '%hamilton%' AND content ILIKE '%tyre%';

-- Raw sidecar lookups: "tables extracted from final race classification 2020 Bahrain"
SELECT title, json_extract_string(data,'$.tables') FROM documents
WHERE event = 'bahrain-grand-prix' AND title ILIKE '%final race classification%';
```

Conventions the agent must follow (documented in README):
- Text search = `ILIKE '%term%'`; combine multiple terms with `AND` (DuckDB scans columns).
- Always add `WHERE year = <n>` when querying the combined JSON DB.
- Prefer `doc_type`, `event`, `date` predicates first (indexed) before ILIKE.
- Full content lives in `content`; a document can also be re-read from
  `../extracted/{year}/{event}/{pdf_stem}.md` via the recorded `pdf_stem`.

---

## 8. WASM considerations (explicitly designed for)

1. **No custom extensions** in schema or queries: no FTS (`fts_main_*`), no vector, no
   spatial. Everything is core SQL types (`VARCHAR/INTEGER/SMALLINT/DATE/TIMESTAMP/JSON`).
2. **Indexes are stored in the file** and used automatically by WASM reads.
3. **File sizes stay small**: per-year markdown ~5–15 MB, JSON ~1–3 MB — easily fetched over
   HTTP into DuckDB-WASM. `json_all.duckdb` (~20–30 MB) is the only bigger file.
4. `data` JSON column queryable in WASM via the bundled `json` extension — but all metadata
   is also flattened, so even an extension-less WASM build works.

---

## 9. Rollout steps (ordered)

1. **Scaffold** — create `duckdb/` folder, `config.py`, `schema.py`, `data/.gitignore`
   (`*.duckdb`), add `duckdb>=1.2` to `pyproject.toml`.
2. **`build_markdown.py`** — implement diff/upsert; run `--all --dry-run`; then run for real
   (all 9 years).
3. **`build_json.py`** — implement per-year + combined; run all + combined.
4. **`verify.py`** — run all checks from §6; fix discrepancies (counts, nulls, spot checks).
5. **`README.md` cookbook** — schema reference + query patterns (§7) for the agent.
6. **Idempotency + incremental test** — run both builders a second time → expect 0 changes;
   simulate a new doc (copy an existing md/json under a new key) → only that row appears.
7. **(Optional) CI hook** — extend `.github/workflows/scrape.yml` to run the builders after
   extraction and commit/upload the refreshed `.duckdb` files (deliberately deferred: ask
   before adding).
8. **(Optional) pytest** — `duckdb/tests/` for schema + build logic.

## 10. Implementation notes (2026-08-29)

- **Ship**: `duckdb/` now contains `config.py`, `schema.py`, `build_markdown.py`,
  `build_json.py`, `verify.py`, `README.md` (cookbook), `data/.gitignore`; 19
  `.duckdb` files built (9 markdown, 9 JSON, 1 combined). `uv run
  duckdb/build_markdown.py --all` / `build_json.py --all` / `verify.py` all work.
  Added `duckdb>=1.2` to `pyproject.toml`; created `.python-version` (3.13) so
  `uv run` honors the repo's Python pin instead of the system 3.14 (which can't
  build the `regex` wheel pinned in the lock).
- **Counts**: totals == **10,300** per sidecar type (plan said 10,311; the
  2026 count is 880 on disk — the manifest's 684 snapshot is stale, so the
  builders ingest from disk, using the manifest for `success` gating and as
  the cross-check source for duplicate fields). `json_all` is ~50 MB (plan
  estimate 20–30 MB) because it stores full raw sidecars — fine over HTTP.
- **Duplicate `source_hash`**: the FIA corpus genuinely contains the same PDF
  uploaded twice (`foo.pdf` / `foo_1.pdf`). The verify check is therefore
  “all rows sharing a hash carry identical content/data”, not “no duplicate
  hashes”.
- **Sample query**: the §7 “Leclerc impeding Spa 2020” example returns 0 rows
  (no 2020 Belgian GP doc contains both terms — Leclerc and impeding appear in
  separate documents); verify.py compares DB results against a direct disk
  scan instead of hardcoding a count.
- **Future/optional (not done)**: `--prune` flag, CI hook (scrape workflow),
  pytest suite, `regs.duckdb` — all deliberately deferred.

---

## 11. Out of scope (explicit non-goals)

- ✅ **Excluded:** `extracted/regs/`, images, `processed_documents/`, `documents/`.
- No vector embeddings, no chunking, no BM25/FTS, no rank fusion (per decision).
- No changes to `fia_rag/` (Django app stays as-is).
- No query-serving layer (WASM handles it; we only ship optimized `.duckdb` files + docs).
- 2018/2019 archive data IS included (present in `extracted/`).
- `regs/` regulations DB could be added later as `regs.duckdb` if wanted (easy extension, out
  of scope now).