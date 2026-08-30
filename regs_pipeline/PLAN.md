# Implementation Plan — FIA Regulations → Markdown/JSON → Parquet for Agentic Search

**Date:** 2026-08-30
**Status:** 📋 Plan approved in clarification; not yet implemented

---

## 1. Goal

Convert the FIA F1 regulation PDFs in `regs/` into **markdown + JSON sidecars**, then into
**per-year parquet files** so the LLM agent at `app.tracinginsights.com/chat` can answer
regulation questions with **plain SQL only** — no vector DB, no FTS extension, no RAG infra —
mirroring the proven `duckdb/` pipeline used for decision documents.

Three problems make regs different from decision docs, and each has a designed solution:

| Problem | Solution |
|---|---|
| Regs are huge (e.g. 96 pages) — one row per doc overflows LLM context | **`articles.parquet`: one row per level-2 article section** (~0.5–3 KB), with a `documents.parquet` fallback table for full text |
| Changes are encoded in **text color**, not text | PyMuPDF per-span color extraction → semantic tags: `[CHANGED]`, `[GOVERNANCE]`, `[REFERENCE]`, `[COMMENT]` |
| Deletions are **struck-out text**, not flagged in fonts | Geometric detection of FIA-drawn strike rectangles via `page.get_drawings()` → `[REMOVED]` tags |
| Large docs need page citations | Visible `[PAGE n]` markers in markdown + printed/pdf page columns |

---

## 2. Decisions locked in during clarification

| # | Question | Decision |
|---|---|---|
| 1 | Which issues to process | **All issues** of every document, with an `is_latest` flag per (year, section) so queries default to latest but can reach back |
| 2 | Row granularity | **Two tables**: `articles` (search) + `documents` (full text) |
| 3 | Tag syntax | Option b: `[CHANGED]`, `[REMOVED]`, `[GOVERNANCE]`, `[REFERENCE]`, `[COMMENT]`. Boolean flag columns per row. **No `content_clean` duplicate** |
| 4 | Page markers | **Visible `[PAGE n]`** (printed page number) inline; both `pdf_page_*` and `printed_page_*` columns |
| 5 | JSON structure | **Per-document sidecars only** (no per-article JSON files — article facts are columns in `articles.parquet`). Single parquet per year + year folders |
| 6 | Article row level | **Level-2 sections** (e.g. `B1.1` + title + all clauses) **plus a `clauses` JSON column** (clause numbers + offsets) |
| 7 | Layout | New `regs_pipeline/` folder; extracted md/json inside `regs_pipeline/extracted/`; parquet in `regs_pipeline/data/<year>/`; **no combined `_all` files** |
| 8 | Markdown content | Strip running headers/footers; keep front matter as a `FRONT_MATTER` article row; keep appendices; normalize ligature bugs |
| 9 | Tables | **Both**: markdown pipe tables (with tags inside cells) in `content` + raw cell data in sidecar `tables[]` |
| 10 | Refresh | **Hybrid**: extraction cached per-PDF by `source_hash`; parquet rebuild is always full (fast, keeps `is_latest` correct) |
| 11 | Document scope | **Everything in `regs/`**, including historical (2018+) docs; the **actual document year** (parsed from title/filename) drives `year` and the output folder |
| 12 | Agent integration | Schema + SQL cookbook in `regs_pipeline/README.md` (same pattern as `duckdb/README.md`); no separate system-prompt snippet |

---

## 3. Verified PDF facts (from inspection)

Inspected: `regs/2026/section-a/fia-2026-f1-regulations-section-b-sporting-iss-05-2026-02-27.pdf` (96 pages).

### 3.1 Color convention (legend on cover page, verified against body text)

| Meaning | Span color (hex int) | Tag |
|---|---|---|
| Text unchanged from previous issue | `0x000000` | *(none)* |
| Changes vs previous issue, WMSC-approved | `0xff00ff` (pink/magenta) | `[CHANGED]…[/CHANGED]` |
| Governance / Advisory Committee info | `0xb02418`, `0xc00000` (reds) | `[GOVERNANCE]…[/GOVERNANCE]` |
| Reference to FIA F1 documents | `0xff9900` (orange) | `[REFERENCE]…[/REFERENCE]` |
| Non-binding comments / explanations | `0x00b050` (green) | `[COMMENT]…[/COMMENT]` |
| Struck-out (removed) text | drawn rects, see 3.2 | `[REMOVED]…[/REMOVED]` |
| Page furniture (headers, blue section banner, white text) | `0x002d5f`, `0xffffff` | stripped |

Colors may vary slightly across documents/years → **match by nearest hue**, not exact int
(see §6.3). The legend page of each document is parsed and stored in the sidecar
(`convention` field) so tag semantics are always traceable per document.

### 3.2 Strikethrough mechanics

- **Not** a font flag — PyMuPDF span flags don't report it.
- FIA draws it as **thin filled rectangles** (`page.get_drawings()`, type `f`,
  fill ≈ `(1.0, 0.0, 1.0)` magenta, height ≈ 0.4–0.6 pt, often as a **pair** of parallel
  rects) overlapping the vertical middle of the struck text.
- Detection: for each text line, collect thin horizontal filled rects (height ≤ ~1.5 pt,
  width ≥ ~3 pt) whose y-range intersects the line's mid-band; a span is `[REMOVED]` if a
  strike rect horizontally overlaps ≥ ~60% of the span width. Tune thresholds on samples;
  log per-doc strike counts for sanity (this doc: ~88 struck spans).
- A span can be **both** pink and struck (removed text is also magenta) — `[REMOVED]` wins
  as the outer tag; do not nest.

### 3.3 Article structure (font-based, verified)

```
ARTICLE B1: ORGANISATION OF A COMPETITION   ← 12 pt bold  → level-1 (article group)
B1.1  General Principles & Provisions       ← 11 pt bold number + bold title → level-2 (ROW UNIT)
B1.1.1  Competitions are reserved for…      ← bold inline number, regular body → level-3 (clause)
```

- Row unit = level-2 (`B1.1`), matching how the FIA titles and cross-references rules.
- Clauses (`B1.1.1`, lettered sub-items `a.`, `b.`…) stay inline in `content` and are
  indexed in the `clauses` JSON column.
- Historical docs (2018–2025) have **no section letter** (plain `5.4` numbering) and no
  color convention — parser must handle both (§6.4).
- Known quirk: this PDF contains duplicate numbers (two `B1.4` in TOC) — keep both rows,
  disambiguate with `occurrence` index (§6.5).

### 3.4 Encoding quirks

- `ffi` ligature extracts as `=`: "O=icials" (should be "Officials"). Also `ﬁ`/`ﬂ`
  ligature codepoints appear. → Normalization pass (§6.6) with a verified replacement
  table; count replacements per doc into the sidecar `stats`.

---

## 4. Output layout

```
regs_pipeline/
├── PLAN.md                     ← this file
├── README.md                   ← schema docs + SQL cookbook (written at build time)
├── config.py                   ← paths, color map, thresholds, regexes
├── schema.py                   ← parquet column definitions (articles / documents / regs_json)
├── extract_markdown.py         ← PDF → .md + .json sidecar (cached by source_hash)
├── build_articles.py           ← sidecars → article rows
├── convert_to_parquet.py       ← rows → per-year parquet (zstd)
├── verify.py                   ← sanity checks (§9)
├── extracted/
│   └── <year>/<doc-slug>/iss-<NN>/       e.g. extracted/2026/section-b-sporting/iss-05/
│       ├── fia-…-iss-05-….md
│       └── fia-…-iss-05-….json
└── data/
    └── <year>/                 e.g. data/2026/
        ├── articles.parquet
        ├── documents.parquet
        └── regs_json.parquet
```

- `<year>` = **actual regulation year** parsed from document title/filename (2018 regs →
  `data/2018/`), not the source folder year (the manifest's `year` is wrong for some files).
- `doc-slug`: normalized section name, e.g. `section-b-sporting`, `section-c-technical`,
  `sporting-2018` for historical docs.

---

## 5. Schemas

Core SQL types only (`VARCHAR/SMALLINT/INTEGER/DATE/TIMESTAMP/JSON/BOOLEAN/TEXT`) —
DuckDB-WASM safe, no extensions. zstd compression.

### 5.1 `articles.parquet` — the search table (one row per level-2 article section)

| Column | Type | Notes |
|---|---|---|
| `row_id` | VARCHAR (PK) | `{doc_id}#{article_number}#{occurrence}` |
| `doc_id` | VARCHAR | `{year}/{doc-slug}/iss-{NN}` |
| `year` | SMALLINT | actual regulation year |
| `section` | VARCHAR | `A`–`F` for 2026+ regs; NULL for historical docs |
| `regulation_type` | VARCHAR | `general`/`sporting`/`technical`/`financial-teams`/`financial-pu`/`operational` |
| `section_name` | VARCHAR | e.g. `Sporting Regulations` |
| `issue` | SMALLINT | |
| `is_latest` | BOOLEAN | exactly one true per (year, regulation_type) |
| `article_number` | VARCHAR | e.g. `B1.1`; `FRONT_MATTER`; `APPENDIX-n` |
| `article_title` | VARCHAR | e.g. `General Principles & Provisions` |
| `parent_article` | VARCHAR | e.g. `B1 — ORGANISATION OF A COMPETITION` |
| `occurrence` | SMALLINT | 0 unless the number repeats within the doc |
| `printed_page_start/end` | SMALLINT | footer page numbers |
| `pdf_page_start/end` | SMALLINT | 1-based PDF pages |
| `content` | TEXT | markdown: tags, `[PAGE n]` markers, pipe tables |
| `char_len` | INTEGER | pre-size context before fetching |
| `has_changes` / `has_removals` / `has_governance` / `has_reference` / `has_comment` | BOOLEAN | fast filters, no regex needed |
| `n_changed` / `n_removed` | INTEGER | tagged-span counts (for "most changed articles") |
| `clauses` | JSON | `[{"number":"B1.1.1","offset":0,"length":214}, …]` |
| `source_pdf` | VARCHAR | path under `regs/` |
| `source_hash` | VARCHAR | `sha256:…` |
| `extracted_at` | TIMESTAMPTZ | |
| `ingested_at` | TIMESTAMP | |

### 5.2 `documents.parquet` — one row per PDF (fallback / full context)

| Column | Type | Notes |
|---|---|---|
| `doc_id` (PK), `year`, `section`, `regulation_type`, `section_name`, `issue`, `is_latest` | as above | |
| `title` | VARCHAR | e.g. `2026 Formula 1: Sporting Regulations` |
| `status` | VARCHAR | e.g. `PUBLISHED` (from cover) |
| `issue_date` / `wmsc_approval_date` | DATE | from cover (NULL when absent) |
| `pages` | INTEGER | PDF page count |
| `content` | TEXT | full markdown (all articles, tags, page markers) |
| `char_len` | INTEGER | |
| `tables_extracted` | INTEGER | |
| `stats` | JSON | span/tag/replacement counts |
| `source_pdf`, `source_hash`, `extracted_at`, `ingested_at` | as above | |

### 5.3 `regs_json.parquet` — sidecars (mirrors `json_*.parquet` in `duckdb/`)

Flattened queryable columns (`doc_id`, `year`, `section`, `regulation_type`, `issue`,
`is_latest`, `title`, `status`, dates, `pages`, `source_hash`) **plus** `data` JSON = the
raw sidecar, full fidelity (incl. `toc[]`, `convention`, `tables[]` with cell/bbox info).

### 5.4 Sidecar JSON shape (`extracted/<year>/<slug>/iss-NN/*.json`)

```json
{
  "doc_id": "2026/section-b-sporting/iss-05",
  "title": "2026 Formula 1: Sporting Regulations",
  "year": 2026, "section": "B", "regulation_type": "sporting",
  "section_name": "Sporting Regulations",
  "issue": 5, "is_latest": true,
  "status": "PUBLISHED",
  "issue_date": "2026-02-27", "wmsc_approval_date": "2026-02-27",
  "pages": 96,
  "source_pdf": "regs/2026/section-a/fia-…-iss-05-….pdf",
  "source_hash": "sha256:…",
  "convention": {"pink": "changes vs Iss 04 …", "red": "…", "orange": "…", "green": "…"},
  "toc": [{"article": "B1.1", "title": "General Principles & Provisions", "printed_page": 4}],
  "tables": [{"page": 41, "bbox": […], "cells": [[…]]}],
  "stats": {"articles": 143, "changed_spans": 612, "removed_spans": 88,
            "governance_spans": 269, "reference_spans": 5, "comment_spans": 0,
            "ligature_fixes": 1210},
  "extracted_at": "…"
}
```

---

## 6. Extraction pipeline (`extract_markdown.py`)

### 6.1 Per-PDF flow

1. Hash PDF → skip if `extracted/**/{stem}.json` with matching `source_hash` exists
   (the hybrid cache from decision #10).
2. Parse cover page: title, section letter, issue number, status, dates, **convention
   legend** (regex per color line; tolerate wording drift across years).
3. Parse CONTENTS → `toc[]` (article number ↔ title ↔ printed page). Used for article
   boundary cross-checking, not as the primary splitter.
4. For each page: strip furniture (running header lines, footer ©-line, blue banner
   spans — colors `0x002d5f`/`0xffffff` and repeated-line heuristics).
5. Extract spans with color → tag assignment (§6.3); extract drawings → strike rects →
   `[REMOVED]` (§3.2); merge adjacent same-tag spans; emit `[PAGE n]` at each page break
   (printed number from footer; fall back to PDF page if unparseable).
6. Split body into articles by font-size rules (§3.3); slice into level-2 sections;
   record clause offsets; wrap tables as pipe tables (tags preserved inside cells) and
   stash raw cells in `tables[]`.
7. Normalize text (§6.6). Write `.md` (front matter + all articles) and `.json` sidecar.
8. Per-PDF `try/except`: failures logged to `extraction_errors` list in the run manifest;
   one bad PDF never blocks the batch.

### 6.2 Tag emission rules

- Order of precedence (outer → inner, never nested same-type): `[REMOVED]` >
  `[GOVERNANCE]`/`[REFERENCE]`/`[COMMENT]` > `[CHANGED]`.
- Whitespace-only spans ignored; tags merged across span/line breaks within a paragraph so
  output reads `[CHANGED]…multi-line sentence…[/CHANGED]`, not per-line fragments.
- Struck text keeps its inner color tag only if non-pink (rare; e.g. struck red governance
  note → `[REMOVED][GOVERNANCE]…[/GOVERNANCE][/REMOVED]`).

### 6.3 Color matching

Exact ints first; fallback = nearest neighbor in RGB space within a tolerance, classified
into {pink, red, orange, green, furniture, black}. Anything unclassifiable → plain text +
counted in `stats.unknown_color_spans` (surfaced by `verify.py`).

### 6.4 Historical documents (2018–2025)

- No section letters → `section = NULL`, article numbers plain (`5.4`), `regulation_type`
  parsed from title ("Sporting", "Technical", "Financial…").
- No color convention → tags simply absent; all booleans false. Parser must not require
  the legend.

### 6.5 Duplicate article numbers

Keep source order; `occurrence` = 0, 1, 2…; `row_id` unique via occurrence suffix.
Counted in `stats.duplicate_articles` and flagged by `verify.py` (FIA renumbering bugs
exist, e.g. two `B1.4`).

### 6.6 Text normalization

- `=` → `ffi` **only** in verified ligature contexts (build replacement list from sample
  docs; e.g. `O=icials`→`Officials`, `O=icial`→`Official`); `ﬁ`→`fi`, `ﬂ`→`fl`.
- Keep smart quotes/em-dashes as-is. Collapse >2 consecutive blank lines.
- Every replacement counted (`stats.ligature_fixes`) so regressions are visible.

---

## 7. Parquet build (`build_articles.py` + `convert_to_parquet.py`)

1. Scan all sidecars → compute `is_latest` = max `issue` per `(year, regulation_type)`
   (ties → later `issue_date`).
2. Slice each `.md` into article rows per sidecar structure → assemble `articles` rows.
3. Assemble `documents` rows (full md) and `regs_json` rows (sidecar).
4. Write `data/<year>/{articles,documents,regs_json}.parquet`, zstd, core types.
   Full rebuild every run (seconds; correctness of `is_latest` guaranteed).

---

## 8. README.md — agent-facing cookbook (built at implementation time)

Same pattern as `duckdb/README.md`. Contents:

1. File/table inventory + full schema docs (§5).
2. **Tag legend** — what `[CHANGED]`/`[REMOVED]`/`[GOVERNANCE]`/`[REFERENCE]`/`[COMMENT]`
   and `[PAGE n]` mean, with the FIA convention quoted.
3. **The two-stage query pattern** (the context-size contract):
   - Stage 1 (default): search `articles.parquet` with metadata filters + `ILIKE`;
     `char_len` to budget context; cite `section`, `article_number`, `printed_page_*`.
   - Stage 2 (rare): pull full text from `documents.parquet` only when cross-article
     context is genuinely needed.
4. Canned recipes:
   - *"What does rule X say?"* → `WHERE article_number = 'B3.4' AND is_latest`
   - *"What changed in the latest Sporting issue?"* → `WHERE has_changes AND is_latest AND regulation_type='sporting'`
   - *"Diff issue 4 vs 5"* → fetch same `article_number` from both issues, compare `[CHANGED]`/`[REMOVED]`
   - *"What was deleted this year?"* → `WHERE has_removals AND is_latest`
   - *"Who governs article X?"* → `WHERE has_governance AND article_number LIKE 'B1%'`
   - *"Most-changed articles in 2026"* → `ORDER BY n_changed DESC`
   - *Historical*: `WHERE year = 2018 AND content ILIKE '%halo%'`
5. WASM-safe notes: `read_parquet()` + `CREATE VIEW`, no extensions, `year` filter first
   (partition pruning over year folders via glob `data/*/articles.parquet` when needed).

---

## 9. Verification (`verify.py`)

| Check | Rule |
|---|---|
| Tag balance | every `[X]` has `[/X]` in every row; zero orphan tags |
| Page coverage | every PDF page 1..N appears as exactly one `[PAGE n]` in its doc |
| Article cross-check | level-2 row count per doc ≈ TOC entries (± appendix/front matter); mismatches reported |
| `is_latest` uniqueness | exactly one per (year, regulation_type) |
| Strikethrough sanity | `n_removed` > 0 for docs whose legend announces changes and issue > 1 |
| Unknown colors | `stats.unknown_color_spans` == 0 (or explicitly whitelisted) |
| Ligature sanity | spot-check banned patterns (`\w=\w` outside `=`-normal contexts) |
| Round-trip | DuckDB reads all parquet; row counts match sidecar counts; sample queries from README run green |
| Duplicates | `row_id` unique across all years |

---

## 10. Effort / risk notes

- **Extraction** is the only slow step (~100+ PDFs, some 300+ pages) — one-time cost of a
  few minutes; cached afterwards by `source_hash`.
- **Biggest risk**: strike-rect false positives/negatives and color-int drift across
  generator versions → mitigated by per-doc `stats`, `unknown_color_spans`, and the
  verify checks above; tune thresholds against 3–4 diverse sample docs before the full run.
- **Second risk**: font-size article detection varies for historical docs → font rules are
  per-doc *adaptive* (learn sizes from bold/numbered-line clusters) rather than hard-coded.
- Historical docs add years 2018–2025 folders; totals stay trivial (≪ duckdb/ corpus).
