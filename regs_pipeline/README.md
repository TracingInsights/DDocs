# FIA F1 Regulations — Parquet Corpus for Agentic Search

212 regulation PDFs (`regs/`, 2018–2027) extracted to markdown + JSON sidecars
(`extracted/`) and shipped as per-year parquet (`data/<year>/`). Query with
**plain SQL only** — no vector DB, no FTS extension, DuckDB-WASM safe.

```
regs_pipeline/
├── PLAN.md                  ← design doc (decisions, schemas, verification)
├── README.md                ← this file (schema + SQL cookbook)
├── config.py                ← paths, color map, thresholds, ligature rules
├── schema.py                ← column definitions (single source of truth)
├── extract_markdown.py      ← PDF → .md + .json sidecar (cached by source_hash)
├── build_articles.py        ← sidecars → staging duckdb (data/_work/)
├── convert_to_parquet.py    ← staging → per-year parquet (verified export)
├── verify.py                ← sanity battery (PLAN.md §9)
├── extracted/<year>/<doc-slug>/iss-<NN>/*.md|.json
└── data/<year>/{articles,documents,regs_json}.parquet
```

## Quick start

```sql
-- DuckDB (CLI, Python, or WASM): point at one year or glob all years
CREATE VIEW articles  AS SELECT * FROM 'regs_pipeline/data/2026/articles.parquet';
CREATE VIEW articles  AS SELECT * FROM 'regs_pipeline/data/*/articles.parquet';  -- all years
SELECT article_number, article_title, char_len
FROM articles WHERE is_latest AND regulation_type = 'sporting' LIMIT 10;
```

## Tables

### `articles.parquet` — the search table (one row per level-2 article section)

| Column | Type | Notes |
|---|---|---|
| `row_id` | VARCHAR | PK: `{doc_id}#{article_number}#{occurrence}` |
| `doc_id` | VARCHAR | `{year}/{doc-slug}/iss-{NN}` |
| `year` | SMALLINT | **actual regulation year** (parsed from the cover, not the folder) |
| `section` | VARCHAR | `A`–`F` for 2026+ regs; NULL for historical |
| `regulation_type` | VARCHAR | `general` / `sporting` / `technical` / `financial-teams` / `financial-pu` / `operational` (+ `-pu` variants) |
| `section_name` | VARCHAR | e.g. `Sporting Regulations` |
| `issue` | SMALLINT | |
| `is_latest` | BOOLEAN | exactly one true per `(year, regulation_type)` |
| `article_number` | VARCHAR | e.g. `B1.1`, `5.4`; `FRONT_MATTER`; `APPENDIX-n` |
| `article_title` | VARCHAR | e.g. `General Principles & Provisions`; may be empty for inline-number families (2018–2025 sporting/technical, financial) |
| `parent_article` | VARCHAR | e.g. `B1 — ORGANISATION OF A COMPETITION` |
| `occurrence` | SMALLINT | 0 unless the number repeats within the doc (FIA renumbering bugs exist) |
| `printed_page_start/end` | SMALLINT | footer page numbers (citable) |
| `pdf_page_start/end` | SMALLINT | 1-based PDF pages |
| `content` | TEXT | markdown: tags, `[PAGE n]` markers, pipe tables |
| `char_len` | INTEGER | pre-size context before fetching |
| `has_changes` `has_removals` `has_governance` `has_reference` `has_comment` | BOOLEAN | fast filters, no regex needed |
| `n_changed` `n_removed` | INTEGER | tagged-span counts (for "most changed articles") |
| `clauses` | JSON | `[{"number":"B1.1.1","offset":0,"length":214}, …]` — offsets are relative to `content` |
| `source_pdf` `source_hash` `extracted_at` `ingested_at` | | provenance |

### `documents.parquet` — one row per PDF (fallback / full context)

`doc_id` (PK) + same identity columns as above, plus `title`, `status` (e.g.
`PUBLISHED`), `issue_date`, `wmsc_approval_date` (DATE, NULL when absent),
`pages`, full-text `content`, `char_len`, `tables_extracted`, `stats` (JSON:
span/tag/replacement counts), provenance columns.

### `regs_json.parquet` — raw sidecars

Flattened queryable metadata (same identity columns) plus `data` JSON = the
raw sidecar, full fidelity: `toc[]`, `convention` (per-document color legend —
meanings drift by year), `tables[]` (raw cell grids + bboxes), `articles[]`
offsets, `stats`.

## Tag legend (inside `content`)

The FIA encodes change-tracking in **text color**, not markup. The extractor
resolves each span against the document's own legend page and emits:

| Tag | Meaning | FIA color |
|---|---|---|
| `[CHANGED]…[/CHANGED]` | Changes vs previous issue (WMSC-approved) | pink/magenta |
| `[REMOVED]…[/REMOVED]` | Struck-out (deleted) text | drawn strike rectangles (detected geometrically) |
| `[GOVERNANCE]…[/GOVERNANCE]` | Governance / Advisory Committee info | dark red |
| `[REFERENCE]…[/REFERENCE]` | Reference to other FIA F1 documents | orange |
| `[COMMENT]…[/COMMENT]` | Non-binding comments / explanations | green |
| `[PAGE n]` | Printed page number marker at each page break | — |

- Precedence: `[REMOVED]` is always the outer tag; struck colored text keeps an
  inner tag only if non-pink: `[REMOVED][GOVERNANCE]…[/GOVERNANCE][/REMOVED]`.
- Hyperlink blue and gray are informational (not change markers) → untagged.
- Historical docs (2018–2025) mostly have **no color convention** → tags simply
  absent, all booleans false.
- The per-document legend text is in `regs_json.data.convention` — check it
  when tag semantics matter for a specific old document.

## The two-stage query pattern (context-size contract)

**Stage 1 (default):** search `articles.parquet`. Filter by metadata first
(`year`, `regulation_type`, `is_latest`), then `content ILIKE`. Use `char_len`
to budget how many rows you pull into context. Cite `article_number`,
`section_name`, `printed_page_start`.

**Stage 2 (rare):** pull full text from `documents.parquet` only when
cross-article context is genuinely needed (a full doc can be 100+ KB).

## Cookbook

```sql
-- "What does rule X say?" (latest issue)
SELECT article_number, article_title, content
FROM 'regs_pipeline/data/*/articles.parquet'
WHERE article_number = 'B3.4' AND is_latest;

-- "What changed in the latest Sporting issue?"
SELECT article_number, article_title, n_changed, n_removed
FROM 'regs_pipeline/data/*/articles.parquet'
WHERE has_changes AND is_latest AND regulation_type = 'sporting'
ORDER BY n_changed DESC;

-- "Diff issue 4 vs issue 5" — fetch the same article from both issues
SELECT issue, content FROM 'regs_pipeline/data/2026/articles.parquet'
WHERE regulation_type = 'sporting' AND article_number = 'B3.4'
ORDER BY issue;   -- compare [CHANGED]/[REMOVED] spans

-- "What was deleted this year?"
SELECT article_number, content
FROM 'regs_pipeline/data/2026/articles.parquet'
WHERE has_removals AND is_latest;

-- "Who governs article X?"
SELECT article_number, content
FROM 'regs_pipeline/data/*/articles.parquet'
WHERE has_governance AND article_number LIKE 'B1%' AND is_latest;

-- "Most-changed articles in 2026"
SELECT article_number, article_title, n_changed
FROM 'regs_pipeline/data/2026/articles.parquet'
WHERE is_latest ORDER BY n_changed DESC LIMIT 20;

-- Historical full-text search
SELECT doc_id, article_number, char_len
FROM 'regs_pipeline/data/2018/articles.parquet'
WHERE content ILIKE '%halo%';

-- Clause-level lookup inside an article
SELECT article_number, c->>'number' AS clause,
       substr(content, (c->>'offset')::INT + 1, (c->>'length')::INT) AS clause_text
FROM 'regs_pipeline/data/2026/articles.parquet',
     LATERAL json_each(clauses) AS t(c)
WHERE row_id = '2026/section-b-sporting/iss-05#B1.1#0';

-- Which documents exist?
SELECT doc_id, title, issue, is_latest, pages
FROM 'regs_pipeline/data/*/documents.parquet'
ORDER BY year, regulation_type, issue;
```

## WASM / tooling notes

- Core SQL types only (VARCHAR/SMALLINT/INTEGER/DATE/TIMESTAMP/JSON/BOOLEAN/
  TEXT) — `read_parquet()` + `CREATE VIEW`, no extensions required.
- Filter `year` first and read `data/<year>/` directly when possible; glob
  `data/*/articles.parquet` only for cross-year queries (no hive partitioning
  — the folder name is the partition, the `year` column is inside the files).
- `clauses` / `stats` / `data` are JSON columns: `json_extract`,
  `->>`, `json_each` all work.

## Regenerating

```bash
uv run regs_pipeline/extract_markdown.py            # all PDFs (cached by hash+version)
uv run regs_pipeline/extract_markdown.py --force --pdf regs/…/one.pdf
uv run regs_pipeline/build_articles.py              # sidecars → staging duckdb
uv run regs_pipeline/convert_to_parquet.py          # staging → data/<year>/*.parquet (verified)
uv run regs_pipeline/verify.py                      # sanity battery
```
