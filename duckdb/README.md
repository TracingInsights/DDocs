# DuckDB files for agentic search

One row per FIA F1 document, denormalized so an LLM agent can run fast structured
searches with plain SQL (**no joins, no FTS, no vector DB, no RAG infra**).

| File | Contents |
|---|---|
| `markdown_YYYY.duckdb` | every `.md` document of that year — full markdown text + metadata on one row |
| `json_YYYY.duckdb` | the `.json` sidecars of that year — flattened metadata + raw sidecar in `data` |
| `json_all.duckdb` | combined table of all years (2018–2026); filter with `WHERE year = <n>` |

All files use **core SQL types only** (`VARCHAR/INTEGER/SMALLINT/DATE/TIMESTAMP/JSON`)
with B-tree indexes — compatible with DuckDB-WASM over HTTP (the bundled `json`
extension provides `json_extract_*`; every JSON field is also flattened, so even
an extension-less WASM build works).

## Schema — table `documents` (every file)

| Column | Type | Notes |
|---|---|---|
| `manifest_key` | VARCHAR (PK) | e.g. `documents/2020/belgian-grand-prix/decision-...pdf` |
| `source_hash` | VARCHAR | `sha256:...`, stable across re-extractions |
| `year` | SMALLINT | equals the DB's year; disambiguates `json_all` |
| `event` | VARCHAR | event slug, e.g. `belgian-grand-prix` |
| `pdf_stem` | VARCHAR | filename without `.pdf`; the `.md`/`.json` live at `../extracted/{year}/{event}/{pdf_stem}.{md,json}` |
| `doc_type` | VARCHAR | one of 30 types: classification, summons, infringement, decision, scrutineering, offence, … |
| `title` / `date` / `doc_number` / `time` / `event_name` / `sender` / `recipient` | VARCHAR/DATE/VARCHAR | from the sidecar (`sender` = sidecar `from`, `recipient` = sidecar `to`) |
| `pages` / `tables_extracted` / `images_extracted` | INTEGER | from the manifest/sidecar |
| `extracted_at` | TIMESTAMPTZ | when the PDF was extracted |
| `content` | TEXT | **markdown tables only** — full document text |
| `char_len` | INTEGER | markdown tables only — pre-size context, skip/award large documents |
| `data` | JSON | **JSON tables only** — raw sidecar (full fidelity incl. `tables[]` bbox info) |
| `ingested_at` | TIMESTAMP | when this row version was written |

Indexes (persisted, used automatically by WASM): `event`, `doc_type`, `date`, `source_hash`.

## Query cookbook (as an agent, write SQL like this)

```sql
-- Open-ended: "what happened with Leclerc's impeding at Spa 2020?"
SELECT event, title, doc_type, date, substring(content, 1, 1200) AS excerpt
FROM documents
WHERE event = 'belgian-grand-prix' AND content ILIKE '%leclerc%impeding%';

-- Analytical: "how many decisions per event in 2020?"
SELECT event, count(*) FROM documents WHERE doc_type = 'decision' GROUP BY event ORDER BY 2 DESC;

-- Analytical across years: "most common doc types 2018-2026" (run against json_all.duckdb)
SELECT doc_type, count(*) FROM documents GROUP BY doc_type ORDER BY 2 DESC;

-- Filter + full text: "summons for Hamilton in 2021 with tyre temps"
SELECT date, title, substring(content, 1, 800)
FROM documents WHERE doc_type = 'summons'
  AND content ILIKE '%hamilton%' AND content ILIKE '%tyre%';

-- Raw sidecar lookups: "tables extracted from final race classification 2020 Bahrain"
SELECT title, json_extract_string(data, '$.tables') FROM documents
WHERE event = 'bahrain-grand-prix' AND title ILIKE '%final race classification%';
```

### Conventions

- **Text search** = `content ILIKE '%term%'`; combine terms with `AND` (columnar scans are sub-second per year).
- **Always add `WHERE year = <n>`** when querying `json_all.duckdb`.
- **Prefer `doc_type`, `event`, `date` predicates first** — they use indexes, so filter before ILIKE.
- **Never** use FTS/vector operators — the files ship no such extensions.
- Full content lives in `content`; a document can also be re-read from `../extracted/{year}/{event}/{pdf_stem}.md`.
- `char_len` lets you pre-size context (skip or truncate very large documents).
- Cite answers with `source_hash` / `manifest_key` / `event` so sources are verifiable.

## Building / refreshing

```bash
uv run duckdb/build_markdown.py --all          # all 9 markdown year files
uv run duckdb/build_json.py --all              # json_YYYY.duckdb + json_all.duckdb
uv run duckdb/build_markdown.py --year 2020    # single year
uv run duckdb/build_markdown.py --all --dry-run
uv run duckdb/build_json.py --all --force      # deterministic rebuild (schema changes)
uv run duckdb/verify.py                        # QA checks
```

The builders are **idempotent**: rerunning changes nothing (diff on `source_hash`).
Safe to run after every scrape — only new/changed documents are written, in one
transaction per file (a crash never leaves a torn file).

### Data source & manifest staleness

Documents are discovered on disk under `extracted/{year}/{event}/` and paired
with their `.json` sidecar; `extracted/manifest.json` is used as a cross-check
(it supplies `success`, and is authoritative for the fields it duplicates:
`source_hash`, `doc_type`, `pages`, `tables_extracted`, `images_extracted`,
`extracted_at`). Entries the manifest marks `success: false` are skipped.
Sidecars that exist on disk but have no manifest entry (stale manifest, e.g.
196 files in 2026) are still ingested — the sidecar carries the full metadata —
so row counts always match what is actually on disk. Run the builders after each
extraction run to pick up new documents.

## Layout

```
duckdb/
├── PLAN.md            # approved design
├── README.md          # this file
├── config.py          # paths, years, sidecar flattening, DB helpers
├── schema.py          # shared DDL + indexes (single source of truth)
├── build_markdown.py  # markdown_{year}.duckdb builder (CLI)
├── build_json.py      # json_{year}.duckdb + json_all.duckdb builder (CLI)
├── verify.py          # QA checks (row counts, idempotency, sample queries)
└── data/              # generated *.duckdb files (gitignored)
```

Scripts import `config`/`schema` from their own directory — run them as files
(`uv run duckdb/build_markdown.py`), not via `python -m`.