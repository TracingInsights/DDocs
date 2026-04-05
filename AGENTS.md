# agents.md — Available CLI Tools (WSL2)

> **WSL2 path rule**: Always work under `~/`. Never use `/mnt/c/...` — slower I/O, permission issues, symlink breakage.

---

## Search & Navigation

| Tool | Replaces | Purpose |
|------|----------|---------|
| `rg` | `grep -r` | Fast recursive content search. Respects `.gitignore`. |
| `fd` | `find` | Find files by name, type, or extension. |
| `fzf` | — | Fuzzy filter. Use `--filter` flag for non-interactive/agent use. |
| `zoxide` (`z`) | `cd` | Frecency-based directory jumping. |

## File Viewing & Manipulation

| Tool | Replaces | Purpose |
|------|----------|---------|
| `bat` | `cat` | View files with line numbers. Use `--plain` for agent output. |
| `eza` | `ls` | Directory listing with tree support. |
| `sd` | `sed` | Find & replace in files. Simpler regex syntax than sed. |
| `fastmod` | — | Codebase-wide find & replace. Prefer over `sd` for multi-file refactors. |

## Data Processing

| Tool | Replaces | Purpose |
|------|----------|---------|
| `jq` | — | JSON query, filter, and transform. |
| `yq` | — | YAML/TOML/XML processing. Same syntax as `jq`. |

## HTTP

| Tool | Use when |
|------|----------|
| `xh` | One-off HTTP requests (curl/httpie alternative). |
| `hurl` | Scripted HTTP sequences and API testing. |

## Code Analysis & Quality

| Tool | Language | Purpose |
|------|----------|---------|
| `ast-grep` (`sg`) | Any | AST-based structural code search and rewrite. |
| `tokei` | Any | Count lines of code by language. |
| `ruff` | Python | Lint and format. Replaces flake8 + black. |
| `oxlint` | JS/TS | Fast JS/TS linter (Rust-based). Replaces eslint for most rules. |
| `oxfmt` | JS/TS | JS/TS formatter. Replaces prettier. |
| `basedpyright` | Python | Type checking. Prefer over `ty` (immature) and Pyrefly (resource heavy). |
| `lychee` | Markdown/HTML | Link checker. |
| `pip-audit` | Python | Scan dependencies for known vulnerabilities. |

## Runtimes & Package Management

| Tool | Purpose |
|------|---------|
| `bun` | **Use for all JS/TS** — runtime, package manager, bundler, test runner. Replaces node/npm/npx/yarn. |

## Browser Automation

| Tool | Purpose |
|------|---------|
| `playwright` | Browser automation and end-to-end testing. |

## Media & WASM

| Tool | Purpose |
|------|---------|
| `ffmpeg` | Video/audio processing and conversion. |
| `wasmtime` | Run WebAssembly modules. |

## Parallelism

| Tool | Purpose |
|------|---------|
| `parallel` | Run shell commands concurrently across inputs. |

## Cargo / Rust Maintenance

| Tool | Purpose |
|------|---------|
| `cargo install-update` | Update installed cargo binaries. Use `-a` to update all. |

> `cargo binstall` is available but has security concerns — prefer `cargo install`.

## LSP / Language Servers

| Tool | Purpose |
|------|---------|
| `tailwindcss-language-server` | Tailwind CSS LSP (editor-invoked). |
