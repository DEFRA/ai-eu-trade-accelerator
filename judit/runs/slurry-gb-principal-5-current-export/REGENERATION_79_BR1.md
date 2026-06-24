# Slurry current export regeneration — Prompt 79-BR1

**Date:** 2026-06-11  
**Case:** `runs/slurry-gb-principal-5-current/case.json`  
**Export:** `runs/slurry-gb-principal-5-current-export`  
**Derived cache:** `runs/slurry-gb-principal-5-current/.derived-cache`  
**Source cache:** `runs/slurry-gb-principal-5-current/.source-cache`

## Why regeneration was required

Recent fixes to CLML text serialisation, regulation/article/rule paragraph fragmentation, and context locator resolution were not reflected in the stale export. The 2026-06-04 export still contained:

- `itertext()`-era excerpt corruption (`181The`, `amake`, `andbassess`, `361Before`, …)
- No `regulation:36:paragraph:*` child fragments (0 regulation paragraph children)
- Parent `regulation:36` bodies prefixed with merged number labels (`361Before…`) instead of `36. Before…`

A further structural serialisation bug (Prompt 79-BR1) caused live legislation.gov.uk P1 `Pnumber` nodes without a trailing dot (`36` not `36.`) to be rendered as sub-paragraph markers `(36)` instead of provision markers `36.`.

## Code change (pre-flight)

`judit/packages/pipeline/src/judit_pipeline/sources/clml_text.py` — `_append_number_for_child()` now selects numbering style from CLML parent level:

| Parent | Style | Example |
|--------|-------|---------|
| P1 | provision | `36. Before the relevant date…` |
| P2 | paragraph | `(1) Paragraph text…` |
| P3 | list | `(a) Item text…` |
| P4+ | nested | `(i) Nested item…` |

Regression tests: `judit/tests/unit/test_clml_text.py`, `test_regulation_paragraph_fragmentation_verification.py`, `test_legislation_gov_uk_adapter.py`. Live-shaped fixture updated: `wsi_2021_77_regulation_36_live_clml.xml` (`<Pnumber>36</Pnumber>` without dot).

## Cache clears performed

| Cache | Action |
|-------|--------|
| `runs/slurry-gb-principal-5-current/.source-cache/` | Removed `b965ac5e7b7222173f56e787db9118491adfdac5ef77c76b1b6f133ad11ce89b.json` (WSI 2021/77 verification snapshot with `(36)` parent text) |
| `runs/slurry-gb-principal-5-current/.derived-cache/` | Entire directory removed (~4.0M; proposition extraction + narrative caches) |
| `${TMPDIR}/judit/source-snapshots/` | 7 global snapshot files cleared |

## Pre-flight verification (no LLM) — PASS

```bash
cd judit
uv run --package judit-pipeline python scripts/verify_regulation_paragraph_fragmentation.py \
  --case runs/slurry-gb-principal-5-current/case.json \
  --source-cache-dir runs/slurry-gb-principal-5-current/.source-cache \
  --output-dir runs/slurry-gb-principal-5-current-export
```

Evidence (WSI 2021/77, `lex-805b03f284dcf364`):

- `regulation:36` preview starts with `36. (1) Before the relevant date…`
- `regulation:36:paragraph:4` preview starts with `(4) The occupier must make a record…`
- Schedule fragments present (141 schedule locators in fresh intake)
- No corruption tokens in refreshed intake: `181The`, `amake`, `andbassess`, `m anure`, `361Before`

Artifacts: `REGULATION_PARAGRAPH_FRAGMENTATION_VERIFICATION.md`, `regulation_paragraph_fragmentation_verification.json` in this directory.

Context locator unit tests: `cd apps/web && npm test -- context-locator-resolution.test.ts` — 22 passed.

## Regeneration statistics

| Metric | Before (stale export) | After (fresh intake only) | After (frontier export) |
|--------|----------------------:|--------------------------:|------------------------:|
| Source fragment count | 279 | 727 | _pending_ |
| Regulation `:paragraph:` children | 0 | 448 | _pending_ |
| Article `:paragraph:` children | 0 | 0 | _pending_ |
| Rule `:paragraph:` children | 0 | 0 | _pending_ |
| Corrupt excerpt hits (`181The` etc.) | 6+ patterns | 0 | _pending_ |
| `regulation:36` parent prefix | `361Before…` | `36. (1) Before…` | _pending_ |

Resolution metrics (partially_resolved / exact regulation paragraph) require a completed export with refreshed `source_fragments.json` and law-statement metadata — not available until frontier re-export completes.

## Frontier re-export — BLOCKED

LiteLLM started (`just litellm`) but `frontier_extract` (`anthropic/claude-sonnet-4-5-20250929`) is **unhealthy**: `ANTHROPIC_API_KEY` not set in the environment.

**Do not** fall back to `--extraction-mode local` for this regeneration.

When credentials are available:

```bash
cd judit
just litellm   # separate terminal; ensure ANTHROPIC_API_KEY is set

uv run --package judit-pipeline python -m judit_pipeline run-and-export-case \
  runs/slurry-gb-principal-5-current/case.json \
  --output-dir runs/slurry-gb-principal-5-current-export \
  --source-cache-dir runs/slurry-gb-principal-5-current/.source-cache \
  --derived-cache-dir runs/slurry-gb-principal-5-current/.derived-cache \
  --use-llm \
  --extraction-mode frontier
```

Post-export checks:

1. `source_fragments.json` contains `regulation:36:paragraph:4` on Wales source.
2. Review Workbench resolves `regulation 36(4)` exactly to `regulation:36:paragraph:4`.
3. No corrupt excerpts in refreshed fragments or re-extracted proposition evidence.
4. Parent regulation fragments use `36.` (not `(36)` or `361`).

## Compatibility notes

- **Review Workbench review exports** are unchanged by this prompt; they remain separate human-review artifacts.
- Regenerated runs may change `source_fragment_id` values because ordinal fragment IDs (`frag-lex-*-NNN`) are still in use.
- **Locator-based joins remain stable** (`regulation:36:paragraph:4`, etc.).
- Older workbench review exports may reference stale fragment IDs after re-ingestion.
- Locator-derived stable fragment IDs remain out of scope (desirable follow-up).
