# CLI reference

Source of truth is `uv run --package judit-pipeline python -m judit_pipeline --help` and per-command `--help`.

## Core run/export

| Command | Purpose | Important options | Guide |
|---|---|---|---|
| `run-case` | Run a case JSON through pipeline stages. | `--use-llm`, `--extraction-mode`, `--extraction-execution-mode`, `--extraction-fallback`, `--divergence-reasoning`, `--source-cache-dir`, `--derived-cache-dir` | `README.md` |
| `export-case` | Run a case and write static export bundle. | `--output-dir`, plus the same extraction/runtime options as `run-case` | `README.md` |
| `lint-export` | Evaluate exported bundle lint/quality summary. | `--export-dir`, `--no-quality-summary` | [`docs/dev/quality-runs.md`](../dev/quality-runs.md) |
| `compare-runs` | Compare baseline vs candidate exported runs. | `--baseline-export-dir`, `--candidate-export-dir`, `--write-summary` | (no dedicated guide yet) |

## Corpus workflows

| Command | Purpose | Important options | Guide |
|---|---|---|---|
| `build-equine-corpus` | Build equine corpus export with coverage artifacts. | `--corpus-config`, `--output-dir`, `--use-llm`, `--extraction-mode`, `--extraction-fallback`, `--divergence-reasoning` | [`docs/dev/equine-passport-staged-runs.md`](../dev/equine-passport-staged-runs.md) |
| `estimate-corpus-run` | Estimate fragments/LLM calls without provider invocation. | `--profile`, `--extraction-mode`, `--offline-only`, `--fetch` | [`docs/dev/equine-passport-staged-runs.md`](../dev/equine-passport-staged-runs.md) |

## Registry workflows

| Command | Purpose | Important options | Guide |
|---|---|---|---|
| `source-registry-list` | List source registry entries. | `--source-registry-path`, `--source-cache-dir` | (no dedicated guide yet) |
| `source-registry-inspect` | Inspect one registry entry. | `--source-registry-path`, `--source-cache-dir` | (no dedicated guide yet) |
| `source-registry-register` | Register source reference JSON/file. | `--reference-json`, `--reference-file`, `--refresh`, `--source-registry-path` | (no dedicated guide yet) |
| `source-registry-refresh` | Refresh a registered source snapshot. | `--source-registry-path`, `--source-cache-dir` | (no dedicated guide yet) |
| `run-registry-sources` | Run pipeline from selected registry IDs. | `--registry-id`, `--topic-name`, `--analysis-mode`, `--analysis-scope`, `--use-llm`, `--extraction-mode`, `--focus-scopes`, `--max-propositions-per-source` | [`docs/dev/jurisdiction-analysis-runs.md`](../dev/jurisdiction-analysis-runs.md) |

## Inspection commands

| Command | Purpose | Important options | Guide |
|---|---|---|---|
| `list-runs` | List exported runs. | `--export-dir` | (no dedicated guide yet) |
| `inspect-run` | Inspect one run payload. | `--run-id`, `--export-dir` | (no dedicated guide yet) |
| `inspect-stage-traces` | Inspect stage traces for a run. | `--run-id`, `--export-dir` | (no dedicated guide yet) |
| `list-run-review-decisions` | List review decisions for a run. | `--run-id`, `--export-dir` | (no dedicated guide yet) |
| `list-sources` | List source records in exported run. | `--run-id`, `--export-dir` | (no dedicated guide yet) |
| `inspect-source` | Inspect source detail. | `--run-id`, `--export-dir` | (no dedicated guide yet) |
| `inspect-source-snapshots` | Inspect source snapshots. | `--run-id`, `--export-dir` | (no dedicated guide yet) |
| `inspect-source-fragments` | Inspect source fragments. | `--run-id`, `--export-dir` | (no dedicated guide yet) |
| `list-propositions` | List propositions in exported run. | `--run-id`, `--export-dir` | (no dedicated guide yet) |
| `inspect-proposition-history` | Inspect proposition history/lineage. | `--include-runs`, `--export-dir` | [`docs/dev/jurisdiction-analysis-runs.md`](../dev/jurisdiction-analysis-runs.md) |
| `inspect-extraction-failures` | Inspect extraction failure rows. | `--export-dir` | [`docs/dev/quality-runs.md`](../dev/quality-runs.md) |

## Review/governance commands

| Command | Purpose | Important options | Guide |
|---|---|---|---|
| `add-review-decision` | Append review decision row. | `--artifact-type`, `--artifact-id`, `--decision`, `--reviewer`, `--reason` | [`docs/dev/article-109-equine-pilot.md`](../dev/article-109-equine-pilot.md) |
| `list-review-decisions` | Query review decisions by filters. | `--export-dir`, `--run-id`, `--artifact-type`, `--artifact-id`, `--decision` | [`docs/dev/article-109-equine-pilot.md`](../dev/article-109-equine-pilot.md) |
| `apply-assessment-review` | Apply review decision update to assessment flows. | `--reviewer`, `--note`, `--edited-fields-json` | (no dedicated guide yet) |

## Repair/batch commands

| Command | Purpose | Important options | Guide |
|---|---|---|---|
| `repair-extraction` | Re-run repairable extraction jobs from exported bundle. | `--export-dir`, `--output-dir`/`--in-place`, `--only`, `--extraction-mode`, `--extraction-fallback`, `--retry-failed-llm` | [`docs/dev/quality-runs.md`](../dev/quality-runs.md) |
| `plan-extraction-batch` | Prepare external extraction batch plan. | `--export-dir` | (no dedicated guide yet) |
| `submit-extraction-batch` | Submit extraction batch job. | `--export-dir` | (no dedicated guide yet) |
| `poll-extraction-batch` | Poll extraction batch status/results. | `--export-dir`, `--fetch-results` | (no dedicated guide yet) |
| `import-extraction-batch` | Import completed batch outputs. | `--export-dir`, `--extraction-fallback` | (no dedicated guide yet) |

## Dev reset commands

| Command | Purpose | Important options | Guide |
|---|---|---|---|
| `clear-operations-runs` | Remove exported run bundles; keep registry. | `--export-dir`, `--dry-run`, `--confirm`, `--source-registry-path` | [`docs/dev/operations-state-reset.md`](../dev/operations-state-reset.md) |
| `clear-operations-all` | Remove exported bundles, registry, source/derived caches. | `--export-dir`, `--dry-run`, `--confirm`, `--source-registry-path`, `--source-cache-dir`, `--derived-cache-dir` | [`docs/dev/operations-state-reset.md`](../dev/operations-state-reset.md) |
