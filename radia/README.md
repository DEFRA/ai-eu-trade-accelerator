# Radia

Radia is the opinionated filter that Mary is not. Given the full Defra
guidance corpus and a category, she narrows it to the pages that genuinely
belong to that topic. Where Mary asks "is this a Defra guidance page?",
Radia asks "is this a Defra guidance page about *this* category?".

This package is Radia's **production shape**: one classifier, one category,
standalone — nothing to run beyond this directory and the Anthropic API.

## What it bakes in

One configuration — word-search routing in front of Claude Haiku:

- **Word-search routing.** A page reaches the LLM only if its body hits the
  curated slurry inclusion lexicon (`src/radia/data/lexicon.json`), NLTK
  Porter-stemmed. On a slurry-sparse corpus this prunes ~97% of pages before any
  API call. Pages with no hit are recorded `slurry: false` **without** an LLM
  call — the absence of an inclusion hit *is* the exclusion mechanism.
- **Anthropic, `claude-haiku-4-5-20251001`** via the batch API for the pages
  that do hit.
- **One prompt:** the slurry description with its *Exclusion Criteria* section
  removed (`src/radia/data/categories.json`). Leaving exclusion criteria in the
  prompt cost ~7pp of recall on borderline pages, so it's out.
- **Oversized-page rescue.** A page too large for the batch is retried once on
  the immediate Messages API with its body truncated; the trim is stamped into
  the reason, never silent.
- **No silent fallbacks.** Skipped pages carry an explicit "excluded by
  word-search" reason. A routed page that can't be classified even after rescue
  raises `ClassificationIncomplete` — it is never quietly defaulted to FALSE.
- **No Ollama, no tf-idf, no all-categories prompt.** Radia bakes in a single
  strategy; the alternatives that were trialled are out of scope for this package.

## Usage

```bash
uv sync
export ANTHROPIC_API_KEY=sk-...

# Narrow a page corpus (Mary's output shape) down to on-topic pages
uv run radia run path/to/pages.json path/to/output_dir/
```

`pages.json` is a list of `{url, content_id, meta_data: {title, body_text, ...}}`
objects — pages must carry `meta_data.body_text` (Radia classifies the body, she
does not fetch it). For a sharded fetch, pass `--shard-name <name>` to stamp
per-row `source_shard` provenance.

### Configuration

- **`ANTHROPIC_API_KEY`** (required) — the routed pages go to the Anthropic API.
  It can be exported or placed in a `.env` file (loaded automatically).
- **`LLM_MODEL_ANTHROPIC`** (optional) — overrides the default model
  (`claude-haiku-4-5-20251001`); `--model` on the command line wins over both.

`output_dir/` ends up containing:

- `output.json` — the input pages with `meta_data.labels.slurry` (bool) added,
  plus `meta_data.scores.slurry`, `meta_data.reasons.slurry`, and
  `selection_strategy_body` / `selected_categories_body` / `selection_meta_body`.
  `body_text` is dropped (it lives upstream). This is the output shape that
  downstream steps (e.g. Susan) consume.
- `MODEL.md` — model, started_at, page count, base rate, and the routed /
  skipped / rescued breakdown.

## Tests

```bash
uv run pytest
```

The tests cover word-search routing, response parsing (including truncated-JSON
repair), batch labelling, oversized-page rescue, and the no-silent-fallback
contract. They run **offline** with no API key: the skip path never calls the
API, and the routed/rescue paths use a stubbed Anthropic client.

## Validation

Radia only writes runs. Scoring a run's labels against a labelled benchmark
(recall over on-topic pages / precision / per-page agreement) is a separate
validation step, maintained outside this pipeline repo.

## What this isn't

- Not multi-provider. Single API, single model.
- Not multi-category. The package bakes in the slurry category + lexicon; a new
  category is a new pair of data files (`categories.json` + `lexicon.json`).
- Not a fetcher. Pages must already carry `body_text`.
- Not wired into any orchestrator. Radia is standalone — wiring is a later,
  separate decision.
- Not a workspace member of any monorepo. Standalone `pyproject.toml`.
