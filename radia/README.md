# Radia

Radia is the opinionated filter that Mary is not. Given the full Defra
guidance corpus and a category, she narrows it to the pages that genuinely
belong to that topic. Where Mary asks "is this a Defra guidance page?",
Radia asks "is this a Defra guidance page about *this* category?".

This package is Radia's **production shape**: one classifier, one category,
standalone — nothing to run beyond this directory and the Anthropic API.

## What it bakes in

Two passes, both on the Anthropic batch API: a recall-maximising classifier in
front of a precision adjudicator.

**Pass 1 — recall (word-search routing + Claude Haiku).**

- **Word-search routing.** A page reaches the LLM only if its body hits the
  curated slurry inclusion lexicon (`src/radia/data/lexicon.json`), NLTK
  Porter-stemmed. On a slurry-sparse corpus this prunes ~97% of pages before any
  API call. Pages with no hit are recorded `slurry: false` **without** an LLM
  call — the absence of an inclusion hit *is* the exclusion mechanism.
- **`claude-haiku-4-5-20251001`** via the batch API for the pages that do hit,
  prompted with the slurry description **minus** its *Exclusion Criteria* section
  and told to label TRUE on any mention. Dropping the exclusion criteria here is
  worth ~7pp of recall on borderline pages — exclusion is pass 2's job.
- **Oversized-page rescue.** A page too large for the batch is retried once on
  the immediate Messages API with its body truncated; the trim is stamped into
  the reason, never silent.

**Pass 2 — precision (Claude Sonnet adjudication).**

- Every page pass 1 labelled `slurry: true` is re-read by `claude-sonnet-4-6`
  (batch API) with the *Exclusion Criteria* **re-introduced**
  (`categories.json`'s `exclusion_criteria`). It drops the false positives pass 1
  over-includes — homonyms (coal / laboratory / NORM slurry), sewage sludge, and
  pages that merely name slurry in passing.
- **It can only turn a TRUE into a FALSE**, so the exclusion criteria cost pass 1
  no recall: the ~98% of pages the word-search never routed are untouched. The
  prompt is tuned to the recall-priority operating point — it removes only
  unambiguous non-slurry noise and keeps anything with real slurry-audit value
  (rules, grants/funding, infrastructure, feedstock).

**No silent fallbacks.** Skipped pages carry an explicit "excluded by
word-search" reason. A pass-1 routed page that can't be classified even after
rescue raises `ClassificationIncomplete`. A pass-2 verdict that won't parse leaves
the page as pass 1 labelled it (kept) and is counted `unadjudicated` — never
quietly dropped.

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
- **`LLM_MODEL_ANTHROPIC`** (optional) — overrides the pass-1 model
  (`claude-haiku-4-5-20251001`); `--model` on the command line wins over both.
- **`RADIA_ADJUDICATOR_MODEL`** (optional) — overrides the pass-2 model
  (`claude-sonnet-4-6`); `--adjudicator-model` on the command line wins over both.

`output_dir/` ends up containing:

- `output.json` — the input pages with the **final** `meta_data.labels.slurry`
  (bool, post-pass-2), plus `meta_data.scores.slurry` (pass-1 signal),
  `meta_data.reasons.slurry` (the deciding pass's reason), and
  `selection_strategy_body` / `selected_categories_body` / `selection_meta_body`.
  `body_text` is dropped (it lives upstream). This is the output shape that
  downstream steps (e.g. Susan) consume.
- `adjudication.json` — the pass-1-vs-pass-2 audit trail, one record per pass-1
  positive: `pass1_score`, `pass1_reason`, `pass2_keep`, `exclusion_hit`,
  `pass2_reason`. Kept off the pipeline output path.
- `MODEL.md` — models, started_at, page count, base rate, the
  routed/skipped/rescued and kept/dropped/unadjudicated breakdowns, and a
  per-pass **cost** summary.
- `metrics.json` — machine-readable token cost: per-pass `batch`/`rescue` token
  totals and `cost_usd` (pass 1 Haiku, pass 2 Sonnet, batch-discounted), plus the
  run total. An overridden model that isn't priced records `cost_usd: null` (tokens
  still recorded) rather than a guessed figure.

## Tests

```bash
uv run pytest
```

The tests cover word-search routing, pass-1 response parsing (including
truncated-JSON repair), batch labelling, oversized-page rescue, pass-2
adjudication (drop / keep / keep-on-unparseable / truncation-stamping), and the
no-silent-fallback contract. They run **offline** with no API key: the skip path
never calls the API, and the LLM paths use a stubbed Anthropic client.

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
