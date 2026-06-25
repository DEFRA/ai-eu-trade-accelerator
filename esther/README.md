# Esther

Esther is the final step in the pipeline. By the time data reaches her the
analysis is done; her job is to gather the upstream outputs and render them in
the shape the [content-audit frontend](../../ai-sdlc-content-audit) consumes.

She joins:

- **Beatrice** — `results.json` (matched + classified guidance↔law propositions,
  each guidance entry carrying its native Susan `id`).
- **Beatrice's input pair** — `law.json` (Judit's propositions). The guidance ids
  now come from Beatrice's own output, so the input pair's `guidance.json` is no
  longer read.
- **Radia** — `output.json`, the source for the **audited pages** (`content_id`,
  page metadata + gov.uk analytics), per-page relevance (`meta_data.scores.slurry`),
  and the audited-corpus size (`total_pages_audited` = number of pages Radia was
  given). The pages Esther emits are those Beatrice produced guidance for.
- **Reading age** — recomputed per audited page from the gov.uk content API
  (SMOG; ported from the standalone `reading-age` tool, cached on disk).
- **Seeds** — `categories.json` + `legislation.json` + `legislation-propositions.json`
  (the manually maintained law catalogue; re-keyed to native shape, never new data).

## Output shape (pipeline-native keys)

One file per entity, keyed by pipeline-native identifiers (no synthetic integer
ids):

| File | Key | Notes |
|---|---|---|
| `categories.json` | `id` (slug) | |
| `legislation.json` | `source_record_id` (`lex-…`) | |
| `legislation-propositions.json` | `id` (`prop:…`) | links to legislation by `source_record_id` |
| `pages.json` | `content_id` | audited pages |
| `guidance-propositions.json` | `id` (`susan-…`) | native Susan id from Beatrice; links to page by `content_id` |
| `proposition-matches.json` | `id` (`m-…`) | links by `guidance_proposition_id` + `law_proposition_id`; `relationship` value; `GUIDANCE_MISSING` rows have null guidance id |
| `page-relevance.json` | `content_id` | `relevance_score` from Radia |
| `pages-reading-age.json` | `content_id` | `word_count`, `reading_age` |
| `page-analytics.json` | `content_id` | `last_updated_date`, `view_count_period` |
| `subject-summary.json` | `category` | rollups; `total_pages_audited` = Radia corpus size |

There is **no** `page-aggregations.json` (correctness is not computable at this
stage) and **no** `legislation-aggregations.json` (the frontend derives coverage
itself). No silent fallbacks: anything that can't be derived is left null and
logged as a warning.

## Usage

```bash
uv sync

DATA=../../content-audit-data-assets/steps
uv run esther build \
  --beatrice-run $DATA/beatrice/runs/heron-verdict \
  --input-pair   $DATA/beatrice/inputs/oats-byre-x-statute-badger \
  --radia-run    $DATA/radia/runs/lagoon-curlew/output.json \
  --seeds        $DATA/esther/seeds \
  --dest         ../../ai-sdlc-content-audit/src/server/services/audit/data \
  --reading-age-cache ../../reading-age/cache \
  --dry-run
```

Drop `--dry-run` to write the files. The reading-age cache is keyed the same way
as the standalone tool (`sha1(content-api-url)`), so pointing `--reading-age-cache`
at the existing `reading-age/cache` reuses already-fetched pages.

## Tests

```bash
uv run pytest        # pure transform + reading-age scoring, no network
```
