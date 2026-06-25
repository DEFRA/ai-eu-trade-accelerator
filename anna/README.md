# Anna

Anna sits between **Beatrice** and **Esther**. Beatrice classifies each guidance
proposition against the law one pair at a time, so it over-reports gaps and
conflicts — the resolving text is usually in a *sibling* proposition on the same
page that Beatrice's per-pair view never sees. Anna re-reads each flagged finding
with the whole page's guidance plus Beatrice's stated reason, and decides whether
the verdict still holds.

By the time the corpus reaches Anna it has narrowed to the flagged findings only
(`CONFLICTS` + `GUIDANCE_INCOMPLETE`), so it can afford this wider, more expensive
second look.

## What it does

- Reads Beatrice's `results.json` (guidance propositions, each with a best-first
  `matches` list; the match's law text lives in `law_proposition`).
- For every **top match** (the one Esther surfaces) whose relationship is
  `CONFLICTS` or `GUIDANCE_INCOMPLETE`, re-adjudicates it against the other
  guidance propositions on the same page (`url`) plus Beatrice's reason for the
  verdict.
- All flagged findings are judged in **one Anthropic Message Batch** (50% cost).
- **Commits the decision**: the finding either stays flagged or becomes
  `GROUNDED`. There is no "maybe" — the corrected status is what comes out.

## Output shape — a drop-in for Esther

Anna writes a run in **Beatrice's own shape**, so Esther consumes it unchanged:

```
anna/runs/<name>/
  results.json       # Beatrice schema; top-match relationship + explanation corrected in place
  intermediates/     # passed through from the Beatrice run (Esther reads 01-content.json)
  anna-report.json   # audit trail: every change (old -> new status, both reasons). Not in the pipeline path.
```

`results.json` carries no extra fields and no review flags — it is exactly what
Beatrice would have produced had it judged with page context. Esther reads
`matches[0]` as before:

```bash
esther build --beatrice-run ../anna/runs/<name> --input-pair … --radia-run … --seeds … --dest …
```

The full audit (what Anna changed and why) lives in `anna-report.json`, outside
the pipeline, for humans and debugging.

## Usage

```bash
uv sync
export ANTHROPIC_API_KEY=sk-...

DATA=../../content-audit-data-assets/steps
uv run anna readjudicate \
  --beatrice-run $DATA/beatrice/runs/heron-verdict \
  --dest         $DATA/anna/runs/heron-verdict \
  --model        claude-sonnet-4-6 \
  --dry-run
```

`--dry-run` counts the flagged findings the batch would judge and stops before any
API spend. Drop it to submit the batch and write the run (`--poll-seconds` tunes
the batch poll interval, default 30). Then point Esther's `--beatrice-run` at the
Anna run instead of the Beatrice run.

## Tests

```bash
uv run pytest        # pure transform, no network (a stub judge stands in for the LLM)
```

## Provenance

The re-adjudication prompt is the shipped v5 from the Anna benchmark
(`draft-pipelines/anna/`): brief instruction + Beatrice's reason + a page-context
coverage check and a light subject-match note. The benchmark established that on
the labelled feedback set it clears the false positives Beatrice raised while
holding the genuine findings, and that prompt length / model tier moved results
only within the eval's own noise band — so Anna ships on `claude-sonnet-4-6`.
