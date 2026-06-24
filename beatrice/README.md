# Beatrice

The matching stage of the Radia → Susan → Beatrice pipeline. It takes Susan's
guidance propositions and Judit's law propositions, retrieves the closest law
candidates by semantic similarity, then **group-reranks** each guidance
proposition — a single LLM call sees all of its surviving law candidates at once
and labels each one — to find where guidance conflicts with, matches, or omits
the law.

This is a **headless pipeline** (JSON in, JSON out) run via `beatrice run`. No
server, no UI.

## What it does

1. **Retrieve** — embeds guidance and law propositions and selects a small survivor set of the closest law candidates per guidance proposition (cosine threshold + an adaptive `score_gap` window, capped at `top_k`)
2. **Group-rerank** — a single LLM call per guidance proposition judges every surviving candidate together, using typed clause-function / topic rules, and labels each: `GROUNDED`, `UNGROUNDED`, `CONFLICTS`, `GUIDANCE_INCOMPLETE`, `GUIDANCE_BROADER`, or `GUIDANCE_MISSING`. A proposition with no candidate above the threshold costs no LLM call.
3. **Summarise** — generates a concise compliance summary for each guidance proposition that has at least one non-`UNGROUNDED` match
4. **Output** — writes `results.json` + `results.csv` (one row per guidance/law match) plus `metrics.json` and a human-readable `MODEL.md`

## Architecture

A single package (`src/beatrice/`), like Radia and Susan. Run via `./run.sh` or
`uv run beatrice`.

```
src/beatrice/
  cli.py                beatrice {run|tag-topics|type-law}
  domain/               Proposition (law) model + SourceRecord models
  guidance/             GuidanceProposition model + GOV.UK Content API extraction
  llm/                  LiteLLM-compatible client (OpenAI API) — used by extraction
  matching/             Embeddings + cosine retrieval, group-rerank matcher, prompts
  pipeline/             batch_match (Susan -> Beatrice runner), Susan mapping,
                        the typed-tag enrichment tools, and the topic taxonomy
scripts/
  extract_guidance_from_text.py          Extract guidance propositions from a .txt file
  extract_law_propositions_from_text.py  Extract law propositions from a .txt file
```

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- An `ANTHROPIC_API_KEY` (the group-rerank + summarise calls go to the Anthropic Message Batches API)
- [Ollama](https://ollama.com/) with `nomic-embed-text:v1.5` pulled (`ollama pull nomic-embed-text:v1.5`) for local embeddings

## Setup

```bash
cp .env.example .env        # set ANTHROPIC_API_KEY
uv sync
```

## Configuration

All configuration is via environment variables in `.env`. Key settings:

| Variable | Default | Description |
|---|---|---|
| `LLM_BASE_URL` | `http://127.0.0.1:4000/v1` | LiteLLM proxy URL |
| `LLM_API_KEY` | — | LiteLLM API key |
| `MODEL_GUIDANCE_CLASSIFY` | `claude_sonnet` | Model alias for the group-rerank call |
| `MODEL_GUIDANCE_SUMMARISE` | `claude_sonnet` | Model alias for summarisation |
| `MODEL_EMBED` | `local_embed` | Model alias for embeddings |

See `.env.example` for all options.

## Caching

Results are cached to `/tmp/beatrice/` by default:

- `group-rerank-cache.json` — group-rerank verdicts, keyed by SHA256 of the rendered prompt
- `summarise-cache.json` — LLM summaries
- `extract-cache.json` — Extracted propositions per URL+section
- `law-embeddings-cache.json` — Law proposition embeddings

Paths can be overridden via environment variables (see `.env.example`).

## Processing unpublished guidance

To extract propositions from a local `.txt` file (e.g. a Word document export) rather than a live GOV.UK page:

```bash
# Extract guidance propositions
uv run scripts/extract_guidance_from_text.py "my-guidance.txt" \
  --source-url "https://www.gov.uk/guidance/my-page" \
  --topic "My topic" \
  --output guidance_propositions.json

# Extract law propositions from a plain-text legal document
uv run scripts/extract_law_propositions_from_text.py "my-law.txt" \
  --citation "My Act 2024" \
  --jurisdiction "UK" \
  --topic "My topic" \
  --output law_propositions.json
```

## Batch matching (Susan -> Beatrice)

Runs the full corpus through group-rerank via the Anthropic Message Batches API
(50% cost), one grouped request per guidance proposition:

```bash
./run.sh run \
  --guidance path/to/susan-output.json \
  --law path/to/judit-propositions.json \
  --out runs/my-run/
  # tuning knobs (defaults shown): --top-k 15 --threshold 0.65 --score-gap 0.04
  # optional typed-tag caches: --clause-functions <json> --law-topics <json> --guidance-topics <json>
  # --no-sidebar to omit Judit's per-candidate confidence sidebar
```

(`./run.sh run …` is shorthand for `uv run beatrice run …`.) Outputs
`results.json`, `results.csv`, `metrics.json`, and a human-readable `MODEL.md`.
Use `--dry-run` to embed + retrieve and count requests without any API spend.

## Typed-tag enrichment

The group-rerank prompt keys off `clause_function` (law side) and `topic` /
`regulatory_kind` (guidance side). `regulatory_kind` arrives from Susan; the
`clause_function` and `topic` tags are produced by two typing/tagging passes and
fed into `beatrice run` via the optional cache arguments above:

```bash
./run.sh type-law    judit-propositions.json --out clause-functions.json
./run.sh tag-topics  law  judit-propositions.json --out law-topics.json
./run.sh tag-topics  guidance susan-output.json   --out guidance-topics.json
```

When a tag is missing it renders as `unknown` and the prompt's typed rules
degrade gracefully to reading the candidate on its merits.
