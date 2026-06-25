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
2. **Group-rerank** — a single LLM call per guidance proposition judges every surviving candidate together and labels each: `GROUNDED`, `UNGROUNDED`, `CONFLICTS`, `GUIDANCE_INCOMPLETE`, `GUIDANCE_BROADER`, or `GUIDANCE_MISSING`. A proposition with no candidate above the threshold costs no LLM call.
3. **Summarise** — generates a concise compliance summary for each guidance proposition that has at least one non-`UNGROUNDED` match
4. **Output** — writes `results.json` + `results.csv` (one row per guidance/law match) plus `metrics.json` and a human-readable `MODEL.md`

## Architecture

A single package (`src/beatrice/`), like Radia and Susan. Run via `./run.sh` or
`uv run beatrice`.

```
src/beatrice/
  cli.py                beatrice run
  domain/               Proposition (law) model + SourceRecord models
  guidance/             GuidanceProposition model + Susan-output mapping
  matching/             Embeddings + cosine retrieval, group-rerank matcher, prompts
  pipeline/             batch_match (Susan -> Beatrice runner) + Susan mapping
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

The matcher's behaviour (retrieval geometry, prompt, models) is fixed in code —
it is the configuration that won the benchmark ladder, not a set of runtime
knobs. Only infra is environment-driven:

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | — | group-rerank + summarise calls |
| `EMBED_BASE_URL` | `http://127.0.0.1:11434/v1` | local embedding endpoint (Ollama) |
| `EMBED_MODEL` | `nomic-embed-text:v1.5` | embedding model |

## Batch matching (Susan -> Beatrice)

One opinionated command runs the full group-rerank match via the Anthropic
Message Batches API (50% cost), one grouped request per guidance proposition:

```bash
./run.sh run \
  --guidance path/to/susan-output.json \
  --law path/to/judit-propositions.json \
  --out runs/my-run/
```

(`./run.sh run …` is shorthand for `uv run beatrice run …`.) Outputs
`results.json`, `results.csv`, `metrics.json`, and a human-readable `MODEL.md`.

Both inputs are consumed **verbatim** — Beatrice does not refetch or re-extract:
`--guidance` is a **Susan run's `output.json`** (flattened to `GuidanceProposition`
by `pipeline/mapping.py`), and `--law` is a **Judit run's `output.json`** — the
`Proposition` export validated by `_load_law`. A reshaped `legislation-propositions.json`
(e.g. Esther's seeds) is **not** a valid `--law` input; it won't pass `Proposition`
validation.
The retrieval geometry (`top_k 15`, `threshold 0.65`, `score_gap 0.04`), the
prompt, and the model are fixed in code — the configuration that won the
benchmark ladder (see `benchmark_generators/beatrice/versions/`).

Use `--dry-run` to embed + retrieve and count requests without any API spend.
Nothing is cached between runs — each run embeds, retrieves, and group-reranks
afresh.
