# Beatrice

A tool for checking whether GOV.UK guidance accurately reflects the law. It extracts propositions from guidance pages, matches them against law propositions using semantic similarity, classifies the relationship using an LLM, and highlights discrepancies in an overlay on the live GOV.UK page.

## What it does

1. **Extract** — fetches a GOV.UK guidance page via the Content API and extracts discrete legal propositions (what the guidance says someone must/should/may do)
2. **Match** — embeds guidance and law propositions and finds the closest law candidates using cosine similarity, then ranks the closest by BERTScore
3. **Classify** — uses an LLM to judge the relationship between each guidance proposition and its matched law propositions: `confirmed`, `outdated`, `guidance omits detail`, `guidance contains additional detail`, `contradicts`, or `does not match`
4. **Summarise** — generates a concise compliance summary for each guidance proposition that is a match i.e. it is not `does not match`
5. **Overlay** — highlights propositions directly on the live GOV.UK page with colour-coded markers (green = confirmed, amber = partial, red = contradicts/outdated)
6. **Export** — downloads results as a CSV for reporting

## Architecture

```
apps/
  guidance-explorer/    Next.js frontend (port 3001)
  guidance-api/         FastAPI backend  (port 8011)
packages/
  domain/               Proposition and SourceRecord models
  llm/                  LiteLLM-compatible client (OpenAI API)
  guidance/             GOV.UK Content API extraction
  matching/             Embeddings, BERTScore, classification cache
scripts/
  extract_guidance_from_text.py       Extract guidance propositions from a .txt file
  extract_law_propositions_from_text.py  Extract law propositions from a .txt file
```

## Prerequisites

- Python 3.13+
- Node.js 18+
- [uv](https://docs.astral.sh/uv/)
- [LiteLLM](https://docs.litellm.ai/) (`pip install litellm`)
- [Ollama](https://ollama.com/) with `nomic-embed-text:v1.5` pulled (`ollama pull nomic-embed-text:v1.5`) for embeddings

## Setup

```bash
# 1. Clone the repo
git clone <repo-url> beatrice
cd beatrice

# 2. Copy and fill in environment variables
cp .env.example .env
# Edit .env — set ANTHROPIC_API_KEY and LiteLLM connection details

# 3. Install Python dependencies
uv sync

# 4. Install frontend dependencies
npm --prefix apps/guidance-explorer install
```

## Running

Start all three services in separate terminals:

```bash
# Terminal 1 — LiteLLM proxy (port 4000)
set -a && source .env && set +a && litellm --config config/litellm.yaml
```

```bash
# Terminal 2 — Guidance API (port 8011)
uv run --env-file .env --package beatrice-guidance-api uvicorn beatrice_guidance_api.main:app --reload --host 127.0.0.1 --port 8011
```

```bash
# Terminal 3 — Frontend (port 3001)
npm --prefix apps/guidance-explorer run dev
```

Then open [http://localhost:3001](http://localhost:3001).

> **Note:** LiteLLM requires `litellm` to be installed (`pip install litellm`) and Ollama running locally for the `local_embed` model. Edit `config/litellm.yaml` to change which models are used.

## Configuration

All configuration is via environment variables in `.env`. Key settings:

| Variable | Default | Description |
|---|---|---|
| `LLM_BASE_URL` | `http://127.0.0.1:4000/v1` | LiteLLM proxy URL |
| `LLM_API_KEY` | — | LiteLLM API key |
| `MODEL_GUIDANCE_CLASSIFY` | `claude_sonnet` | Model alias for classification |
| `MODEL_GUIDANCE_SUMMARISE` | `claude_sonnet` | Model alias for summarisation |
| `MODEL_EMBED` | `local_embed` | Model alias for embeddings |
| `MODEL_BERT_SCORE` | `microsoft/deberta-xlarge-mnli` | HuggingFace model for BERTScore re-ranking |

See `.env.example` for all options.

## Caching

Results are cached to `/tmp/beatrice/` by default:

- `classify-cache.json` — LLM classification results, keyed by SHA256 of model+prompt
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

## BERTScore model

The default BERTScore model is `microsoft/deberta-xlarge-mnli`. This is an NLI-tuned model well suited to judging semantic entailment between guidance and law text. It will be downloaded from HuggingFace on first use (~900MB).

For a lighter alternative, use `roberta-large` (the `bert-score` library default, ~500MB):

```
MODEL_BERT_SCORE=roberta-large
```
