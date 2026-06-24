# LiteLLM and AI routing

Ada uses AI **only** through **Pydantic AI**. Application code must **not** call OpenAI, Anthropic, Google, Gemini, Ollama, the LiteLLM Python SDK, or other provider SDKs directly.

**LiteLLM is only for model routing.** It is not a discovery substrate and does not replace Lex for finding legal sources.

## Intended stack

```
Ada application code
        ↓
   Pydantic AI          ← typed agents, structured outputs → Pydantic models
        ↓
LiteLLM OpenAI-compatible proxy   ← model routing, provider keys, aliases
        ↓
 underlying model provider        ← OpenAI, Anthropic, Azure, etc. (configured in LiteLLM)
```

## What Pydantic AI is used for (V1)

| Feature | Pydantic output model | Required? |
|---------|----------------------|-----------|
| Category expansion | additional synonyms, rationale | optional |
| Candidate relevance assessment | structured relevance rationale | optional |

All model responses must validate into Pydantic models before Ada uses them in business logic.

## Environment variables

```bash
# AI provider selection (V1: litellm only)
ADA_AI_PROVIDER=litellm
ADA_AI_MODEL=<model-or-litellm-alias>

# LiteLLM OpenAI-compatible proxy
ADA_LITELLM_BASE_URL=http://localhost:4000/v1
ADA_LITELLM_API_KEY=<key>
```

| Variable | Required for AI | Description |
|----------|-----------------|-------------|
| `ADA_AI_PROVIDER` | yes | Must be `litellm` in V1 |
| `ADA_AI_MODEL` | yes | Model name or LiteLLM alias passed to Pydantic AI |
| `ADA_LITELLM_BASE_URL` | yes | Proxy base URL including `/v1` |
| `ADA_LITELLM_API_KEY` | depends | API key if the proxy requires authentication |

When these are unset, AI features are disabled. **Deterministic commands** (`build-query-plan`, `discover --no-network`, `make-register`, `export-for-judit`, etc.) work without LiteLLM.

## Configuration behaviour

- `ADA_AI_PROVIDER=litellm` → Ada constructs a Pydantic AI model pointing at `ADA_LITELLM_BASE_URL`
- Model id comes from `ADA_AI_MODEL` (not hard-coded provider names in application code)
- Failures to parse AI output into Pydantic models are errors, not silent fallbacks to raw text

## Local development

Run LiteLLM (or your organisation's shared proxy) separately:

```bash
# illustrative — use your org's LiteLLM setup
litellm --config config.yaml
```

Point Ada at the proxy:

```bash
export ADA_AI_PROVIDER=litellm
export ADA_AI_MODEL=ada-default
export ADA_LITELLM_BASE_URL=http://localhost:4000/v1
export ADA_LITELLM_API_KEY=sk-local
```

Check status:

```bash
ada ai-status
```

## Testing

- Unit tests mock HTTP or use deterministic fallbacks
- `pytest` must not require live models or network access
- See `tests/test_ai.py`

## Non-goals

- LiteLLM does not search legislation — use Lex for discovery ([lex-api-notes.md](lex-api-notes.md))
- Ada does not embed LiteLLM as an in-process SDK dependency for routing; the proxy is HTTP OpenAI-compatible
