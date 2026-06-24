# Model & run metadata

Human-readable summary of how this Judit run was produced.
Generated **2026-06-03T13:33:38.893575Z**.

> **Run quality:** fail — 327 warnings.

## Run identity

| Field | Value |
| --- | --- |
| **Description** | slurry manure agricultural effluent |
| **Input pipeline** | Ada Judit intake |
| **Input asset** | slurry_manure_agricultural_effluent (`6dac82d0…`) |
| **Run ID** | `run-001` |
| **Workflow** | `single_jurisdiction` |
| **Completed** | 2026-06-02T12:55:39.563294Z |
| **Output directory** | `runs/slurry-gb-principal-5-frontier-export` |

## Models used

_Provider models resolved from: config:/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/config/litellm.yaml._

| Role(s) | LiteLLM alias | Provider model | Live | Cached |
| --- | --- | --- | ---: | ---: |
| proposition extraction | `frontier_extract` | `anthropic/claude-sonnet-4-5-20250929` | 253 | 26 |

## Runtime

| Phase | Duration |
| --- | --- |
| source intake | 1s |
| proposition extraction | 47m33s |
| proposition inventory | 0s |
| proposition pairing | 0s |
| divergence classification | 0s |
| narrative generation | 0s |
| proposition extraction (instrumented) | 47m32s |
| Σ stage traces (excl. export) | 47m33s |

## Indicative cost estimate

| Measure | Value |
| --- | --- |
| Estimated input tokens (all traces) | 368,997 |
| Estimated input tokens (live calls only) | 330,371 |
| Estimated input tokens (cached calls only) | 38,626 |
| Lower-bound indicative USD (live input tokens only) | ~$0.99 |

_Indicative only: live-call input tokens × $3.0/1M input-token price (frontier mode). This is not a total run cost unless output-token and cache-billing data are also included._

## Additional cost estimates

_Not estimated. Add `model_metadata.additional_cost_estimates` in case.json (e.g. `co2_kg`, `water_litres`) to record CO₂, water, or other impacts._

## Settings & notes

- **Extraction mode:** `frontier` (requested `frontier`)
- **Fallback policy:** `fail_closed`
- **Divergence reasoning:** `none`
- **Source selection:** principal only
- **Sources / bundled propositions:** 5 / 678
- **LLM calls (live / cached ok / fallbacks):** 253 / 26 / 0
- **Run quality:** fail (327 warnings)
- **Pipeline version:** `0.1.0`

### Operator notes

_None. Set `model_metadata.notes` in case.json for free-text about prompts, ablations, or why this run is interesting._
