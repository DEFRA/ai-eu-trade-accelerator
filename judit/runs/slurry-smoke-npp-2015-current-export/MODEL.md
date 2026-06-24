# Model & run metadata

Human-readable summary of how this Judit run was produced.
Generated **2026-06-04T12:23:39.645395Z**.

> **Run quality:** fail — 87 warnings.

## Run identity

| Field | Value |
| --- | --- |
| **Description** | slurry manure agricultural effluent |
| **Input pipeline** | Ada Judit intake |
| **Input asset** | slurry_manure_agricultural_effluent (`0690aa25…`) |
| **Run ID** | `run-001` |
| **Workflow** | `single_jurisdiction` |
| **Completed** | 2026-06-04T12:23:33.132454Z |
| **Output directory** | `runs/slurry-smoke-npp-2015-current-export` |

## Models used

_Provider models resolved from: config:/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/config/litellm.yaml, litellm:/model/info._

| Role(s) | LiteLLM alias | Provider model | Live | Cached |
| --- | --- | --- | ---: | ---: |
| proposition extraction | `frontier_extract` | `anthropic/claude-sonnet-4-5-20250929` | 75 | 0 |

## Runtime

| Phase | Duration |
| --- | --- |
| source intake | 1s |
| proposition extraction | 17m47s |
| proposition inventory | 0s |
| proposition pairing | 0s |
| divergence classification | 0s |
| narrative generation | 0s |
| proposition extraction (instrumented) | 17m47s |
| Σ stage traces (excl. export) | 17m48s |

## Indicative cost estimate

| Measure | Value |
| --- | --- |
| Estimated input tokens (all traces) | 106,002 |
| Estimated input tokens (live calls only) | 106,002 |
| Lower-bound indicative USD (live input tokens only) | ~$0.32 |

_Indicative only: live-call input tokens × $3.0/1M input-token price (frontier mode). This is not a total run cost unless output-token and cache-billing data are also included._

## Additional cost estimates

_Not estimated. Add `model_metadata.additional_cost_estimates` in case.json (e.g. `co2_kg`, `water_litres`) to record CO₂, water, or other impacts._

## Settings & notes

- **Extraction mode:** `frontier` (requested `frontier`)
- **Fallback policy:** `fail_closed`
- **Source selection:** principal only, max_sources=1
- **Sources / bundled propositions:** 1 / 231
- **LLM calls (live / cached ok / fallbacks):** 75 / 0 / 0
- **Run quality:** fail (87 warnings)
- **Pipeline version:** `0.1.0`
- **Proposition normalisation:** v1 (enabled; passes: classification, jurisdiction, labelling, relationship_keys)

### Operator notes

_None. Set `model_metadata.notes` in case.json for free-text about prompts, ablations, or why this run is interesting._

### Proposition normalisation

Deterministic passes after extraction (tier, legal effect, territory, labels, relationship keys). Full reference: docs/architecture/proposition-classification.md.

- **Proposition normalisation:** v1 (enabled; passes: classification, jurisdiction, labelling, relationship_keys)
- Bundle field: `pipeline_case_inputs.proposition_normalisation` (`version`, `enabled`, `passes`). Optional case.json note: `model_metadata.proposition_normalisation`.

**Proposition normalisation quality:**
- Warnings: 6
- Errors: 0
- Legacy category conflicts: 0
- Missing territorial application on application-scope rows: 6
- Dangerous legacy keys: 0
- Debug leakage: 0

_Interpretation:_ warnings do not necessarily invalidate a run. Errors mean the export should not be used for downstream comparison without review. Legacy `categories` conflicts are expected during migration but should trend down over time. Full detail: `normalisation_quality.json`, `NORMALISATION_QUALITY.md`.
