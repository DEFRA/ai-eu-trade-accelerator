# Model & run metadata

Human-readable summary of how this Judit run was produced.
Generated **2026-06-04T13:22:18.502154Z**.

> **Regeneration status (Prompt 79-BR1, 2026-06-11):** Pre-flight source intake verification **passed** after CLML P1 provision-numbering fix and cache clears. **Frontier re-export not completed** — `ANTHROPIC_API_KEY` unset; LiteLLM reports `frontier_extract` unhealthy. Export artifacts below are still from the **2026-06-04** run until step 3 in `REGENERATION_79_BR1.md` is run. See `REGENERATION_79_BR1.md` and `REGULATION_PARAGRAPH_FRAGMENTATION_VERIFICATION.md`.

> **Run quality:** fail — 330 warnings.

## Run identity

| Field | Value |
| --- | --- |
| **Description** | slurry manure agricultural effluent |
| **Input pipeline** | Ada Judit intake |
| **Input asset** | slurry_manure_agricultural_effluent (`0690aa25…`) |
| **Run ID** | `run-001` |
| **Workflow** | `single_jurisdiction` |
| **Completed** | 2026-06-04T13:21:53.601870Z |
| **Output directory** | `runs/slurry-gb-principal-5-current-export` |

## Models used

_Provider models resolved from: config:/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/config/litellm.yaml, litellm:/model/info._

| Role(s) | LiteLLM alias | Provider model | Live | Cached |
| --- | --- | --- | ---: | ---: |
| proposition extraction | `frontier_extract` | `anthropic/claude-sonnet-4-5-20250929` | 279 | 0 |

## Runtime

| Phase | Duration |
| --- | --- |
| source intake | 3s |
| proposition extraction | 54m43s |
| proposition inventory | 0s |
| proposition pairing | 0s |
| divergence classification | 0s |
| narrative generation | 0s |
| proposition extraction (instrumented) | 54m42s |
| Σ stage traces (excl. export) | 54m46s |

## Indicative cost estimate

| Measure | Value |
| --- | --- |
| Estimated input tokens (all traces) | 368,997 |
| Estimated input tokens (live calls only) | 368,997 |
| Lower-bound indicative USD (live input tokens only) | ~$1.11 |

_Indicative only: live-call input tokens × $3.0/1M input-token price (frontier mode). This is not a total run cost unless output-token and cache-billing data are also included._

## Additional cost estimates

_Not estimated. Add `model_metadata.additional_cost_estimates` in case.json (e.g. `co2_kg`, `water_litres`) to record CO₂, water, or other impacts._

## Settings & notes

- **Extraction mode:** `frontier` (requested `frontier`)
- **Fallback policy:** `fail_closed`
- **Source selection:** principal only
- **Sources / bundled propositions:** 5 / 729
- **LLM calls (live / cached ok / fallbacks):** 279 / 0 / 0
- **Run quality:** fail (330 warnings)
- **Pipeline version:** `0.1.0`
- **Proposition normalisation:** v1 (enabled; passes: classification, jurisdiction, labelling, relationship_keys)

### Operator notes

_None. Set `model_metadata.notes` in case.json for free-text about prompts, ablations, or why this run is interesting._

### Proposition normalisation

Deterministic passes after extraction (tier, legal effect, territory, labels, relationship keys). Full reference: docs/architecture/proposition-classification.md.

- **Proposition normalisation:** v1 (enabled; passes: classification, jurisdiction, labelling, relationship_keys)
- Bundle field: `pipeline_case_inputs.proposition_normalisation` (`version`, `enabled`, `passes`). Optional case.json note: `model_metadata.proposition_normalisation`.

**Proposition normalisation quality:**
- Warnings: 29
- Errors: 0
- Legacy category conflicts: 0
- Missing territorial application on application-scope rows: 29
- Dangerous legacy keys: 0
- Debug leakage: 0

_Interpretation:_ warnings do not necessarily invalidate a run. Errors mean the export should not be used for downstream comparison without review. Legacy `categories` conflicts are expected during migration but should trend down over time. Full detail: `normalisation_quality.json`, `NORMALISATION_QUALITY.md`.
