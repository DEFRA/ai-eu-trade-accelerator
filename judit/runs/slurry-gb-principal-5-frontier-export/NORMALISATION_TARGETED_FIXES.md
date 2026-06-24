# Slurry export — targeted normalisation fixes (Prompt 22)

Review source: regenerated `NORMALISED_PROPOSITION_REVIEW.md` / `normalised_proposition_review.json`.

## Before / after counts

| Section | Before | After |
| --- | ---: | ---: |
| `unknown_classifications` | 24 | **0** |
| `cross_reference_rows` | 46 | **7** |
| `compliance_without_clear_actor` | 1 | **0** |
| `semantic_comparison_buckets` | 86 | **81** |
| `application_scope_rows` | 42 | 43 |

Implemented in `packages/domain/src/judit_domain/proposition_classification.py` with regression tests in `tests/unit/test_proposition_classification.py`. No extraction or `categories` changes.

---

## 1. Unknown classifications (24 → 0)

| Issue group | Count | Example IDs / locators | Proposed fix | Expected Δ | Risk |
| --- | ---: | --- | --- | ---: | --- |
| Slurry / table **available nitrogen %** | 4 | `prop:d6e668b72bea0040`, `prop:94b1f3f8c5159534`, `prop:acbd81fe33258bfb`, `prop:70a5a0d948be19ca` — reg 14(3) / reg 9(2) tables | `definition`: `\bavailable nitrogen is \d+%` | −4 unknown | Low |
| **Schedule 1 livestock coefficients** (daily manure/N/P) | 8 | `prop:96b01dc9a10a91f8`, `prop:b8911ef7953c8dfa`, Schedule 1 table rows | `definition`: `\bproduce[sd]?\s+\d+…(litres\|grams)` | −8 unknown | Low |
| **Holding area exclusions** (calculation) | 6 | `prop:e120d3dd63c750f6`, `prop:c1a98f427fe8ec4f`, reg 4(3) / 4A(3) / 7(4) | `definition`: `\bno account is (?:to be )?taken of` | −6 unknown | Low |
| **NVZ designation** criterion | 1 | `prop:204ed1287e94d38f` — reg 3(1) | `application_scope`: `\bis designated as a nitrate vulnerable zone` | −1 unknown; +1 scope row | Low |
| **Conditional permission** (spread window) | 1 | `prop:cee83e885f4c664d` — reg 19 | `permission`: `\bis permitted\b` | −1 unknown | Low |
| **Storage capacity** relaxation | 1 | `prop:fe5b84e255cf981c` — Sch 2 para 6(2) | `permission`: `\bneed not have\b` | −1 unknown | Low |
| **Crop N limit table** | 1 | `prop:b03252d4824c8865` — Sch 4 winter OSR | `definition`: `\bmaximum nitrogen rate of` | −1 unknown | Low |
| **Incorporation by reference** | 1 | `prop:2f0166ce06f1e72c` — reg 13(3) | `cross_reference`: `\bapply as if they were provisions of` | −1 unknown; +1 xref | Low |
| **Amending other SIs** | 4 | `prop:474f9825df85d79e`, `prop:74d61714e99dfee3`, reg 49 | `cross_reference`: `\bis substituted with` / `\bare omitted` | −4 unknown; +4 xref | Low |

**Genuinely unknown after fixes:** none.

---

## 2. Cross-reference rows (46 → 7)

### Root cause

`cross_reference` was **overused** for two reasons:

1. **`in accordance with` / `referred to in` ran before `must` / `may not`** — operative duties with internal reg/sched pointers were tagged `cross_reference`.
2. **`provision_type: cross_reference` from extraction** short-circuited text rules (6 rows).

### Reclassification summary

| Issue group | Before (approx.) | Example IDs | Better effect | Fix | Expected Δ | Risk |
| --- | ---: | --- | --- | --- | ---: | --- |
| Operative **obligations** with internal pointers | ~22 | `prop:d417b7b1faae0850`, `prop:ab14f69466939e38`, `prop:fbdecbf0375618f6` | `obligation` / `recordkeeping` / `prohibition` | Run substantive patterns **before** xref phrases; defer `provision_type: cross_reference` when substantive text matches | −22 xref | Low |
| **BS / standard** “in accordance with BS …” | ~10 | `prop:893268e0f2b35de7`, `prop:92c340613eab0454` | `obligation` | Same ordering fix | −10 xref | Low |
| **External definition** | 1 | `prop:028167005afa45aa` — “meaning given by” | `definition` | `\bhas the meaning given by\b` before xref | −1 xref | Low |
| **Genuine incorporation** | 2 | `prop:72b085a51cee4288`, `prop:2f0166ce06f1e72c` | `cross_reference` | Keep / add `apply as if they were provisions` | 2 remain | Low |
| **Amending other instruments** | 4 | `prop:474f9825df85d79e`, reg 49 | `cross_reference` | Substitution/omission patterns | 4 remain | Low |
| **Pure internal pointer** (no modal duty) | 1 | `prop:663fa1514a43f815` — reg 9(3) factors | `cross_reference` | Legitimate | 1 remain | Low |

**Remaining 7 `cross_reference` rows** are appropriate relationship/incorporation/amendment references, not operator duties.

---

## 3. Compliance without clear actor (1 → 0)

| Field | Value |
| --- | --- |
| **ID** | `prop:8f5f0132f0513894` |
| **Locator** | regulation 47 |
| **Label** | Enforcement by NRW |
| **Text** | The Water Resources (Control of Agricultural Pollution) (Wales) Regulations 2021 are enforced by Natural Resources Wales. |
| **Subject / action** | These Regulations / are enforced by |
| **Effect** | `enforcement` (unchanged) |

**Decision:** Not an operator compliance duty — instrument metadata about the enforcing body. **Do not** force actor extraction onto NRW for compliance matrices.

**Fix:** `is_compliance_relevant = false` when text matches `\bthese regulations\b…\bare enforced by\b`.

| Expected Δ | Risk |
| ---: | --- |
| −1 compliance_weak | Low |

---

## 4. Semantic comparison buckets (86 → 81) — sample of largest 10

Buckets are **`{effect}:{normalised_action_prefix}`** hints only. Sample:

| Bucket key (size) | Useful as review hint? | Notes |
| --- | --- | --- |
| `cross_reference:must_be_protected_against_corrosion…` (5) | **Poor** — should collapse after reclass to `obligation` | Key embeds wrong effect |
| `obligation:occupier_must_calculate` (4) | **Good** | Cross-instrument calc duties |
| `obligation:occupier_of_a_holding_must_ensure_that_the_total_amount_of_nitrogen_in_organic` (4) | **Good** but long | NVZ organic N limits |
| `unknown:produce_daily` (4) | **OK** until classified | Now `definition:produce_daily` after fix |
| `application_scope:a_slurry_storage_tank_that_drains…` (3) | **Mixed** | Subject-fragment keys are noisy |
| `definition:slurry` (3) | **Excellent** | Canonical cross-SI definition compare |
| `derogation:does_not_apply` (3) | **Moderate** | Broad action stem |
| `enforcement:is_guilty_of_the_offence…` (3) | **Good** | Corporate liability cluster |
| `obligation:enhanced_nutrient_management_plan_must_record` (3) | **Good** | Wales Sch 1A cluster |
| `obligation:fertilisation_plan_must_record` (3) | **Good** | Derogation plan cluster |

**No automatic links created.** Keys remain hints; effect prefix quality improves when classification is correct.

---

## Code changes applied

- Substantive-before-cross-reference pattern ordering in `derive_legal_effect_type`.
- New definition / scope / permission / amendment heuristics (see above).
- `provision_type: cross_reference` no longer overrides clear substantive modals.
- Enforcement-authority boilerplate excluded from `is_compliance_relevant`.
