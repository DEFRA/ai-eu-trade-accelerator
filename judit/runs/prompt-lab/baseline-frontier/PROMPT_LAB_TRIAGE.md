# Prompt-lab triage — baseline-frontier

**Batch:** `runs/prompt-lab/baseline-frontier` · **Mode:** frontier · **Fixtures:** 10  
**Machine-readable:** [`prompt_lab_triage.json`](prompt_lab_triage.json)

## Applied (Prompt 39 — 2026-06-04)

Fixture `evaluation.mode` updates from triage + Prompt 38 re-score (no prompt changes):

| Fixture | Was | Now | Re-score |
| --- | --- | --- | --- |
| slurry-bad-over-compressed-crop-nitrogen-table | exhaustive | **minimum** | warn (1/1, extras allowed) |
| slurry-good-definition-slurry-sssaho-2010 | exhaustive | **targeted** | warn |
| slurry-good-simple-obligation-170kg-n | exhaustive | **targeted** | warn |
| slurry-ugly-appeal-nvz-designation-reg-6 | exhaustive | **minimum** | warn |
| slurry-ugly-cross-reference-derogation-directive | exhaustive | **minimum** | warn (2/2) |
| slurry-ugly-transitional-nvz-wales-reg-2 | exhaustive | **minimum** | warn |

**Batch verdict after re-score:** `pass_with_warnings` (1 pass, 9 warn, 0 fail). See [`PROMPT_LAB_SUMMARY.md`](PROMPT_LAB_SUMMARY.md).

Unchanged: diffuse reg 1 `exhaustive`; spread-buffer `targeted`; schedule table `table_rows`; unless-except `minimum`.

---

## Executive summary (pre–Prompt 39 triage)

| Metric | Count |
| --- | --- |
| Pass | 1 |
| Warn | 2 |
| Fail | 7 |
| **True prompt / extraction omissions** | **0** |
| **Fixture / eval policy (extras-only under exhaustive)** | **5** |
| **Classifier + evaluator matching** | **2** |

**Revised batch verdict (at triage time):** `fixture_policy_review_needed` (was `failures_suggest_prompt_change`).

Five of seven failures matched **every** gold row and failed only on proposition count / disallowed extras under `exhaustive`. Do **not** change extraction prompts on this evidence.

---

## Triage table

| Fixture | Tier | Eval mode | Status | Matched | Act/Exp | Triage category | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| slurry-bad-diffuse-2018-reg-1-boilerplate | bad | exhaustive | pass | 4/4 | 4/4 | `pass` | — |
| slurry-bad-over-compressed-crop-nitrogen-table | bad | exhaustive | fail | 1/1 | 8/1 | `fixture_mode_too_strict` | Extras-only |
| slurry-bad-unless-except-organic-manure-250kg | bad | minimum | fail | 5/8 | 8/8 | `evaluator_matching_issue` | Bundling + effect types |
| slurry-good-definition-slurry-sssaho-2010 | good | exhaustive | fail | 1/1 | 8/1 | `acceptable_with_extras` | +1× `classifier_issue` (`unknown`) |
| slurry-good-simple-obligation-170kg-n | good | exhaustive | fail | 1/1 | 5/1 | `fixture_mode_too_strict` | Extras-only |
| slurry-good-simple-prohibition-spread-buffer | good | targeted | warn | 1/1 | 7/1 | `acceptable_with_extras` | Policy OK |
| slurry-ugly-appeal-nvz-designation-reg-6 | ugly | exhaustive | fail | 1/1 | 8/1 | `fixture_expected_rows_incomplete` | Extras-only |
| slurry-ugly-cross-reference-derogation-directive | ugly | exhaustive | fail | 0/2 | 8/2 | `classifier_issue` | Content present |
| slurry-ugly-schedule-livestock-manure-table | ugly | table_rows | warn | 4/4 | 8/4 | `acceptable_with_extras` | Policy OK |
| slurry-ugly-transitional-nvz-wales-reg-2 | ugly | exhaustive | fail | 2/2 | 4/2 | `acceptable_with_extras` | Scope duplicates |

---

## Deep dives (requested cases)

### A — `slurry-bad-over-compressed-crop-nitrogen-table`

- **Matched:** 1/1 (crop nitrogen table obligation, prop-001).
- **Extras (7):** definitional “total amount of nitrogen”; reg 12 ↔ 13 cross-reference; five permission uplifts (straw/paper sludge, shallow soil, yield, milling wheat, cut grass).
- **Verdict:** Fragment is a full regulation 12 block, not a single-row table test. **`exhaustive` is too strict.**
- **Recommendation:** Switch to **`minimum`** (keep one gold obligation) or **`table_rows`** if the intent is to test row explosion only. Do not tighten the prompt to suppress valid permission rows.

### B — `slurry-good-definition-slurry-sssaho-2010`

- **Matched:** 1/1 (`"slurry" means…`, actual prop-004).
- **Extras:** Six sibling definitions (construct, fuel oil, livestock, slurry storage tank, reception pit, storage system reference) — all in the interpretation fragment.
- **`unknown` effect:** prop-007 (British Standard equivalence for silos/tanks) — **`classifier_issue`**, not missing extraction.
- **Recommendation:** **`targeted`** (mirror spread-buffer good case). Optional: add gold row for BS equivalence if that behaviour must be tracked.

### C — `slurry-good-simple-obligation-170kg-n`

- **Matched:** 1/1 (170 kg N holding limit).
- **Extras:** derogation if granted; calculation via reg 14; two holding-area exclusions (hardstanding/woodland; greenhouse) — all substantive and quoted.
- **Recommendation:** **`targeted`**, not prompt change.

### D — `slurry-ugly-appeal-nvz-designation-reg-6`

- **Matched:** 1/1 (appeal to First-tier Tribunal).
- **Extras:** seven further **`appeal`** rows (grounds, Wales/Scotland variants, Secretary of State consequences, tribunal rules cross-ref) — same legal_effect family, no missing/unexpected effect types.
- **Verdict:** **`fixture_expected_rows_incomplete`**, not over-extraction.
- **Recommendation:** **`minimum`** for the headline row, or expand gold to cover reg 6(2)–(4) if procedural completeness is the test goal.

### E — `slurry-ugly-transitional-nvz-wales-reg-2`

- **Matched:** 2/2 (2023 bundle of regs; 2024/2025 for regs 4 & 36).
- **Extras:** two **`application_scope`** rows that restate the same transitional rules with explicit “holding not previously in NVZ per 2013 map” scope.
- **Verdict:** **`acceptable_with_extras`** — content is valid; gold uses **`commencement`** while model used **`application_scope`** for scoped commencement sentences.
- **Recommendation:** **`minimum`**, or allow `application_scope` in gold / effect-equivalence in evaluator.

### F — `slurry-bad-unless-except-organic-manure-250kg` (likely real failure)

| Expected idx | Gold effect | Match? | Root cause |
| --- | --- | --- | --- |
| 0–3 | obligation, permission, 2× prohibition | ✓ | — |
| 4 | obligation (Condition 3: no other organic manure) | ✗ | **Evaluator:** prop-002 already matched to expected[1]; Condition 3 is inside prop-002 text and evidence but not matchable as separate gold row |
| 5 | derogation (grassland exemption) | ✓ | prop-005 |
| 6 | prohibition (80% grass + N limits) | ✗ | **Evaluator + fixture:** bundled in prop-005 as single **derogation**; gold expects separate **prohibition** row |
| 7 | derogation (greenhouse land exclusion) | ✗ | **Classifier:** prop-008 **`application_scope`** vs gold **`derogation`**; evidence and text align |

**Evidence failures (2):** artefacts of failed row match on [4] and [6], not missing quotes in isolation.

**Extras (3, allowed in minimum):** grassland area definition; reg 14 calculation cross-ref; greenhouse scope (duplicate of expected[7] semantics).

**Verdict:** **`evaluator_matching_issue`** (primary), **`classifier_issue`** (greenhouse effect type). **Not** a prompt omission — eight rows cover the fragment.

### G — `slurry-ugly-cross-reference-derogation-directive` (likely real failure)

| Gold row | Expected | Closest actual | Issue |
| --- | --- | --- | --- |
| 0 | permission — may apply for derogation (80% grass) | prop-001 | Same text & evidence; tagged **`application_scope`** / `scope_rule` |
| 1 | definition — “Derogation” means… | prop-002 | Verbatim definition; tagged **`derogation`** / `substantive_rule` |

**0/2 matched** despite substantive extraction — **taxonomy mismatch**, not structural failure. Six additional reg-36 rows (greenhouse scope, declaration, deadlines, application window, 2017 transitional, form) are correct for the fragment.

**Fixture:** `exhaustive` with 2 gold rows on an 8-paragraph regulation; **`why_this_case`** targets Directive cross-refs, not “extract exactly two sentences”.

**Verdict:** **`classifier_issue`** + **`fixture_mode_too_strict`** / incomplete gold. **No prompt change** until evaluator accepts effect-type equivalence or gold is expanded.

---

## Recommendations (no prompt changes yet)

### Fixture `evaluation.mode` (low-risk first)

| Fixture | Current | Suggested | Risk |
| --- | --- | --- | --- |
| slurry-bad-over-compressed-crop-nitrogen-table | exhaustive | **minimum** | low |
| slurry-good-definition-slurry-sssaho-2010 | exhaustive | **targeted** | low |
| slurry-good-simple-obligation-170kg-n | exhaustive | **targeted** | low |
| slurry-ugly-transitional-nvz-wales-reg-2 | exhaustive | **minimum** | low |
| slurry-ugly-appeal-nvz-designation-reg-6 | exhaustive | **minimum** | medium (or expand gold) |
| slurry-ugly-cross-reference-derogation-directive | exhaustive | **minimum** + classifier-tolerant eval | medium |

### Prompt changes

**None recommended** from this batch. Frontier extraction is materially complete on failed fragments; failures are policy, classifier, or matcher geometry.

### Evaluator / classifier (before next prompt iteration)

1. Effect-type equivalence: `permission` ↔ `application_scope` for eligibility-to-apply; `definition` ↔ `derogation` for `"X" means` clauses.
2. Multi-gold ↔ one-actual when conditions are enumerated inside one extracted proposition (reg 8 compost block).
3. Fix greedy 1:1 matching so subset gold rows can match the same actual row.
4. Classifier: stop emitting `unknown` on BS-equivalence sentences; prefer `definition` for inline means clauses in derogation parts.

---

## Batch verdict logic (updated)

`compute_batch_verdict` now emits **`fixture_policy_review_needed`** when ≥50% of fail rows are **extras-only** (all gold matched, `extra_actual_count > 0`, no evidence failures). **`failures_suggest_prompt_change`** is reserved for rows with unmatched expected propositions from omissions, weak subject/action, or missing conditions — not for exhaustive count mismatch alone.

Re-run batch or refresh summary from existing rows to pick up the new verdict in `prompt_lab_summary.json` / `PROMPT_LAB_SUMMARY.md`.
