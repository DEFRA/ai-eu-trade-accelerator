# Prompt-lab evaluation

**Result:** PASS

- **Case:** `slurry-bad-unless-except-organic-manure-250kg` — BAD — unless / except / conditions (organic manure 250 kg limit)
- **Run directory:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-bad-unless-except`
- **Propositions:** 8 actual / 8 expected (8 matched)

## Summary

- **pass:** True
- **eval_status:** pass
- **evaluation_mode:** minimum
- **allow_extra_actual:** True
- **matched_expected:** 8/8
- **extra_actual_count:** 0
- **suggested_focus:** —

## Warnings

- over_compression findings present (1); warn-only in minimum mode

## Checks

- **Evaluation mode:** `minimum` (extra actual propositions allowed)

- **load:** ok
- **evaluation_mode:** ok
- **proposition_count:** ok
  - actual=8, expected=8, mode=minimum
  - status: ok
- **extra_actual:** ok
  - no extra actual propositions
  - status: none
- **no_actual_propositions:** ok
- **expected_row_matches:** ok
- **legal_effect_coverage:** ok
- **tier_coverage:** ok
- **boilerplate_classification:** ok
- **over_compression:** ok
  - single proposition contains multiple modal verbs (possible over-compression)
- **checkable_count:** ok

## Expected proposition matches

### [matched] expected #1 — `obligation`
- Matched actual: `prop-lex-120b4f9c395b3f94-001` (score 1.05)

### [matched] expected #2 — `permission`
- Matched actual: `prop-lex-120b4f9c395b3f94-002` (score 1.05)

### [matched] expected #3 — `prohibition`
- Matched actual: `prop-lex-120b4f9c395b3f94-003` (score 1.05)

### [matched] expected #4 — `prohibition`
- Matched actual: `prop-lex-120b4f9c395b3f94-004` (score 1.05)

### [matched] expected #5 — `obligation`
- Matched actual: `prop-lex-120b4f9c395b3f94-005` (score 1.05)

### [matched] expected #6 — `derogation`
- Matched actual: `prop-lex-120b4f9c395b3f94-006` (score 1.05)

### [matched] expected #7 — `prohibition`
- Matched actual: `prop-lex-120b4f9c395b3f94-007` (score 1.05)

### [matched] expected #8 — `derogation`
- Matched actual: `prop-lex-120b4f9c395b3f94-008` (score 1.05)
