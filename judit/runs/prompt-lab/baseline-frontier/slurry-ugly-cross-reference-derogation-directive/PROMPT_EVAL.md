# Prompt-lab evaluation

**Result:** PASS

- **Case:** `slurry-ugly-cross-reference-derogation-directive` — UGLY — cross-reference-heavy derogation (regulation 36)
- **Run directory:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-ugly-cross-reference-derogation-directive`
- **Propositions:** 8 actual / 2 expected (2 matched)

## Summary

- **pass:** True
- **eval_status:** pass
- **evaluation_mode:** minimum
- **allow_extra_actual:** True
- **matched_expected:** 2/2
- **extra_actual_count:** 6
- **suggested_focus:** —

## Warnings

- 6 extra actual propositions (allowed by minimum mode)

## Checks

- **Evaluation mode:** `minimum` (extra actual propositions allowed)

- **load:** ok
- **evaluation_mode:** ok
- **proposition_count:** ok
  - actual=8, expected=2, mode=minimum
  - status: ok
- **extra_actual:** ok
  - 6 extra actual propositions (allowed by minimum mode)
  - status: extras_allowed
- **no_actual_propositions:** ok
- **expected_row_matches:** ok
- **legal_effect_coverage:** ok
- **tier_coverage:** ok
- **boilerplate_classification:** ok
- **over_compression:** ok
- **checkable_count:** ok

## Expected proposition matches

### [matched] expected #1 — `permission`
- Matched actual: `prop-lex-120b4f9c395b3f94-001` (score 1.05)

### [matched] expected #2 — `definition`
- Matched actual: `prop-lex-120b4f9c395b3f94-002` (score 1.00)

## Extra actual propositions

_6 extra row(s); allowed by minimum mode._

- `prop-lex-120b4f9c395b3f94-003` (derogation, substantive_rule): The reference to agricultural area does not include land covered by a greenhouse.
- `prop-lex-120b4f9c395b3f94-004` (derogation, substantive_rule): A derogation ceases to have effect unless the occupier sends to the Agency a written declaration that the derogation con
- `prop-lex-120b4f9c395b3f94-005` (obligation, substantive_rule): The written declaration must be sent to the Agency within 28 calendar days of the derogation being granted.
- `prop-lex-120b4f9c395b3f94-006` (obligation, substantive_rule): The application must be submitted between 1st October and 31st December in the calendar year preceding that to which the
- `prop-lex-120b4f9c395b3f94-007` (permission, substantive_rule): An application relating to the calendar year commencing on 1st January 2017 may be submitted by 20th March 2017.
- `prop-lex-120b4f9c395b3f94-008` (obligation, substantive_rule): The application must be made in the form and manner published by the Secretary of State.
