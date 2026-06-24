# Prompt-lab evaluation

**Result:** PASS

- **Case:** `slurry-ugly-schedule-livestock-manure-table` — UGLY — Schedule 1 livestock manure / nitrogen table
- **Run directory:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-ugly-schedule-livestock-manure-table`
- **Propositions:** 8 actual / 4 expected (4 matched)

## Summary

- **pass:** True
- **eval_status:** pass
- **evaluation_mode:** table_rows
- **allow_extra_actual:** True
- **matched_expected:** 4/4
- **extra_actual_count:** 4
- **suggested_focus:** —

## Warnings

- 4 extra actual propositions (allowed by table_rows mode)

## Checks

- **Evaluation mode:** `table_rows` (extra actual propositions allowed)

- **load:** ok
- **evaluation_mode:** ok
- **proposition_count:** ok
  - actual=8, expected=4, mode=table_rows
  - status: ok
- **extra_actual:** ok
  - 4 extra actual propositions (allowed by table_rows mode)
  - status: extras_allowed
- **no_actual_propositions:** ok
- **expected_row_matches:** ok
- **legal_effect_coverage:** ok
- **tier_coverage:** ok
- **boilerplate_classification:** ok
- **over_compression:** ok
- **checkable_count:** ok

## Expected proposition matches

### [matched] expected #1 — `definition`
- Matched actual: `prop-lex-120b4f9c395b3f94-001` (score 1.00)

### [matched] expected #2 — `definition`
- Matched actual: `prop-lex-120b4f9c395b3f94-002` (score 0.90)
- Weak: subject

### [matched] expected #3 — ``
- Matched actual: `prop-lex-120b4f9c395b3f94-008` (score 1.00)

### [matched] expected #4 — `definition`
- Matched actual: `prop-lex-120b4f9c395b3f94-003` (score 1.00)

## Extra actual propositions

_4 extra row(s); allowed by table_rows mode._

- `prop-lex-120b4f9c395b3f94-004` (definition, definitional_rule): A sow including litter up to 7kg fed on a diet without synthetic amino acids produces 10.9 litres of manure, 49 grams of
- `prop-lex-120b4f9c395b3f94-005` (definition, definitional_rule): A chicken from 17 weeks used for producing eggs for human consumption that is caged produces 0.12 kilograms of manure, 1
- `prop-lex-120b4f9c395b3f94-006` (definition, definitional_rule): A pig weighing from 66kg and intended for slaughter that is liquid fed produces 10 litres of manure, 33 grams of nitroge
- `prop-lex-120b4f9c395b3f94-007` (definition, definitional_rule): For a ewe, the daily manure, nitrogen and phosphate production figures include one or more suckled lambs until the lambs
