# Prompt-lab evaluation

**Result:** PASS

- **Case:** `slurry-good-simple-obligation-170kg-n` — GOOD — simple obligation (170 kg N per hectare holding limit)
- **Run directory:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-good-simple-obligation-170kg-n`
- **Propositions:** 5 actual / 1 expected (1 matched)

## Summary

- **pass:** True
- **eval_status:** pass
- **evaluation_mode:** targeted
- **allow_extra_actual:** True
- **matched_expected:** 1/1
- **extra_actual_count:** 4
- **suggested_focus:** —

## Warnings

- 4 extra actual propositions (allowed by targeted mode)

## Checks

- **Evaluation mode:** `targeted` (extra actual propositions allowed)

- **load:** ok
- **evaluation_mode:** ok
- **proposition_count:** ok
  - actual=5, expected=1, mode=targeted
  - status: ok
- **extra_actual:** ok
  - 4 extra actual propositions (allowed by targeted mode)
  - status: extras_allowed
- **no_actual_propositions:** ok
- **expected_row_matches:** ok
- **legal_effect_coverage:** ok
- **tier_coverage:** ok
- **boilerplate_classification:** ok
- **over_compression:** ok
- **checkable_count:** ok

## Expected proposition matches

### [matched] expected #1 — `obligation`
- Matched actual: `prop-lex-120b4f9c395b3f94-001` (score 1.05)

## Extra actual propositions

_4 extra row(s); allowed by targeted mode._

- `prop-lex-120b4f9c395b3f94-002` (derogation, substantive_rule): The nitrogen limit in paragraph (1) does not apply where the occupier has been granted a derogation.
- `prop-lex-120b4f9c395b3f94-003` (obligation, substantive_rule): The amount of nitrogen for the purposes of this regulation is to be calculated in accordance with Schedule 1.
- `prop-lex-120b4f9c395b3f94-004` (definition, definitional_rule): In calculating the area of a holding for nitrogen spreading purposes, no account is to be taken of surface waters, hards
- `prop-lex-120b4f9c395b3f94-005` (definition, definitional_rule): In calculating the area of a holding for nitrogen spreading purposes, no account is to be taken of land which is covered
