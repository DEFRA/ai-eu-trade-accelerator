# Prompt-lab evaluation

**Result:** PASS

- **Case:** `slurry-good-simple-prohibition-spread-buffer` — GOOD — simple prohibition (organic manure spreading buffer)
- **Run directory:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-good-prohibition`
- **Propositions:** 8 actual / 1 expected (1 matched)

## Summary

- **pass:** True
- **eval_status:** pass
- **evaluation_mode:** targeted
- **allow_extra_actual:** True
- **matched_expected:** 1/1
- **extra_actual_count:** 7
- **suggested_focus:** —

## Warnings

- 7 extra actual propositions (allowed by targeted mode)

## Checks

- **Evaluation mode:** `targeted` (extra actual propositions allowed)

- **load:** ok
- **evaluation_mode:** ok
- **proposition_count:** ok
  - actual=8, expected=1, mode=targeted
  - status: ok
- **extra_actual:** ok
  - 7 extra actual propositions (allowed by targeted mode)
  - status: extras_allowed
- **no_actual_propositions:** ok
- **expected_row_matches:** ok
- **legal_effect_coverage:** ok
- **tier_coverage:** ok
- **boilerplate_classification:** ok
- **over_compression:** ok
- **checkable_count:** ok

## Expected proposition matches

### [matched] expected #1 — `prohibition`
- Matched actual: `prop-lex-120b4f9c395b3f94-001` (score 0.95)
- Weak: action

## Extra actual propositions

_7 extra row(s); allowed by targeted mode._

- `prop-lex-120b4f9c395b3f94-002` (permission, substantive_rule): Livestock manure (other than slurry or poultry manure) may be spread on land managed for breeding wader birds or species
- `prop-lex-120b4f9c395b3f94-003` (notification, procedural_rule): Land is designated for the livestock manure exception if it is notified as a site of special scientific interest under t
- `prop-lex-120b4f9c395b3f94-004` (unknown, unknown): Land is designated for the livestock manure exception if it is subject to an agri-environmental commitment entered into 
- `prop-lex-120b4f9c395b3f94-005` (permission, substantive_rule): Organic manure in the form of slurry, sewage sludge or anaerobic digestate may be spread except within 6 metres of surfa
- `prop-lex-120b4f9c395b3f94-006` (unknown, unknown): Anaerobic digestate means the product of anaerobic digestion other than from sewage or material in a landfill.
- `prop-lex-120b4f9c395b3f94-007` (prohibition, substantive_rule): A person must not spread organic manure within 50 metres of a borehole, spring or well.
- `prop-lex-120b4f9c395b3f94-008` (application_scope, scope_rule): The regulation on spreading organic manure does not apply to land which is covered by a greenhouse.
