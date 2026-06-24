# Prompt-lab evaluation

**Result:** PASS

- **Case:** `slurry-bad-unless-except-organic-manure-250kg` — BAD — unless / except / conditions (organic manure 250 kg limit)
- **Run directory:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-bad-unless-except-organic-manure-250kg`
- **Propositions:** 8 actual / 8 expected (8 matched)

## Summary

- **pass:** True
- **eval_status:** pass
- **evaluation_mode:** minimum
- **allow_extra_actual:** True
- **matched_expected:** 8/8
- **extra_actual_count:** 7
- **suggested_focus:** —

## Warnings

- 7 extra actual propositions (allowed by minimum mode)
- expected[0]: classification_mismatch (contained_in_actual); gold legal_effect accepted via equivalence
- expected[1]: contained_in_actual match (expected row found inside actual conditions/evidence envelope)
- expected[2]: contained_in_actual match (expected row found inside actual conditions/evidence envelope)
- expected[3]: contained_in_actual match (expected row found inside actual conditions/evidence envelope)
- expected[4]: classification_mismatch (contained_in_actual); gold legal_effect accepted via equivalence
- expected[6]: classification_mismatch (contained_in_actual); gold legal_effect accepted via equivalence
- expected[7]: contained_in_actual match (expected row found inside actual conditions/evidence envelope)

## Checks

- **Evaluation mode:** `minimum` (extra actual propositions allowed)

- **load:** ok
- **evaluation_mode:** ok
- **proposition_count:** ok
  - actual=8, expected=8, mode=minimum
  - status: ok
- **extra_actual:** ok
  - 7 extra actual propositions (allowed by minimum mode)
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
- Matched actual: `prop-lex-120b4f9c395b3f94-004` (score 0.95)
- Weak: subject

### [matched] expected #2 — `permission`
- Matched actual: `prop-lex-120b4f9c395b3f94-001` (score 1.05)

### [matched] expected #3 — `prohibition`
- Matched actual: `prop-lex-120b4f9c395b3f94-003` (score 1.05)

### [matched] expected #4 — `prohibition`
- Matched actual: `prop-lex-120b4f9c395b3f94-003` (score 1.05)

### [matched] expected #5 — `obligation`
- Matched actual: `prop-lex-120b4f9c395b3f94-003` (score 0.95)
- Weak: subject

### [matched] expected #6 — `derogation`
- Matched actual: `prop-lex-120b4f9c395b3f94-005` (score 1.05)

### [matched] expected #7 — `prohibition`
- Matched actual: `prop-lex-120b4f9c395b3f94-005` (score 1.05)

### [matched] expected #8 — `derogation`
- Matched actual: `prop-lex-120b4f9c395b3f94-008` (score 1.05)

## Extra actual propositions

_7 extra row(s); allowed by minimum mode._

- `prop-lex-120b4f9c395b3f94-001` (permission, substantive_rule): The occupier of a holding must ensure that in any twelve-month period, the total amount of nitrogen in organic manure sp
- `prop-lex-120b4f9c395b3f94-002` (permission, substantive_rule): The occupier may exceed the 250kg nitrogen limit if organic manure is in the form of certified green compost or certifie
- `prop-lex-120b4f9c395b3f94-003` (prohibition, substantive_rule): For certified green compost or green/food compost, the total amount of nitrogen spread on any given hectare must not exc
- `prop-lex-120b4f9c395b3f94-004` (prohibition, substantive_rule): For orchard land, the total amount of nitrogen in certified green compost or green/food compost spread as mulch must not
- `prop-lex-120b4f9c395b3f94-006` (derogation, substantive_rule): For the purposes of the grassland exemption, the area of the holding does not include any land on which the occupier doe
- `prop-lex-120b4f9c395b3f94-007` (obligation, substantive_rule): For the purposes of regulation 8, the total amount of nitrogen in organic manure is to be calculated by reference to the
- `prop-lex-120b4f9c395b3f94-008` (derogation, substantive_rule): The 250kg nitrogen limit does not apply to land covered by a greenhouse for the whole of the period concerned.
