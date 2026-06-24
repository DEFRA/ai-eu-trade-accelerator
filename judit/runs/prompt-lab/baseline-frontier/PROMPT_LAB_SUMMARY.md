# Prompt-lab batch summary

**Verdict:** pass_with_warnings

All runnable fixtures passed; some rows have warnings or were skipped.

- **Output root:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier`
- **Mode:** `dry`
- **Fixtures run:** 10
- **Generated:** 2026-06-04T12:01:18.698877Z

## Results

| Fixture | Tier | Mode | Status | Matched | Actual/Expected | Extras | Run dir |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `slurry-bad-diffuse-2018-reg-1-boilerplate.json` | bad | exhaustive | pass | 4/4 | 4/4 | 0 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-bad-diffuse-2018-reg-1-boilerplate` |
| `slurry-bad-over-compressed-crop-nitrogen-table.json` | bad | minimum | warn | 1/1 | 8/1 | 7 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-bad-over-compressed-crop-nitrogen-table` |
| `slurry-bad-unless-except-organic-manure-250kg.json` | bad | minimum | warn | 8/8 | 8/8 | 7 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-bad-unless-except-organic-manure-250kg` |
| `slurry-good-definition-slurry-sssaho-2010.json` | good | targeted | warn | 1/1 | 8/1 | 7 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-good-definition-slurry-sssaho-2010` |
| `slurry-good-simple-obligation-170kg-n.json` | good | targeted | warn | 1/1 | 5/1 | 4 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-good-simple-obligation-170kg-n` |
| `slurry-good-simple-prohibition-spread-buffer.json` | good | targeted | warn | 1/1 | 7/1 | 6 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-good-simple-prohibition-spread-buffer` |
| `slurry-ugly-appeal-nvz-designation-reg-6.json` | ugly | minimum | warn | 1/1 | 8/1 | 7 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-ugly-appeal-nvz-designation-reg-6` |
| `slurry-ugly-cross-reference-derogation-directive.json` | ugly | minimum | warn | 2/2 | 8/2 | 6 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-ugly-cross-reference-derogation-directive` |
| `slurry-ugly-schedule-livestock-manure-table.json` | ugly | table_rows | warn | 4/4 | 8/4 | 4 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-ugly-schedule-livestock-manure-table` |
| `slurry-ugly-transitional-nvz-wales-reg-2.json` | ugly | minimum | warn | 2/2 | 4/2 | 3 | `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/prompt-lab/baseline-frontier/slurry-ugly-transitional-nvz-wales-reg-2` |

## Failure themes

### fixture/eval policy warning
- `slurry-bad-over-compressed-crop-nitrogen-table`
- `slurry-bad-unless-except-organic-manure-250kg`
- `slurry-good-definition-slurry-sssaho-2010`
- `slurry-good-simple-obligation-170kg-n`
- `slurry-good-simple-prohibition-spread-buffer`
- `slurry-ugly-appeal-nvz-designation-reg-6`
- `slurry-ugly-cross-reference-derogation-directive`
- `slurry-ugly-schedule-livestock-manure-table`
- `slurry-ugly-transitional-nvz-wales-reg-2`

## Per-fixture detail

### `slurry-bad-diffuse-2018-reg-1-boilerplate.json` — pass
- **Case:** slurry-bad-diffuse-2018-reg-1-boilerplate
- **Label:** BAD — Regulation 1 boilerplate (Diffuse Pollution 2018)

### `slurry-bad-over-compressed-crop-nitrogen-table.json` — warn
- **Case:** slurry-bad-over-compressed-crop-nitrogen-table
- **Label:** BAD — crop nitrogen table (regulation 12, long label risk)
- **Warnings:** 7 extra actual propositions (allowed by minimum mode)
- **Themes:** fixture_or_eval_policy_warning

### `slurry-bad-unless-except-organic-manure-250kg.json` — warn
- **Case:** slurry-bad-unless-except-organic-manure-250kg
- **Label:** BAD — unless / except / conditions (organic manure 250 kg limit)
- **Warnings:** 7 extra actual propositions (allowed by minimum mode); expected[0]: classification_mismatch (contained_in_actual); gold legal_effect accepted via equivalence; expected[1]: contained_in_actual match (expected row found inside actual conditions/evidence envelope); expected[2]: contained_in_actual match (expected row found inside actual conditions/evidence envelope); expected[3]: contained_in_actual match (expected row found inside actual conditions/evidence envelope); expected[4]: classification_mismatch (contained_in_actual); gold legal_effect accepted via equivalence; expected[6]: classification_mismatch (contained_in_actual); gold legal_effect accepted via equivalence; expected[7]: contained_in_actual match (expected row found inside actual conditions/evidence envelope)
- **Themes:** fixture_or_eval_policy_warning

### `slurry-good-definition-slurry-sssaho-2010.json` — warn
- **Case:** slurry-good-definition-slurry-sssaho-2010
- **Label:** GOOD — definition of slurry (SSSAHO England 2010)
- **Warnings:** 7 extra actual propositions (allowed by targeted mode)
- **Themes:** fixture_or_eval_policy_warning

### `slurry-good-simple-obligation-170kg-n.json` — warn
- **Case:** slurry-good-simple-obligation-170kg-n
- **Label:** GOOD — simple obligation (170 kg N per hectare holding limit)
- **Warnings:** 4 extra actual propositions (allowed by targeted mode)
- **Themes:** fixture_or_eval_policy_warning

### `slurry-good-simple-prohibition-spread-buffer.json` — warn
- **Case:** slurry-good-simple-prohibition-spread-buffer
- **Label:** GOOD — simple prohibition (organic manure spreading buffer)
- **Warnings:** 6 extra actual propositions (allowed by targeted mode)
- **Themes:** fixture_or_eval_policy_warning

### `slurry-ugly-appeal-nvz-designation-reg-6.json` — warn
- **Case:** slurry-ugly-appeal-nvz-designation-reg-6
- **Label:** UGLY — appeal and designation procedure (regulation 6)
- **Warnings:** 7 extra actual propositions (allowed by minimum mode)
- **Themes:** fixture_or_eval_policy_warning

### `slurry-ugly-cross-reference-derogation-directive.json` — warn
- **Case:** slurry-ugly-cross-reference-derogation-directive
- **Label:** UGLY — cross-reference-heavy derogation (regulation 36)
- **Warnings:** 6 extra actual propositions (allowed by minimum mode)
- **Themes:** fixture_or_eval_policy_warning

### `slurry-ugly-schedule-livestock-manure-table.json` — warn
- **Case:** slurry-ugly-schedule-livestock-manure-table
- **Label:** UGLY — Schedule 1 livestock manure / nitrogen table
- **Warnings:** 4 extra actual propositions (allowed by table_rows mode)
- **Themes:** fixture_or_eval_policy_warning

### `slurry-ugly-transitional-nvz-wales-reg-2.json` — warn
- **Case:** slurry-ugly-transitional-nvz-wales-reg-2
- **Label:** UGLY — transitional commencement (Wales NVZ reg 2)
- **Warnings:** 3 extra actual propositions (allowed by minimum mode); expected[0]: contained_in_actual match (expected row found inside actual conditions/evidence envelope)
- **Themes:** fixture_or_eval_policy_warning
