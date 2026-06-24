# Slurry frontier export: normalisation before/after

**Baseline:** `runs/slurry-gb-principal-5-frontier-export` (frontier extraction, 5 principal sources, 678 propositions).

**After:** same proposition texts and source selection; **post-extraction normalisation v1** (classification → jurisdiction → labelling → relationship keys) re-applied in-process. No re-run of frontier LLM extraction (47m / cost unchanged; comparison isolates normalisation).

## Count summary

| Metric | Before | After | Δ |
| --- | ---: | ---: | ---: |
| Total propositions | 678 | 678 | 0 |
| Explorer visible (default filters) | 678 | 651 | -27 |
| Compliance-relevant only (after filter) | n/a | 404 | — |
| Hidden instrument metadata / citation / commencement | n/a | 27 | — |
| `is_compliance_relevant=true` | 0 | 404 | 404 |
| `is_compliance_relevant=false` | 0 | 274 | 274 |
| `is_comparison_anchor=true` | 0 | 627 | 627 |
| Generic `uk:these-regulations:*` keys | 12 | 0 | -12 |
| Cross-instrument `cross_reference_targets` | 126 | 0 | -126 |
| `semantic_comparison_index` buckets | 0 | 545 | 545 |
| `source_scoped_index` buckets | — | 614 | — |

## By proposition_tier

| Tier | Before | After | Δ |
| --- | ---: | ---: | ---: |
| (none) | 678 | 0 | -678 |
| definitional_rule | 0 | 42 | 42 |
| instrument_metadata | 0 | 27 | 27 |
| procedural_rule | 0 | 39 | 39 |
| relationship_reference | 0 | 46 | 46 |
| scope_rule | 0 | 42 | 42 |
| substantive_rule | 0 | 458 | 458 |
| unknown | 0 | 24 | 24 |

## By legal_effect_type

| Effect | Before | After | Δ |
| --- | ---: | ---: | ---: |
| (none) | 678 | 0 | -678 |
| appeal | 0 | 17 | 17 |
| application_scope | 0 | 42 | 42 |
| certification | 0 | 1 | 1 |
| citation | 0 | 4 | 4 |
| commencement | 0 | 21 | 21 |
| cross_reference | 0 | 46 | 46 |
| definition | 0 | 42 | 42 |
| derogation | 0 | 59 | 59 |
| enforcement | 0 | 18 | 18 |
| extent | 0 | 2 | 2 |
| inspection | 0 | 2 | 2 |
| notification | 0 | 1 | 1 |
| obligation | 0 | 308 | 308 |
| permission | 0 | 13 | 13 |
| power | 0 | 2 | 2 |
| prohibition | 0 | 59 | 59 |
| recordkeeping | 0 | 17 | 17 |
| unknown | 0 | 24 | 24 |

## What improved

- **Scope/application** rows (e.g. reg 1(d)) are no longer labelled as generic obligations; they get `scope_rule` / `application_scope`, specific labels, and `is_compliance_relevant=false`.
- **Instrument boilerplate** (citation, commencement, extent) is classified as `instrument_metadata` and hidden from default explorer view.
- **Generic cross-reference keys** (`uk:these-regulations:apply-to`) are replaced with source-scoped keys; **cross-instrument false links via `cross_reference_targets` are removed**.
- **`semantic_comparison_index`** populated (545 buckets) for cross-instrument *comparison hints* without auto-merging inventory.
- **Human notes** separated from `judit_extraction_meta` blobs (`review_notes` null when only machine meta).

## Obligation category → scope / metadata / definition

**111** propositions had LLM `categories: ["obligation"]` but normalised to scope, instrument metadata, or definition.

- `prop:bdddeff361c8d05e` **citation** / instrument_metadata: Citation as Nitrate Pollution Prevention Regulations 2015
- `prop:2ad380ca2d9206e9` **commencement** / instrument_metadata: Commencement on 1 May 2015
- `prop:4d933b91dd01103d` **application_scope** / scope_rule: Application to England
- `prop:de935c0fcdf5d5a7` **application_scope** / scope_rule: Application to nitrate vulnerable zones
- `prop:2d3577630a9c6c03` **definition** / definitional_rule: Definition of slurry
- `prop:8560e7285cdf5fa7` **definition** / definitional_rule: Definition of organic manure
- `prop:c6c82abaf0fe9568` **definition** / definitional_rule: Definition of spreading
- `prop:5e76402e434f88c4` **definition** / definitional_rule: Definition of relevant map (initial period)
- `prop:7427842461f5ddd6` **definition** / definitional_rule: Definition of relevant map (post-review)
- `prop:672ddc63ba212c4e` **commencement** / instrument_metadata: Secretary of State duty: transitional publication and notice (pre-2017…
- `prop:ade5560b077db2e2` **application_scope** / scope_rule: Transitional exemption for new holdings from Schedule 4 requirements
- `prop:9c8987c8a6c368a3` **application_scope** / scope_rule: Exemption: nitrogen fertiliser spreading in greenhouses
- `prop:712dd64f9b3224f4` **definition** / definitional_rule: Closed period definition for grassland
- `prop:e269106e8d6ddd0f` **definition** / definitional_rule: Closed period definition for tillage land
- `prop:c3aa60cc57841ec0` **definition** / definitional_rule: Definition of storage period for record-keeping
- … and 96 more

## Relationship links

- **Removed** (had targets, now empty): 93
- **Changed** (targets differ): 62

  - `prop:bdddeff361c8d05e` key `uk:these-regulations:may-be-cited-as` → dropped targets [prop:52f3a08c4afdb9a3, prop:f5e68ba42d96d7ca]
  - `prop:2ad380ca2d9206e9` key `uk:these-regulations:come-into-force-on` → dropped targets [prop:1790964aad57cabd, prop:70b04f5dfd14be85]
  - `prop:2d3577630a9c6c03` key `uk:slurry:means` → dropped targets [prop:a69b32f7fd9cc37e, prop:e25a239fecd804aa]
  - `prop:015c227500b5e9dd` key `uk:occupier-of-a-holding:must-ensure-that-the-total-amount-of-nitrogen-in-livestock-m` → dropped targets [prop:3f42718adc9ebf1a]
  - `prop:96087336672e3857` key `uk:occupier-of-a-holding:must-ensure-that-the-total-amount-of-nitrogen-in-organic-man` → dropped targets [prop:07852075fa482055, prop:8ed74b19f101b42f, prop:9bd7a375d749a938]
  - `prop:e170625d7d63d601` key `uk:slurry:maximum-amount-that-may-be-spread-at-any-one-time-is` → dropped targets [prop:20ce3268138d175b]
  - `prop:ad3f8dbb83f6d96f` key `uk:a-person:must-not-spread` → dropped targets [prop:3cfaaf1a19293d55, prop:587b94efa9e930d6, prop:7a349b08ba3650ca]
  - `prop:20a27393e7f92927` key `uk:solid-poultry-manure-that-does-not-have-bedding-mixed-into-it-and-is-stored-on-a-temporary-field-site:must-be-covered-with` → dropped targets [prop:e71d78a2e684d8a4]
  - **Before false link:** `prop:bdddeff361c8d05e` (lex-120b4f9c395b3f94) → `prop:f5e68ba42d96d7ca` (lex-e71fbbe3342ac0be) via `uk:these-regulations:may-be-cited-as`
  - **Before false link:** `prop:bdddeff361c8d05e` (lex-120b4f9c395b3f94) → `prop:52f3a08c4afdb9a3` (lex-2459c955ee13be52) via `uk:these-regulations:may-be-cited-as`
  - **Before false link:** `prop:2ad380ca2d9206e9` (lex-120b4f9c395b3f94) → `prop:70b04f5dfd14be85` (lex-e71fbbe3342ac0be) via `uk:these-regulations:come-into-force-on`
  - **Before false link:** `prop:2ad380ca2d9206e9` (lex-120b4f9c395b3f94) → `prop:1790964aad57cabd` (lex-2459c955ee13be52) via `uk:these-regulations:come-into-force-on`
  - **Before false link:** `prop:2d3577630a9c6c03` (lex-120b4f9c395b3f94) → `prop:e25a239fecd804aa` (lex-e71fbbe3342ac0be) via `uk:slurry:means`

## Label examples (generic → specific)

- `regulation 1(1)`: **Citation** → **Citation as Nitrate Pollution Prevention Regulations 2015**
- `regulation 1(2)`: **Commencement date** → **Commencement on 1 May 2015**
- `regulation 1(3)`: **Territorial application** → **Application to England**
- `regulation 1(2)`: **Territorial application** → **Application to Wales**
- `regulation 1(3)`: **Commencement date** → **Commencement on 1 April 2021**
- `regulation 1(a)`: **Citation** → **Citation as Water Resources (Control of Pollution) (Silage, Slurry and Agricultural Fuel Oil) (England) Regulat…**
- `regulation 1(b)`: **Territorial extent** → **Application to England**
- `regulation 1(a)`: **Citation** → **Citation as Reduction and Prevention of Agricultural Diffuse Pollution (England) Regulations 2018**
- `regulation 1(b)`: **Commencement date** → **Commencement on 2 April 2018**
- `regulation 1(c)`: **Territorial extent** → **Extent to England and Wales**
- `regulation 1(d)`: **Territorial application** → **Application to agricultural land in England**
- `regulation 1(5)`: **Territorial extent** → **Extent to Scotland**

## 2018 Diffuse Pollution — regulation 1 (`lex-2459c955ee13be52`)

| Locator | Before label | After label | After tier | After effect | Compliance | Anchor |
| --- | --- | --- | --- | --- | --- | --- |
| regulation 1(a) | Citation | Citation as Reduction and Prevention of Agricultural Diffuse Pollution (England) Regulations 2018 | instrument_metadata | citation | False | False |
| regulation 1(b) | Commencement date | Commencement on 2 April 2018 | instrument_metadata | commencement | False | False |
| regulation 1(c) | Territorial extent | Extent to England and Wales | instrument_metadata | extent | False | False |
| regulation 1(d) | Territorial application | Application to agricultural land in England | scope_rule | application_scope | False | True |

## Example: agricultural land in England

| Field | Before | After |
| --- | --- | --- |
| label | Territorial application | Application to agricultural land in England |
| categories | ['obligation'] | (unchanged) |
| proposition_tier | — | scope_rule |
| legal_effect_type | — | application_scope |
| territorial_application | — | ['England'] |
| is_compliance_relevant | — | False |
| is_comparison_anchor | — | True |
| cross_reference_key | uk:these-regulations:apply-to | lex-1aee0d95fb0bc6:application_scope:agricultural_land_in_england:england |
| cross_reference_targets | ['prop:6ef6d0757a5242d7'] | [] |
| semantic_comparison_key | — | application_scope:agricultural_land_in_england:england |

## Suspicious regressions / caveats

- **Proposition count unchanged** (678): normalisation only enriches fields; no rows added/removed.
- **LLM `categories` array is not rewritten** — still shows `obligation` on some scope rows; UI/filters should use `legal_effect_type` / `proposition_tier`, not raw categories.
- **Extent row on 2018 reg 1(c)** extends to England and Wales while (d) applies to agricultural land in England — territorially correct but analysts should read both extent and application_scope.
- **Boilerplate vs LLM `provision_type`:** text patterns (`may be cited as`, `come into force`, `apply to`) override noisy extraction meta (e.g. reg 1(a) cited as `definition` in frontier output).
- Re-running full frontier extraction could change proposition texts/counts; this report isolates normalisation only.

## Explorer noise

- Default explorer list shrinks from **678** to **651** (−27 hidden metadata/citation/commencement rows).
- **Compliance-only** filter retains **404** propositions (vs 404 flagged compliance-relevant).
- Example application-scope row is **visible** in default browse but **excluded** from compliance-only — substantive duties easier to scan.
