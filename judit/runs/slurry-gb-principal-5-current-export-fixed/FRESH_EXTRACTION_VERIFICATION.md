# Fresh extraction export verification

**Generated:** 2026-06-04T14:27:55.190954Z
**Export:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/slurry-gb-principal-5-current-export-fixed`
**Status:** PASS

## Summary

| Measure | Value |
| --- | ---: |
| Propositions | 729 |
| Errors | 0 |
| Warnings | 32 |

## Export presence

| Artifact | Present |
| --- | --- |
| `manifest_json` | yes |
| `model_md` | yes |
| `normalisation_quality_embedded` | yes |
| `normalisation_quality_json` | yes |
| `normalisation_quality_md` | yes |
| `proposition_extraction_traces_json` | yes |
| `propositions_json` | yes |

## Counts

- **total:** 729
- **compliance_relevant:** 508
- **comparison_anchor:** 702
- **unknown_tier:** 0
- **unknown_effect:** 0
- **application_scope:** 33
- **cross_reference:** 3
- **definition:** 66
- **table_or_numeric_looking:** 375

### by_source

- The Control of Pollution (Silage, Slurry and Agricultural Fuel Oil) (Scotland) Regulations 2003: 91
- The Nitrate Pollution Prevention Regulations 2015: 226
- The Reduction and Prevention of Agricultural Diffuse Pollution (England) Regulations 2018: 51
- The Water Resources (Control of Agricultural Pollution) (Wales) Regulations 2021: 265
- The Water Resources (Control of Pollution) (Silage, Slurry and Agricultural Fuel Oil) (England) Regulations 2010: 96

### by_proposition_tier

- definitional_rule: 66
- instrument_metadata: 27
- procedural_rule: 49
- relationship_reference: 3
- scope_rule: 33
- substantive_rule: 551

### by_legal_effect_type

- appeal: 19
- application_scope: 33
- certification: 4
- citation: 4
- commencement: 22
- cross_reference: 3
- definition: 66
- derogation: 31
- enforcement: 22
- extent: 1
- inspection: 2
- notification: 2
- obligation: 386
- permission: 36
- power: 3
- prohibition: 72
- recordkeeping: 23

## Evidence / debug health

- **low_confidence_count:** 0
- **missing_evidence_quote_count:** 0
- **repaired_json_job_count:** 0
- **repaired_json_proposition_count:** 0
- **trace_warning_count:** 13
- **validation_error_count:** 34

## Prompt-lab anchors (NPP 2015 locators)

| Locator | Propositions | Compliance-relevant | Effect types |
| --- | ---: | ---: | --- |
| regulation 8 | 4 | 0 | definition, derogation, permission |
| regulation 17 | 4 | 2 | permission, prohibition |
| Schedule 1 | 4 | 0 | definition |
| regulation 36 | 4 | 2 | derogation, obligation, permission |
| regulation 6 | 4 | 0 | appeal |

## Findings

- **warning** `prop:d9a5c5dacf8e8425` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:24622abea42e4591` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:52ac3da904dfb187` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:9f01838a7132a43a` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:9bd1b14b6bb5d5db` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:6a882cfce1ad0a7a` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:e15cf194e096319b` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:066ff1a0c7726bdd` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:618dbaf3fb6cb2c7` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:1684a94afbe16a82` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:3b94bffa42c2ff41` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:51d594dfe9f0e5b1` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:11f79425dbf3f3aa` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:a33d4a9f809b0e55` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:8229610a0cbd31e4` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:eba3ad981f963e6c` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:d63bb5e5e661590a` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:541b129649a57768` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:c80d22fd31cf207f` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:88419ac49fe4a7f9` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:85f2f607beb4269f` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:e04050776754fff1` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:61f5a822c858bbb4` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:6b0c6df3f45f8e11` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:6e00d5cbcc8c32c3` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:5f42dc230daba578` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:3b0ab9118a553702` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:040545de83474159` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:8afc2ec3a3cfba32` (`scope_application_conflict`): application_scope without territorial_application
- **warning** (`validation_errors`): one or more propositions have validation errors in extraction metadata
- **warning** (`trace_warnings`): one or more propositions have trace warnings in extraction metadata
- **warning** (`npp_reg2_definition_anchors`): NPP 2015 regulation 2 missing expected definition anchors: slurry, organic manure, agricultural, spreading
