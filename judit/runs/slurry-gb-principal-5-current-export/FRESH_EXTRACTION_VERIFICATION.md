# Fresh extraction export verification

**Generated:** 2026-06-04T14:27:08.544878Z
**Export:** `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/slurry-gb-principal-5-current-export`
**Status:** FAIL

## Summary

| Measure | Value |
| --- | ---: |
| Propositions | 729 |
| Errors | 3 |
| Warnings | 40 |

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
- **comparison_anchor:** 698
- **unknown_tier:** 4
- **unknown_effect:** 4
- **application_scope:** 33
- **cross_reference:** 3
- **definition:** 61
- **table_or_numeric_looking:** 375

### by_source

- The Control of Pollution (Silage, Slurry and Agricultural Fuel Oil) (Scotland) Regulations 2003: 91
- The Nitrate Pollution Prevention Regulations 2015: 226
- The Reduction and Prevention of Agricultural Diffuse Pollution (England) Regulations 2018: 51
- The Water Resources (Control of Agricultural Pollution) (Wales) Regulations 2021: 265
- The Water Resources (Control of Pollution) (Silage, Slurry and Agricultural Fuel Oil) (England) Regulations 2010: 96

### by_proposition_tier

- definitional_rule: 61
- instrument_metadata: 27
- procedural_rule: 49
- relationship_reference: 3
- scope_rule: 33
- substantive_rule: 552
- unknown: 4

### by_legal_effect_type

- appeal: 19
- application_scope: 33
- certification: 4
- citation: 4
- commencement: 22
- cross_reference: 3
- definition: 61
- derogation: 31
- enforcement: 22
- extent: 1
- inspection: 2
- notification: 2
- obligation: 385
- permission: 37
- power: 3
- prohibition: 73
- recordkeeping: 23
- unknown: 4

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
| Schedule 1 | 4 | 0 | definition, unknown |
| regulation 36 | 4 | 2 | derogation, obligation, permission |
| regulation 6 | 4 | 0 | appeal |

## Findings

- **error** (`duplicate_proposition_id`): duplicate proposition id prop:a0c4c917216ec053 appears 2 times
- **error** (`duplicate_proposition_key`): duplicate proposition_key 'lex-120b4f9c395b3f94:schedule-4-paragraph-1:p001' within source 'lex-120b4f9c395b3f94' (2 rows)
- **error** (`duplicate_proposition_version_id`): duplicate proposition_version_id 'pver:lex-120b4f9c395b3f94:schedule-4-paragraph-1:p001:snap-lex-120b4f9c395b3f94-v1:run-001' (2 propositions)
- **warning** `prop:de935c0fcdf5d5a7` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:204ed1287e94d38f` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:ade5560b077db2e2` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:d6e668b72bea0040` (`unknown_proposition_tier`): proposition_tier is unknown
- **warning** `prop:d6e668b72bea0040` (`unknown_legal_effect_type`): legal_effect_type is unknown
- **warning** `prop:94b1f3f8c5159534` (`unknown_proposition_tier`): proposition_tier is unknown
- **warning** `prop:94b1f3f8c5159534` (`unknown_legal_effect_type`): legal_effect_type is unknown
- **warning** `prop:bbd608a46cce603c` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:609f1c267b1f0fa9` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:3bce0c473e8e7caf` (`unknown_proposition_tier`): proposition_tier is unknown
- **warning** `prop:3bce0c473e8e7caf` (`unknown_legal_effect_type`): legal_effect_type is unknown
- **warning** `prop:28c134de2f220dd0` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:c2b30fe7dbec9ee4` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:43fbf37bff8a772e` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:146a2dd2e3b9db01` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:443dceaf85f3b605` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:62be7a3f42843969` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:75f218cb6e3a852e` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:9bd7a375d749a938` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:b84ca62579a84293` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:6b4cfb122798b9dc` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:922e9797bef1488c` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:edf8825435468a36` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:69fa12ff59c93a00` (`unknown_proposition_tier`): proposition_tier is unknown
- **warning** `prop:69fa12ff59c93a00` (`unknown_legal_effect_type`): legal_effect_type is unknown
- **warning** `prop:a882a661a84b1804` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:3114bc398b37c7b3` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:f80da0f6eb5b0560` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:68f81b26488097fa` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:26b31c75a7eee3dd` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:489cdbceaf1f0114` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:f2c933a18a0c972e` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:ecf2684f2df215fc` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:762d92c26c0995a2` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:facaff6d42d80d48` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:0c81b22c6e4966bc` (`scope_application_conflict`): application_scope without territorial_application
- **warning** `prop:046aca888dc23ec7` (`scope_application_conflict`): application_scope without territorial_application
- **warning** (`validation_errors`): one or more propositions have validation errors in extraction metadata
- **warning** (`trace_warnings`): one or more propositions have trace warnings in extraction metadata
- **warning** (`npp_reg2_definition_anchors`): NPP 2015 regulation 2 missing expected definition anchors: slurry, organic manure, agricultural, spreading
