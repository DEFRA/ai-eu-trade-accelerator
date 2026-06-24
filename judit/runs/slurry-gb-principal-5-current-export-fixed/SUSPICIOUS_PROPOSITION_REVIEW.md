# Suspicious proposition review

Export: `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/slurry-gb-principal-5-current-export-fixed`
Propositions: **729**

_Deterministic review. No LLM. Semantic buckets and cross-jurisdiction matches are hints, not automatic defects._

## Executive summary

- **Duplicates blocker:** no (0 duplicate id group(s), 26 exact-text group(s))
- **Unknown classifications:** 0
- **Validation-warning propositions:** 34
- **Trace-warning propositions:** 13
- **Application-scope missing territory:** 29

### Top duplicate concerns

- `exact_text`: {"normalized_text_preview": "a slurry storage system must satisfy specified requirements.", "size": 3, "members": [{"source_id": "lex-805b03f284dcf364", "source_title": "The Water Resources (Control of Agricultural Pollution) (Wales) Regula
- `exact_text`: {"normalized_text_preview": "the base of a slurry storage tank must be impermeable.", "size": 3, "members": [{"source_id": "lex-805b03f284dcf364", "source_title": "The Water Resources (Control of Agricultural Pollution) (Wales) Regulations 
- `exact_text`: {"normalized_text_preview": "solid poultry manure that does not have bedding mixed into it and is stored on a temporary field site must be covered with an impermeable material.", "size": 2, "members": [{"source_id": "lex-120b4f9c395b3f94", 
- `exact_text`: {"normalized_text_preview": "a slurry store must have the capacity to store, in addition to the manure, any rainfall, washings or other liquid that enters the vessel (either directly or ind", "size": 2, "members": [{"source_id": "lex-120b4f
- `exact_text`: {"normalized_text_preview": "the agency has the function of enforcing these regulations.", "size": 2, "members": [{"source_id": "lex-120b4f9c395b3f94", "source_title": "The Nitrate Pollution Prevention Regulations 2015", "locator": "regulat
- `exact_text`: {"normalized_text_preview": "regulations 7, 8, 10, 11, 12, 13, 15, 16, 17, 19, 23, 25a, 25b, 26, 27, 28, 29, 30, 31, 32, and 34 do not apply to a new holding until the beginning of the seco", "size": 2, "members": [{"source_id": "lex-120b4f
- `exact_text`: {"normalized_text_preview": "silage effluent means effluent from silage.", "size": 2, "members": [{"source_id": "lex-805b03f284dcf364", "source_title": "The Water Resources (Control of Agricultural Pollution) (Wales) Regulations 2021", "loc
- `exact_text`: {"normalized_text_preview": "a reference to a slurry storage system includes a slurry storage tank and any reception pit and any effluent tank used in connection with the tank, and any chan", "size": 2, "members": [{"source_id": "lex-805b03
- `exact_text`: {"normalized_text_preview": "the base of a silo must extend beyond any walls of the silo.", "size": 2, "members": [{"source_id": "lex-805b03f284dcf364", "source_title": "The Water Resources (Control of Agricultural Pollution) (Wales) Regula
- `exact_text`: {"normalized_text_preview": "the base of a silo must be provided at its perimeter with channels designed and constructed so as to collect any silage effluent that escapes from the silo.", "size": 2, "members": [{"source_id": "lex-805b03f284
- `near_dup_same_locator`: {"source_id": "lex-120b4f9c395b3f94", "locator": "schedule 4, paragraph 1", "similarity": 1.0, "proposition_ids": ["prop:618dbaf3fb6cb2c7", "prop:6a882cfce1ad0a7a"], "classification": "true_duplicate"}
- `near_dup_same_locator`: {"source_id": "lex-805b03f284dcf364", "locator": "schedule 5, paragraph 4(2)", "similarity": 1.0, "proposition_ids": ["prop:61898ff33372b41c", "prop:684185c682539494"], "classification": "true_duplicate"}
- `near_dup_same_locator`: {"source_id": "lex-e71fbbe3342ac0be", "locator": "schedule 1, paragraph 4(2)", "similarity": 1.0, "proposition_ids": ["prop:dab7c4ffed1616c8", "prop:e6acb63e69963b44"], "classification": "true_duplicate"}
- `near_dup_same_locator`: {"source_id": "lex-120b4f9c395b3f94", "locator": "schedule 4, paragraph 2(a)", "similarity": 0.9797, "proposition_ids": ["prop:6d76e8502e4653e6", "prop:b2e70eb014d32d5e"], "classification": "semantic_comparison_candidate"}
- `near_dup_same_locator`: {"source_id": "lex-120b4f9c395b3f94", "locator": "schedule 3, paragraph 9(2)", "similarity": 0.9734, "proposition_ids": ["prop:5aa8659de42a2ecd", "prop:708d1efbb1f5b7cf"], "classification": "semantic_comparison_candidate"}

### Top quality concerns

- `npp_reg2_missing`: {'kind': 'npp_reg2_missing', 'severity': 'error'}
- `validation_warnings`: {'kind': 'validation_warnings', 'count': 34}
- `unknown_classifications`: {'kind': 'unknown_classifications', 'count': 0}
- `weak_compliance`: {'kind': 'weak_compliance', 'count': 12}
- `scope_missing_territory`: {'kind': 'scope_missing_territory', 'count': 29}

### Recommended priority fixes

1. Fix JSON extraction repair for quoted evidence_text; targeted re-extract NPP regulation:2
1. Tune evidence matcher for table rows / multi-row chunks
1. Territory inheritance pass for application_scope rows
1. Harden parser against JSON+prose responses (Wales schedule:2 pattern)

## Comparison with baseline export

Baseline: `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/slurry-gb-principal-5-frontier-export`
Overall: **mixed**

| Metric | Current | Baseline |
| --- | ---: | ---: |
| Propositions | 729 | 678 |
| Duplicate proposition ids | 0 | 1 |
| Exact-text duplicate groups | 26 | 23 |
| Validation-warning props | 34 | 0 |
| NPP reg:2 fragment props | 0 | 4 |

**Improved:** proposition_count +51; duplicate_proposition_id_count reduced
**Worsened:** validation_warning_propositions +34; NPP regulation:2 definitions lost (parse failure on fresh extract)

## A. Duplicate / near-duplicate analysis

### A1 Exact duplicate text (26 groups)

- size 3: prop:3b7cb873eeb1ff69, prop:0771ae321e2e26d1, prop:7f203083f6efa087
- size 3: prop:c3db9989546d68e1, prop:22b3e7d6165dbddb, prop:ccf7e24a6624c1f1
- size 2: prop:7bce8625b49c13f7, prop:909d275b41ea21dd
- size 2: prop:182a3207950b9c5f, prop:1e1721a38c46d16f
- size 2: prop:f2cdbb18646d4d7c, prop:beeb081604c03fee
- size 2: prop:618dbaf3fb6cb2c7, prop:6a882cfce1ad0a7a
- size 2: prop:a952f66e95a353c0, prop:36dbe94b6fb34a85
- size 2: prop:b054e9ae8a00e6f9, prop:def06ed39a06c5c4
- size 2: prop:e31a46a2689d7245, prop:f9695cb959a1d3e6
- size 2: prop:7677217d883bf6ca, prop:79e2c2476db17f22
- size 2: prop:805e771e3a2d7fcd, prop:63ff0ec4cfceaa56
- size 2: prop:61898ff33372b41c, prop:dab7c4ffed1616c8
- size 2: prop:4a5bc3a42ab3e4cf, prop:844fde98c9f1b727
- size 2: prop:96eae3b461eb50a0, prop:19ac1f89470e5f42
- size 2: prop:d4f0f391fc52736c, prop:129056261e8bafc7
- size 2: prop:03c5fd54333d0b98, prop:607cae56d17b272f
- size 2: prop:25763f285b606369, prop:3821bd7337107823
- size 2: prop:b6ec38e84a982733, prop:d9f1c91021ca88f8
- size 2: prop:405e191ad16ecf82, prop:f1c67d4e61f20a68
- size 2: prop:7352eaeadc9435cc, prop:46b7e9a36013d810

### A2 Same source + locator + similar text
- `lex-96613ffe71589e1e` `schedule 3, paragraph 5` sim=0.8951 semantic_comparison_candidate: ['prop:5836ab99f71f45c1', 'prop:9a461e6cac852d2f']
- `lex-120b4f9c395b3f94` `schedule 2, part 1, standard table, slurry section` sim=0.9118 semantic_comparison_candidate: ['prop:2b2d0bb5dc1d9ca8', 'prop:7a7252a8d1eee76b']
- `lex-120b4f9c395b3f94` `schedule 3, paragraph 8` sim=0.9583 semantic_comparison_candidate: ['prop:5ba7273843f23e88', 'prop:79dd87760cc6280d']
- `lex-120b4f9c395b3f94` `schedule 3, paragraph 9(2)` sim=0.9734 semantic_comparison_candidate: ['prop:5aa8659de42a2ecd', 'prop:708d1efbb1f5b7cf']
- `lex-120b4f9c395b3f94` `schedule 4, paragraph 1` sim=1.0 true_duplicate: ['prop:618dbaf3fb6cb2c7', 'prop:6a882cfce1ad0a7a']
- `lex-120b4f9c395b3f94` `schedule 4, paragraph 2(a)` sim=0.9797 semantic_comparison_candidate: ['prop:6d76e8502e4653e6', 'prop:b2e70eb014d32d5e']
- `lex-805b03f284dcf364` `regulation 21(a)` sim=0.9073 semantic_comparison_candidate: ['prop:654fa2457faf0c44', 'prop:7a56959cd11d6840']
- `lex-805b03f284dcf364` `schedule 5, paragraph 4(2)` sim=1.0 true_duplicate: ['prop:61898ff33372b41c', 'prop:684185c682539494']
- `lex-805b03f284dcf364` `schedule 5, paragraph 6` sim=0.9688 semantic_comparison_candidate: ['prop:96eae3b461eb50a0', 'prop:d4f0f391fc52736c']
- `lex-805b03f284dcf364` `schedule 6, paragraph 7` sim=0.9184 semantic_comparison_candidate: ['prop:3a006cc75960c2c8', 'prop:8d1595fb0542251d']
- `lex-96613ffe71589e1e` `schedule 1, paragraph 3(1)(a)` sim=0.9582 semantic_comparison_candidate: ['prop:12ba66f28b44aa0e', 'prop:dc97f8a0a9c04920']
- `lex-96613ffe71589e1e` `schedule 1, paragraph 3(1)(b)` sim=0.9522 semantic_comparison_candidate: ['prop:4ca2f4feb8be38a4', 'prop:aaf65a7825099903']
- `lex-96613ffe71589e1e` `schedule 1, paragraph 6` sim=0.9514 semantic_comparison_candidate: ['prop:3f00624560ac6ff3', 'prop:8ff63a82bb9d5fda']
- `lex-96613ffe71589e1e` `schedule 2, paragraph 7` sim=0.9677 semantic_comparison_candidate: ['prop:63e3f7105fadea70', 'prop:912b53f3b0360c05']
- `lex-96613ffe71589e1e` `schedule 3, paragraph 7` sim=0.9697 semantic_comparison_candidate: ['prop:0e64a774a712d58e', 'prop:a24a904922d66466']
- `lex-e71fbbe3342ac0be` `schedule 1, paragraph 4(2)` sim=1.0 true_duplicate: ['prop:dab7c4ffed1616c8', 'prop:e6acb63e69963b44']
- `lex-e71fbbe3342ac0be` `schedule 1, paragraph 6` sim=0.9688 semantic_comparison_candidate: ['prop:129056261e8bafc7', 'prop:19ac1f89470e5f42']
- `lex-e71fbbe3342ac0be` `schedule 2, paragraph 3` sim=0.9512 semantic_comparison_candidate: ['prop:4f6b606d485d58b1', 'prop:6b2d5a58f57bf426']
- `lex-e71fbbe3342ac0be` `schedule 2, paragraph 7` sim=0.9184 semantic_comparison_candidate: ['prop:e6e3d240d0649376', 'prop:fc1f0c00fbafb425']
- `lex-e71fbbe3342ac0be` `schedule 3, paragraph 7` sim=0.9471 semantic_comparison_candidate: ['prop:ac275fc38e8f8158', 'prop:f5cd27eff4ecc1be']

### A3 Duplicate proposition ids

### A3b Source-scoped key collisions (suspicious only)
- `lex-72aa053283580d:prohibition:a_person_must_not_spread` (key_too_coarse, size 4)
- `lex-72aa053283580d:derogation:does_not_apply_in_relation_to_a_new_holding` (key_too_coarse, size 4)
- `lex-e254792c87656c:prohibition:any_person_must_not_spread_organic_manure_with_high_readily_availab` (key_too_coarse, size 4)
- `lex-e254792c87656c:commencement:are_revoked` (key_too_coarse, size 4)
- `lex-72aa053283580d:obligation:occupier_must_calculate` (key_too_coarse, size 3)
- `lex-72aa053283580d:recordkeeping:occupier_must_make_a_record_of` (key_too_coarse, size 3)
- `lex-72aa053283580d:commencement:are_revoked` (key_too_coarse, size 3)
- `lex-72aa053283580d:obligation:fertilisation_plan_must_record` (key_too_coarse, size 3)
- `lex-1aee0d95fb0bc6:obligation:secretary_of_state_must_publish` (key_too_coarse, size 3)
- `lex-e254792c87656c:recordkeeping:occupier_must_make_a_record_of` (key_too_coarse, size 3)
- `lex-e254792c87656c:obligation:occupier_must_ensure_that_no_person_ploughs` (key_too_coarse, size 3)
- `lex-e254792c87656c:obligation:fertilisation_account_must_record` (key_too_coarse, size 3)
- `lex-e254792c87656c:obligation:occupier_must_ensure_that_no_person_spreads_organic_manure_within` (key_too_coarse, size 3)
- `lex-e254792c87656c:obligation:enhanced_nutrient_management_plan_must_record` (key_too_coarse, size 3)
- `lex-5b13540a30862d:enforcement:is_guilty_of_an_offence_and_liable_to` (key_too_coarse, size 3)
- `lex-72aa053283580d:prohibition:a_person_must_not_spread_nitrogen_fertiliser` (key_too_coarse, size 2)
- `lex-72aa053283580d:permission:occupier_of_a_holding_who_has_submitted_undertaking_to_o_may_spread_organic_manu` (key_too_coarse, size 2)
- `lex-72aa053283580d:definition:the_closed_period` (key_too_coarse, size 2)
- `lex-72aa053283580d:recordkeeping:occupier_of_a_new_holding_with_livestock_must_calculate_and_record_the_amount_of` (key_too_coarse, size 2)
- `lex-72aa053283580d:definition:relevant_map` (key_too_coarse, size 2)
- `lex-72aa053283580d:obligation:occupier_of_a_holding_must_within_one_month_of_the_introduction_of_the_animals` (key_too_coarse, size 2)
- `lex-72aa053283580d:obligation:the_agency_must_refuse` (key_too_coarse, size 2)
- `lex-72aa053283580d:obligation:a_report_under_paragraph_1_must_contain` (key_too_coarse, size 2)
- `lex-72aa053283580d:obligation:occupier_must_update` (key_too_coarse, size 2)
- `lex-72aa053283580d:obligation:occupier_must_calculate_and_record` (key_too_coarse, size 2)
- `lex-72aa053283580d:obligation:accounts_must_record` (key_too_coarse, size 2)
- `lex-72aa053283580d:obligation:occupier_must_carry_out_phosphorus_soil_sampling_and_analysis_on_` (key_too_coarse, size 2)
- `lex-72aa053283580d:application_scope:a_new_holding_until_the_beginning_of_the_second_calendar` (key_too_coarse, size 2)
- `lex-72aa053283580d:application_scope:a_new_holding_until_31st_july_in_the_third_calendar_year` (key_too_coarse, size 2)
- `lex-1aee0d95fb0bc6:prohibition:land_manager_must_ensure_that_a_livestock_feeder_is_not_positioned` (key_too_coarse, size 2)
- `lex-1aee0d95fb0bc6:obligation:land_manager_must_ensure_that_reasonable_precautions_are_taken_to_pre` (key_too_coarse, size 2)
- `lex-1aee0d95fb0bc6:prohibition:land_manager_must_ensure_that_organic_manure_or_manufactured_fertilis` (key_too_coarse, size 2)
- `lex-1aee0d95fb0bc6:prohibition:land_manager_must_ensure_that_organic_manure_is_not_stored_on_agricul` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:the_occupier_must_update` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:the_risk_map_must_show` (key_too_coarse, size 2)
- `lex-e254792c87656c:prohibition:any_person_may_not_spread_nitrogen_fertiliser` (key_too_coarse, size 2)
- `lex-e254792c87656c:commencement:do_not_apply` (key_too_coarse, size 2)
- `lex-e254792c87656c:prohibition:any_person_must_not_spread_manufactured_nitrogen_fertiliser` (key_too_coarse, size 2)
- `lex-e254792c87656c:derogation:does_not_apply_to` (key_too_coarse, size 2)
- `lex-e254792c87656c:recordkeeping:occupier_of_a_holding_with_livestock_must_maintain_a_record_of` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:occupier_must_calculate` (key_too_coarse, size 2)
- `lex-e254792c87656c:definition:area_of_the_holding` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:occupier_must_record_within_one_week_of_spreading_organic_manure` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:occupier_must_record` (key_too_coarse, size 2)
- `lex-e254792c87656c:enforcement:is_guilty_of_an_offence_and_liable` (key_too_coarse, size 2)
- `lex-e254792c87656c:commencement:substitute` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:occupier_of_a_holding_must_ensure_that_the_total_amount_of_nitrogen_in_organic` (key_too_coarse, size 2)
- `lex-e254792c87656c:definition:cattle_slurry` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:occupier_must_undertake_soil_sampling_analysis_of_at_least_every_` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:person_sampling_slurry_and_other_liquid_and_semi_liquid__must_take` (key_too_coarse, size 2)
- `lex-e254792c87656c:prohibition:effluent_tank_must_have_capacity_not_less_than` (key_too_coarse, size 2)
- `lex-e254792c87656c:obligation:base_and_walls_of_slurry_storage_tank_effluent_tank_chan_must_be_protected_again` (key_too_coarse, size 2)
- `lex-684df97ee335a2:prohibition:slurry_storage_tank_effluent_tank_channels_pipes_recepti_shall_not_be_situated_w` (key_too_coarse, size 2)
- `lex-5b13540a30862d:commencement:are_revoked` (key_too_coarse, size 2)
- `lex-5b13540a30862d:application_scope:a_silo_slurry_storage_system_or_fuel_storage_tank_which_` (key_too_coarse, size 2)

### A4 Semantic comparison (top 30 buckets)
- size 6: `recordkeeping:occupier_must_make_a_record_of`
- size 5: `obligation:occupier_must_calculate`
- size 4: `definition:slurry`
- size 4: `derogation:does_not_apply_in_relation_to_a_new_holding`
- size 4: `prohibition:a_person_must_not_spread`
- size 4: `prohibition:any_person_must_not_spread_organic_manure_with_high_readily_availab`
- size 3: `definition:slurry_storage_system`
- size 3: `enforcement:is_guilty_of_an_offence_and_liable_to`
- size 3: `enforcement:is_guilty_of_the_offence_and_liable_to_be_proceeded_agai`
- size 3: `obligation:base_and_walls_of_slurry_storage_tank_effluent_tank_chan_must_be_prot`
- size 3: `obligation:enhanced_nutrient_management_plan_must_record`
- size 3: `obligation:fertilisation_account_must_record`
- size 3: `obligation:fertilisation_plan_must_record`
- size 3: `obligation:occupier_must_ensure_that_no_person_ploughs`
- size 3: `obligation:occupier_must_ensure_that_no_person_spreads_organic_manure_within`
- size 3: `obligation:secretary_of_state_must_publish`
- size 3: `obligation:slurry_storage_system_must_satisfy`
- size 3: `obligation:slurry_storage_tank_effluent_tank_or_reception_pit_fitte_must_have_tw`
- size 3: `permission:occupier_of_a_holding_who_has_submitted_undertaking_to_o_may_spread_o`
- size 2: `application_scope:a_new_holding_until_31st_july_in_the_third_calendar_year`
- size 2: `application_scope:a_new_holding_until_the_beginning_of_the_second_calendar`
- size 2: `application_scope:a_silo_slurry_storage_system_or_fuel_storage_tank_which_`
- size 2: `application_scope:england:england`
- size 2: `definition:an_expression_used_in_paragraph_1_and_in_that_order`
- size 2: `definition:area_of_the_holding`
- size 2: `definition:cattle_slurry`
- size 2: `definition:certified_compost`
- size 2: `definition:construction`
- size 2: `definition:organic_manure`
- size 2: `definition:organic_manure_with_high_readily_available_nitrogen`

### A5 High locator counts

## B. Suspicious quality

### D1 NPP 2015 regulation:2
{
  "present_in_export": false,
  "extraction_failure": true,
  "failure_summary": "chunk 1/1: model call or JSON parse failed: Expecting ',' delimiter: line 18 column 26 (char 1240)",
  "recommendation": "targeted re-extract after JSON parser fix (escaped quotes in evidence_text)"
}

### D2 Wales schedule:2
{
  "present_in_export": false,
  "extraction_failure": true,
  "failure_summary": "chunk 1/1: model call or JSON parse failed: Extra data: line 4 column 1 (char 25)",
  "substantive_loss_likely_low": true,
  "note": "Model returned empty propositions plus prose after JSON fence; fragment is fruit species reference table"
}

### B1 Unknown classifications
