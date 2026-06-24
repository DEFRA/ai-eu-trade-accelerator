# Suspicious proposition review

Export: `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/slurry-gb-principal-5-current-export`
Propositions: **729**

_Deterministic review. No LLM. Semantic buckets and cross-jurisdiction matches are hints, not automatic defects._

## Executive summary

- **Duplicates blocker:** yes (2 duplicate id group(s), 26 exact-text group(s))
- **Unknown classifications:** 4
- **Validation-warning propositions:** 34
- **Trace-warning propositions:** 13
- **Application-scope missing territory:** 29

### Top duplicate concerns

- `duplicate_id`: {"proposition_id": "prop:a0c4c917216ec053", "count": 2, "locators": ["Schedule 3, paragraph 3", "Schedule 3, paragraph 3"], "source_fragment_ids": ["frag-lex-96613ffe71589e1e-035", "frag-lex-96613ffe71589e1e-038"]}
- `duplicate_id`: {"proposition_id": "prop:f80da0f6eb5b0560", "count": 2, "locators": ["Schedule 1A, paragraph 3", "Schedule 1A, paragraph 3"], "source_fragment_ids": ["frag-lex-805b03f284dcf364-053", "frag-lex-805b03f284dcf364-056"]}
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
- `near_dup_same_locator`: {"source_id": "lex-120b4f9c395b3f94", "locator": "schedule 4, paragraph 1", "similarity": 1.0, "proposition_ids": ["prop:146a2dd2e3b9db01", "prop:28c134de2f220dd0"], "classification": "true_duplicate"}
- `near_dup_same_locator`: {"source_id": "lex-805b03f284dcf364", "locator": "schedule 5, paragraph 4(2)", "similarity": 1.0, "proposition_ids": ["prop:2b9bae63bc7ba411", "prop:6c278ed82d94a37d"], "classification": "true_duplicate"}
- `near_dup_same_locator`: {"source_id": "lex-e71fbbe3342ac0be", "locator": "schedule 1, paragraph 4(2)", "similarity": 1.0, "proposition_ids": ["prop:b356d3ac66798bde", "prop:d38715b77f21c0d9"], "classification": "true_duplicate"}

### Top quality concerns

- `npp_reg2_missing`: {'kind': 'npp_reg2_missing', 'severity': 'error'}
- `validation_warnings`: {'kind': 'validation_warnings', 'count': 34}
- `unknown_classifications`: {'kind': 'unknown_classifications', 'count': 4}
- `weak_compliance`: {'kind': 'weak_compliance', 'count': 12}
- `scope_missing_territory`: {'kind': 'scope_missing_territory', 'count': 29}

### Recommended priority fixes

1. Fix JSON extraction repair for quoted evidence_text; targeted re-extract NPP regulation:2
1. Deduplicate export proposition ids (merge chunk-level duplicates)
1. Tune evidence matcher for table rows / multi-row chunks
1. Add classifier rules for table numeric rows → obligation + substantive_rule
1. Territory inheritance pass for application_scope rows
1. Harden parser against JSON+prose responses (Wales schedule:2 pattern)

## Comparison with baseline export

Baseline: `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/slurry-gb-principal-5-frontier-export`
Overall: **mixed**

| Metric | Current | Baseline |
| --- | ---: | ---: |
| Propositions | 729 | 678 |
| Duplicate proposition ids | 2 | 1 |
| Exact-text duplicate groups | 26 | 23 |
| Validation-warning props | 34 | 0 |
| NPP reg:2 fragment props | 0 | 4 |

**Improved:** proposition_count +51
**Worsened:** duplicate_proposition_id_count increased; validation_warning_propositions +34; NPP regulation:2 definitions lost (parse failure on fresh extract)

## A. Duplicate / near-duplicate analysis

### A1 Exact duplicate text (26 groups)

- size 3: prop:685bc229214d17bc, prop:470801395695c2c9, prop:7960444e0199ecaa
- size 3: prop:250150bb206cc92b, prop:f6f8be8fcf1a577b, prop:ec14f085fd01980e
- size 2: prop:20a27393e7f92927, prop:e71d78a2e684d8a4
- size 2: prop:f409dd01c71b61e2, prop:935082a95ecddfcb
- size 2: prop:5ceee9e4aa59fe13, prop:f5b281508d1421c4
- size 2: prop:146a2dd2e3b9db01, prop:28c134de2f220dd0
- size 2: prop:77d0adff16f2cc64, prop:09422ecd6db12586
- size 2: prop:834ba32e0027dae3, prop:7fb999700c947785
- size 2: prop:2577e887efae5056, prop:3fa8cedccc63d4c2
- size 2: prop:05a7910d108391a7, prop:13fdfae0b11b9bd0
- size 2: prop:cc11378087e77dc8, prop:a7fa9f6a08390845
- size 2: prop:2b9bae63bc7ba411, prop:b356d3ac66798bde
- size 2: prop:8c85fcda14db84c1, prop:b8f95db58685b181
- size 2: prop:05362091a7804e50, prop:9d49e3573ab242f7
- size 2: prop:f454d852d0948408, prop:b6d0c144d814cf8f
- size 2: prop:684d16e69acf0ea0, prop:006786bade51b875
- size 2: prop:fd51d268b0425b47, prop:71a42a266674a6b0
- size 2: prop:f393b4791f9b3b59, prop:b0218b86e5f8d448
- size 2: prop:a1d5f7b37ee9e1e6, prop:0456d6d077e378ec
- size 2: prop:29d9108a045f18a4, prop:4f2f696615a11489

### A2 Same source + locator + similar text
- `lex-96613ffe71589e1e` `schedule 3, paragraph 5` sim=0.8951 semantic_comparison_candidate: ['prop:e36aa51a79aec666', 'prop:ed698b2db76fdfec']
- `lex-120b4f9c395b3f94` `schedule 2, part 1, standard table, slurry section` sim=0.9118 semantic_comparison_candidate: ['prop:2680ce4f2bd6a332', 'prop:3b5176863e91808d']
- `lex-120b4f9c395b3f94` `schedule 3, paragraph 8` sim=0.9583 semantic_comparison_candidate: ['prop:3e7dc9a339120fdf', 'prop:83881cdd8b3db8ed']
- `lex-120b4f9c395b3f94` `schedule 3, paragraph 9(2)` sim=0.9681 semantic_comparison_candidate: ['prop:10f2c1f756b5a45d', 'prop:9083d3605b80689b']
- `lex-120b4f9c395b3f94` `schedule 4, paragraph 1` sim=1.0 true_duplicate: ['prop:146a2dd2e3b9db01', 'prop:28c134de2f220dd0']
- `lex-120b4f9c395b3f94` `schedule 4, paragraph 2(a)` sim=0.9797 semantic_comparison_candidate: ['prop:1b4415cb64c9235e', 'prop:d8734c618cea0178']
- `lex-805b03f284dcf364` `regulation 21(a)` sim=0.9073 semantic_comparison_candidate: ['prop:20ce3268138d175b', 'prop:45b5635ded8b098e']
- `lex-805b03f284dcf364` `schedule 5, paragraph 4(2)` sim=1.0 true_duplicate: ['prop:2b9bae63bc7ba411', 'prop:6c278ed82d94a37d']
- `lex-805b03f284dcf364` `schedule 5, paragraph 6` sim=0.9688 semantic_comparison_candidate: ['prop:05362091a7804e50', 'prop:f454d852d0948408']
- `lex-805b03f284dcf364` `schedule 6, paragraph 7` sim=0.9184 semantic_comparison_candidate: ['prop:c5241a2fff1d3d93', 'prop:f3ea23383f796107']
- `lex-96613ffe71589e1e` `schedule 1, paragraph 3(1)(a)` sim=0.9582 semantic_comparison_candidate: ['prop:3142197884f968bb', 'prop:c0f15aead9018c1e']
- `lex-96613ffe71589e1e` `schedule 1, paragraph 3(1)(b)` sim=0.9522 semantic_comparison_candidate: ['prop:1fefa9d08f91a8f6', 'prop:ee13910e2b281f40']
- `lex-96613ffe71589e1e` `schedule 1, paragraph 6` sim=0.9062 semantic_comparison_candidate: ['prop:44cc47af754a93d9', 'prop:d55220a81f7b8367']
- `lex-96613ffe71589e1e` `schedule 2, paragraph 7` sim=0.9677 semantic_comparison_candidate: ['prop:0d5119616daa4a15', 'prop:9a7f091b833bd1f8']
- `lex-96613ffe71589e1e` `schedule 3, paragraph 7` sim=0.9697 semantic_comparison_candidate: ['prop:20385f7e92cb0d83', 'prop:b5587f498d6f7ab1']
- `lex-e71fbbe3342ac0be` `schedule 1, paragraph 4(2)` sim=1.0 true_duplicate: ['prop:b356d3ac66798bde', 'prop:d38715b77f21c0d9']
- `lex-e71fbbe3342ac0be` `schedule 1, paragraph 6` sim=0.9688 semantic_comparison_candidate: ['prop:9d49e3573ab242f7', 'prop:b6d0c144d814cf8f']
- `lex-e71fbbe3342ac0be` `schedule 2, paragraph 7` sim=0.9184 semantic_comparison_candidate: ['prop:e17e6de7298738f5', 'prop:f3411c36900d7010']
- `lex-e71fbbe3342ac0be` `schedule 3, paragraph 7` sim=0.9471 semantic_comparison_candidate: ['prop:262cf13629121ada', 'prop:48e10b42da0fb4c1']

### A3 Duplicate proposition ids
- **prop:a0c4c917216ec053** ×2 frags=['frag-lex-96613ffe71589e1e-035', 'frag-lex-96613ffe71589e1e-038']
- **prop:f80da0f6eb5b0560** ×2 frags=['frag-lex-805b03f284dcf364-053', 'frag-lex-805b03f284dcf364-056']

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
- `lex-72aa053283580d:unknown:is` (key_too_coarse, size 2)
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
- size 2: `definition:construction`
- size 2: `definition:organic_manure`
- size 2: `definition:organic_manure_with_high_readily_available_nitrogen`
- size 2: `definition:relevant_map`

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
- `prop:d6e668b72bea0040` regulation 14(3), table row 1, column 3: Cattle slurry available nitrogen percentage (post-2014) — classify table numeric standard values as obligation + substantive_rule (not unknown)
- `prop:94b1f3f8c5159534` regulation 14(3), table row 2, column 3: Pig slurry available nitrogen percentage (post-2014) — classify table numeric standard values as obligation + substantive_rule (not unknown)
- `prop:3bce0c473e8e7caf` Schedule 1, Non-grazing livestock table, Poultry – Chicken used for producing eggs for human consumption – from 17 weeks (not caged): Daily manure, nitrogen and phosphate production by non-caged — classify livestock manure production table rows as obligation + substantive_rule
- `prop:69fa12ff59c93a00` regulation 30(4)(a)-(b): Compliance period for regulation 30 notice — classify procedural notice scope as procedural_rule + notification or application_scope
