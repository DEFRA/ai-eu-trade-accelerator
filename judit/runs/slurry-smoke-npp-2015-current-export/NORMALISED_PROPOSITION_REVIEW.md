# Normalised proposition review

Export: `/Users/bram/Code/defra/ai-eu-trade-accelerator/judit/runs/slurry-smoke-npp-2015-current-export`
Propositions reviewed: **231**

_Deterministic report for human review. Semantic comparison buckets are hints only — not automatic legal links._

## 1. Unknown classifications

Count: **0** — `proposition_tier` or `legal_effect_type` is missing, blank, or `unknown`.

_None._

## 2. Legacy category conflicts

Count: **0** — legacy `categories` contains `obligation` but `is_compliance_relevant` is `false`.

_None._

## 3. Scope / application rows

Count: **7** — `legal_effect_type` is `application_scope`.

| ID | Locator | Source | Label | Territory | Affected subjects |
| --- | --- | --- | --- | --- | --- |
| prop:4d933b91dd01103d | regulation 1(3) | The Nitrate Pollution Prevention Regulations 2015 | Application to England | England | England only |
| prop:de935c0fcdf5d5a7 | regulation 1(4) | The Nitrate Pollution Prevention Regulations 2015 | Application to nitrate vulnerable zones |  | holdings that are in nitrate vulnerable zones designated for the purposes of these Regulations |
| prop:204ed1287e94d38f | regulation 3(1) | The Nitrate Pollution Prevention Regulations 2015 | Designation of nitrate vulnerable zones |  | for the purposes of these Regulations |
| prop:ade5560b077db2e2 | regulation 5A | The Nitrate Pollution Prevention Regulations 2015 | Transitional exemption for new holdings from Schedule 4 requirements |  | a new holding, a new holding until the dates set out in that Schedule |
| prop:8c7cfdac35bdb33c | Schedule 3, paragraph 14(1) | The Nitrate Pollution Prevention Regulations 2015 | Record expected livestock numbers and categories |  | the expected number of livestock animals to be kept on the holding during the calendar year to which the derogation relates, and the category for each animal by reference to the categories in Schedule 1 |
| prop:146a2dd2e3b9db01 | schedule 4, paragraph 1 | The Nitrate Pollution Prevention Regulations 2015 | Transitional exemption for new holdings in nitrate vulnerable zones |  | regulations 7, 8, 10, 11, 12, 13, 15, 16, 17, 19, 23, 25A, 25B, 26, 27, 28, 29, 30, 31, 32, and 34, a new holding until the beginning of the second calendar year after the year in which the Secretary of State revises or adds to the designation of nitrate vulnerable zones under regulation 4(5) so as to include the new holding |
| prop:28c134de2f220dd0 | Schedule 4, paragraph 1 | The Nitrate Pollution Prevention Regulations 2015 | Transitional exemption for new holdings – first set of regulations |  | until the beginning of the second calendar year after the year of designation revision, a new holding until the beginning of the second calendar year after the year in which the Secretary of State revises or adds to the designation of nitrate vulnerable zones under regulation 4(5) so as to include the new holding |

## 4. Cross-reference rows

Count: **2** — `legal_effect_type` is `cross_reference`.

| ID | Locator | Label | Targets | Text |
| --- | --- | --- | --- | --- |
| prop:639fb399243fffa6 | regulation 12(3) | Regulation 12 subject to regulation 13 and independent of fertilisation plan | regulation 12, regulation 13 | Regulation 12 is subject to regulation 13 and applies irrespective of the figure given in the fertilisation plan. |
| prop:72b085a51cee4288 | regulation 42(3) | Incorporation of Environmental Civil Sanctions Order provisions | regulation 42 | The provisions of the Environmental Civil Sanctions (England) Order 2010 relating to the sanctions referred to in regulation 42(1) apply as if they were provisions of these Regulations. |

## 5. Semantic comparison buckets (review hints)

Count: **17** — buckets with more than one proposition sharing a `semantic_comparison_key`. **Not treated as automatic links.**

### `derogation:does_not_apply_in_relation_to_a_new_holding` (4 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:d8734c618cea0178 | The Nitrate Pollution Prevention Regulations 2015 | schedule 4, paragraph 2(a) | Transitional exemption from slurry spreading restrictions for new holdings |  | derogation |
| prop:fe9d9da567372fae | The Nitrate Pollution Prevention Regulations 2015 | schedule 4, paragraph 2(b) | Transitional exemption from organic manure closed periods for new holdings |  | derogation |
| prop:f829874f6f5358fc | The Nitrate Pollution Prevention Regulations 2015 | schedule 4, paragraph 2(e) | Transitional exemption from slurry separation requirements for new holdings |  | derogation |
| prop:d6023706803362f8 | The Nitrate Pollution Prevention Regulations 2015 | schedule 4, paragraph 2(f) | Transitional exemption from storage capacity requirements for new holdings |  | derogation |

### `prohibition:a_person_must_not_spread` (4 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:7a349b08ba3650ca | The Nitrate Pollution Prevention Regulations 2015 | regulation 17(1), (2), (4) | Prohibition on spreading organic manure within 10 metres of surface water |  | prohibition |
| prop:3cfaaf1a19293d55 | The Nitrate Pollution Prevention Regulations 2015 | regulation 17(6) | Prohibition on spreading organic manure within 50 metres of water sources |  | prohibition |
| prop:587b94efa9e930d6 | The Nitrate Pollution Prevention Regulations 2015 | regulation 20(1) | Prohibition on spreading organic manure with high readily available nitrogen during closed period |  | prohibition |
| prop:ad3f8dbb83f6d96f | The Nitrate Pollution Prevention Regulations 2015 | regulation 22(1) | Prohibition on spreading manufactured nitrogen fertiliser during closed period |  | prohibition |

### `derogation:does_not_apply` (3 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:bbd608a46cce603c | The Nitrate Pollution Prevention Regulations 2015 | regulation 31(3) | Greenhouse exception to record-keeping obligations |  | derogation |
| prop:e4e6f5b3bb6fe563 | The Nitrate Pollution Prevention Regulations 2015 | regulation 31(4)-(5) | Low-intensity farming exception to record-keeping obligations |  | derogation |
| prop:9c8987c8a6c368a3 | The Nitrate Pollution Prevention Regulations 2015 | regulation 9 | Greenhouse exemption from nitrogen fertiliser spreading rules |  | derogation |

### `obligation:fertilisation_plan_must_record` (3 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:bfcf7befede3507c | The Nitrate Pollution Prevention Regulations 2015 | Schedule 3, paragraph 4(2)(a) | Fertilisation plan: phosphorus amount and calculation method |  | obligation |
| prop:e0d780f0c24d8020 | The Nitrate Pollution Prevention Regulations 2015 | Schedule 3, paragraph 4(2)(b) | Fertilisation plan: optimum phosphate fertiliser amount |  | obligation |
| prop:c568937225ca641b | The Nitrate Pollution Prevention Regulations 2015 | Schedule 3, paragraph 4(2)(c) | Fertilisation plan: nitrogen from organic manure |  | obligation |

### `obligation:occupier_must_calculate` (3 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:0b25dc61842f0b28 | The Nitrate Pollution Prevention Regulations 2015 | regulation 27(2) | Nitrogen calculation obligation |  | obligation |
| prop:aa312863145a968f | The Nitrate Pollution Prevention Regulations 2015 | schedule 3, paragraph 3(1)(a) | Calculation of available soil phosphorus |  | obligation |
| prop:0e1f1ce5e8538eea | The Nitrate Pollution Prevention Regulations 2015 | schedule 3, paragraph 3(1)(b) | Calculation of optimum phosphate fertiliser amount |  | obligation |

### `appeal:may_be_brought_on_the_ground_that` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:6456dff152f40fd2 | The Nitrate Pollution Prevention Regulations 2015 | regulation 6(2)(a) and 6(2)(aa) | Ground of appeal: holding does not drain into polluted water |  | appeal |
| prop:5e494cb1e7bad37c | The Nitrate Pollution Prevention Regulations 2015 | regulation 6(2)(b) | Ground of appeal: water should not be identified as polluted |  | appeal |

### `application_scope:a_new_holding_until_the_beginning_of_the_second_calendar` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:28c134de2f220dd0 | The Nitrate Pollution Prevention Regulations 2015 | Schedule 4, paragraph 1 | Transitional exemption for new holdings – first set of regulations |  | application_scope |
| prop:146a2dd2e3b9db01 | The Nitrate Pollution Prevention Regulations 2015 | schedule 4, paragraph 1 | Transitional exemption for new holdings in nitrate vulnerable zones |  | application_scope |

### `definition:relevant_map` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:5e76402e434f88c4 | The Nitrate Pollution Prevention Regulations 2015 | regulation 3(2)(a) | Definition of relevant map (initial period) |  | definition |
| prop:7427842461f5ddd6 | The Nitrate Pollution Prevention Regulations 2015 | regulation 3(2)(b) | Definition of relevant map (post-review) |  | definition |

### `definition:the_closed_period` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:712dd64f9b3224f4 | The Nitrate Pollution Prevention Regulations 2015 | regulation 22(2)(a) | Closed period definition for grassland |  | definition |
| prop:e269106e8d6ddd0f | The Nitrate Pollution Prevention Regulations 2015 | regulation 22(2)(b) | Closed period definition for tillage land |  | definition |

### `obligation:a_report_under_paragraph_1_must_contain` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:b9f510fe7cf08b8b | The Nitrate Pollution Prevention Regulations 2015 | regulation 40A(2)(a) | Report must contain details of steps to promote good agricultural practice |  | obligation |
| prop:bc5aa3a65c2a00f5 | The Nitrate Pollution Prevention Regulations 2015 | regulation 40A(2)(b) | Report must contain NVZ map with statement of revisions |  | obligation |

### `obligation:accounts_must_record` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:e456d294078c5807 | The Nitrate Pollution Prevention Regulations 2015 | Schedule 3, paragraph 19(3)(a)-(b) | Derogation accounts: agricultural area and crop recording |  | obligation |
| prop:242ea7f5b6e6b29a | The Nitrate Pollution Prevention Regulations 2015 | Schedule 3, paragraph 19(3)(c)-(h) | Derogation accounts: livestock, manure and fertiliser recording |  | obligation |

### `obligation:occupier_must_calculate_and_record` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:744e667b08ba6e92 | The Nitrate Pollution Prevention Regulations 2015 | Schedule 3, paragraph 14(2) | Calculate and record expected nitrogen and phosphate in manure |  | obligation |
| prop:3e58f26dae040b04 | The Nitrate Pollution Prevention Regulations 2015 | Schedule 3, paragraph 15(1)(b) | Calculate and record nitrogen in livestock manure |  | obligation |

### `obligation:occupier_must_carry_out_phosphorus_soil_sampling_and_analysis` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:8822c97a0cd43cfe | The Nitrate Pollution Prevention Regulations 2015 | schedule 3, paragraph 5(3)(a) | Initial phosphorus sampling requirement (75% within 12 months) |  | obligation |
| prop:8e97116bf78c1e44 | The Nitrate Pollution Prevention Regulations 2015 | schedule 3, paragraph 5(3)(b) | Complete phosphorus sampling requirement (100% at next derogation) |  | obligation |

### `obligation:the_agency_must_refuse_the_application` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:43ad9f88358547b5 | The Nitrate Pollution Prevention Regulations 2015 | regulation 37(5) | Mandatory refusal for adverse effect on European sites |  | obligation |
| prop:4ec5f0041248b27a | The Nitrate Pollution Prevention Regulations 2015 | regulation 37(7) | Mandatory refusal for prior breach of derogation conditions |  | obligation |

### `permission:occupier_of_a_holding_who_has_submitted_undertaking_to_o_may_spread_organic_manu` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:43fed9099ef11684 | The Nitrate Pollution Prevention Regulations 2015 | regulation 21(1)(a), 21(2) | Organic holdings: spreading high readily available nitrogen manure on specified crops |  | permission |
| prop:24d1c229990274dc | The Nitrate Pollution Prevention Regulations 2015 | regulation 21(1)(b), 21(2) | Organic holdings: spreading high readily available nitrogen manure on other crops with FACTS advice |  | permission |

### `prohibition:a_person_must_not_spread_nitrogen_fertiliser` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:afc485ae1766e523 | The Nitrate Pollution Prevention Regulations 2015 | regulation 16(2) | Prohibition on spreading where significant surface water risk |  | prohibition |
| prop:c38f7cc0ab5ef3cf | The Nitrate Pollution Prevention Regulations 2015 | regulation 16(3) | Prohibition on spreading in adverse soil conditions |  | prohibition |

### `recordkeeping:occupier_of_a_new_holding_with_livestock_must_calculate_and_record_the_amount_of` (2 propositions)

| ID | Source | Locator | Label | Territory | Effect |
| --- | --- | --- | --- | --- | --- |
| prop:b687e4541874fe0f | The Nitrate Pollution Prevention Regulations 2015 | regulation 25B(a) | Calculation and recording of manure production for new holdings with livestock |  | recordkeeping |
| prop:36aa97b61148b798 | The Nitrate Pollution Prevention Regulations 2015 | regulation 25B(b) | Calculation and recording of required storage capacity for new holdings with livestock |  | recordkeeping |

## 6. Compliance-relevant rows without clear actor/action

Count: **0** — `is_compliance_relevant` is `true` but `legal_subject` or `action` is missing, placeholder, or very short.

_None._

## 7. Longest labels (top 30)

Count: **30**

| ID | Locator | Len | Label |
| --- | --- | --- | --- |
| prop:0254b2b187cc256c | regulation 44(e) | 202 | Revocation of regulations 2 to 25 of Nitrate Pollution Prevention (Amendment) and Water Resources (Control of Pollution) (Silage, Slurry and Agricultural Fuel Oil) (England) (Amendment) Regulations 2013 |
| prop:b54cf081b7bb5e28 | regulation 6(4) | 116 | Effect of successful appeal on ground (b): water de-designation and consequential treatment of all draining holdings |
| prop:319689437c4700cb | regulation 44(c) | 114 | Revocation of regulation 10 of Environmental Civil Sanctions (Miscellaneous Amendments) (England) Regulations 2010 |
| prop:203d17636aa0781f | regulation 36(7) | 108 | Secretary of State must review derogations against environmental criteria and pollution reduction objectives |
| prop:93a87610e7e3595b | regulation 10(1) | 105 | Duty to calculate soil nitrogen, optimum nitrogen amount, and produce fertilisation plan before spreading |
| prop:55ca01e6cb15fac7 | regulation 13(3)(a)–(b) | 104 | Maximum nitrogen limits for high-protein grass: 700 kg (irrigated) or 500 kg (non-irrigated) per hectare |
| prop:c389492be0ad74a3 | regulation 13(1)–(2) | 103 | Derogation for high-protein grass: nitrogen spreading above standard limits permitted with FACTS advice |
| prop:8158e737399aafb0 | regulation 13(5)–(6) | 100 | Post-application duty: occupier must provide autumn soil analyses to FACTS adviser for future advice |
| prop:24d1c229990274dc | regulation 21(1)(b), 21(2) | 99 | Organic holdings: spreading high readily available nitrogen manure on other crops with FACTS advice |
| prop:587b94efa9e930d6 | regulation 20(1) | 97 | Prohibition on spreading organic manure with high readily available nitrogen during closed period |
| prop:2eb37c76a3a69cef | Schedule 1, Non-grazing livestock table, Pigs – sow (including litter up to 7kg) fed on a diet supplemented with synthetic amino acids | 97 | Daily manure, nitrogen and phosphate production by sow with litter (amino acid-supplemented diet) |
| prop:afba52efcf1a034e | schedule 3, paragraph 9(3) | 91 | Prohibition on ploughing grass not on sandy soils after manure spreading (until 16 January) |
| prop:b016a06775fa8b79 | Schedule 1, Non-grazing livestock table, Pigs – sow (including litter up to 7kg) fed on a diet without synthetic amino acids | 90 | Daily manure, nitrogen and phosphate production by sow with litter (non-supplemented diet) |
| prop:3173feb23ed6a5c4 | regulation 29 | 89 | Duty to retain original laboratory report for nitrogen content analysis in organic manure |
| prop:cda733054cc0fd73 | regulation 34(4)(a) | 88 | Duty to calculate and record expected manure production on first introduction of animals |
| prop:e4404fc36ec0cf91 | regulation 34(4)(b) | 87 | Duty to calculate and record required storage capacity on first introduction of animals |
| prop:9083d3605b80689b | schedule 3, paragraph 9(2) | 87 | Prohibition on ploughing grass on sandy soils after manure spreading (until 16 January) |
| prop:36aa97b61148b798 | regulation 25B(b) | 86 | Calculation and recording of required storage capacity for new holdings with livestock |
| prop:43fed9099ef11684 | regulation 21(1)(a), 21(2) | 85 | Organic holdings: spreading high readily available nitrogen manure on specified crops |
| prop:5097e42798c958de | regulation 25B(c) | 85 | Calculation and recording of current storage capacity for new holdings with livestock |
| prop:311b55eea27bef7b | schedule 3, paragraph 17(a)(ii) | 85 | Record phosphate supplied for crop uptake within one week of spreading organic manure |
| prop:b2d013f402dc64e9 | regulation 17(4)(a)–(c) | 84 | Exception for slurry, sewage sludge and anaerobic digestate with precision equipment |
| prop:0c7a3b8e8b4edab9 | regulation 25(4) | 82 | Exception to storage requirement for off-holding disposal or low run-off spreading |
| prop:bd3d6c6fe53c5c61 | regulation 19(4) | 80 | Incorporation timing for other organic manure near surface water on sloping land |
| prop:722c6c67ed4026f4 | schedule 3, paragraph 2(2), definition of 'Nngl' | 80 | Definition of 'Nngl' (nitrogen from non-grazing livestock) in derogation formula |
| prop:469ac509d66ad0e4 | schedule 3, paragraph 9(1) | 80 | Prohibition on ploughing temporary grassland on sandy soils (1 July–31 December) |
| prop:47ea87a0fa084f10 | regulation 27(3) | 79 | Alternative nitrogen calculation methods for permanently housed pigs or poultry |
| prop:8c96adad51bf8ec0 | schedule 3, paragraph 6(2)(a) | 79 | Pre-spreading recording obligation for manufactured phosphate fertiliser amount |
| prop:f1f1f05063630971 | schedule 3, paragraph 6(2)(b) | 79 | Pre-spreading recording obligation for manufactured phosphate fertiliser timing |
| prop:ad3f8dbb83f6d96f | regulation 22(1) | 78 | Prohibition on spreading manufactured nitrogen fertiliser during closed period |

## 8. Shortest / generic labels (top 30)

Count: **30**

| ID | Locator | Len | Label |
| --- | --- | --- | --- |
| prop:2d3577630a9c6c03 | regulation 2(1), definition of 'slurry' | 20 | Definition of slurry |
| prop:4d933b91dd01103d | regulation 1(3) | 22 | Application to England |
| prop:f5d7f8197260d14b | regulation 39(3) | 22 | Factors for NVZ review |
| prop:c6c82abaf0fe9568 | regulation 2(1), definition of 'spreading' | 23 | Definition of spreading |
| prop:10ade765359fecdc | regulation 38(6) | 24 | Basis for panel decision |
| prop:2ad380ca2d9206e9 | regulation 1(2) | 26 | Commencement on 1 May 2015 |
| prop:3b469740fa37e251 | regulation 15(3) | 26 | Risk map update obligation |
| prop:2864db431b25b706 | regulation 40(1) | 26 | Duty to review Regulations |
| prop:5ceee9e4aa59fe13 | regulation 43 | 27 | Agency enforcement function |
| prop:613ad9fb092eac7f | regulation 5(4) | 27 | Notice content requirements |
| prop:8560e7285cdf5fa7 | regulation 2(1), definition of 'organic manure' | 28 | Definition of organic manure |
| prop:59fd32a5940f49b5 | schedule 3, paragraph 7(1)(c) | 29 | Risk map: completion deadline |
| prop:210f93b2ec28ba59 | regulation 15(4) | 30 | Risk map retention requirement |
| prop:c0cc12c2dfea98f5 | Schedule 2, paragraph 2, subparagraph 1 | 30 | Solid manure sampling location |
| prop:0b25dc61842f0b28 | regulation 27(2) | 31 | Nitrogen calculation obligation |
| prop:5b80071f2f7ccf0b | regulation 3(3) | 31 | Offline access to relevant maps |
| prop:ed8c5c44e861947d | regulation 38(2) | 32 | Time limit for submitting appeal |
| prop:0bf4fa7851fa723a | regulation 38(8) | 32 | Consequence of successful appeal |
| prop:90e8197b468b5efa | regulation 7(3) | 32 | Nitrogen calculation methodology |
| prop:c58fd4b932b03905 | regulation 8(11) | 33 | Definition of 'certified' compost |
| prop:98267568c1171060 | schedule 3, paragraph 7(1)(a) | 33 | Risk map: field reference marking |
| prop:c6cd533b8458e707 | regulation 39(4)-(5) | 34 | Public participation in NVZ review |
| prop:495cdf6289028ee7 | regulation 12(1) | 35 | Nitrogen application limit per crop |
| prop:8163c007990580ae | regulation 16(1) | 35 | Pre-spreading field inspection duty |
| prop:ee7bf7fa5daed41d | regulation 25(8) | 35 | Definition of low run-off risk land |
| prop:7229173a15e4f2d0 | regulation 24 | 36 | Slurry separation method requirement |
| prop:92271df9fa02c9a4 | Schedule 2, paragraph 2, subparagraph 3 | 37 | Solid manure sample depth requirement |
| prop:20b02ae4c3ac2ebd | Schedule 3, paragraph 15(2) | 37 | Deadline for livestock manure records |
| prop:f25adcaea658efa6 | schedule 3, paragraph 7(2) | 37 | Risk map: update obligation on change |
| prop:accc524f2643afb7 | regulation 12(2) | 38 | Definition of total amount of nitrogen |
