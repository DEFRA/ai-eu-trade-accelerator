# Effective law layers (propositions → relationships → guidance statements)

Judit exports four distinct layers for legal content. Each layer has a different job; do not collapse them in UI or Beatrice handoff.

## A. Atomic propositions (`propositions.json`)

Source-faithful extracted statements from instrument fragments.

- One row per extracted proposition.
- Text and locators reflect what the extractor saw in the fragment.
- Cross-reference rows may be incomplete or context-dependent by design.
- **Do not** rewrite these into self-contained “laws” at export time.

## B. Proposition relationships (`proposition-relationships.json`)

Deterministic, reviewable edges between propositions and locators.

- Produced at export from `explicit_cross_reference_targets` (and related fields).
- Edge types include `text_references_locator` and `locator_resolves_to`.
- Same-source resolution only; ambiguous or missing targets stay explicit.
- No LLM calls; stable edge IDs for diffing and review.

Relationship / cross-reference propositions are **legal wiring instructions**: they say how provisions connect, not what the connected rule says. They should be used to resolve and enrich host or imported rules, not matched directly against GOV.UK guidance as primary law.

## C. Effective law statements (`effective_law_statements.json`)

Guidance-facing composed law bundles derived from propositions and relationships.

- `presentation_role` — whether a row is a guidance matching candidate, context connector, supporting definition, etc.
- `standalone_status` — whether the statement can stand alone for matching or needs host/imported context.
- `required_context` — locators and resolution status for unresolved or incorporated material.
- Conservative by default: prefer `context_dependent` or `relationship_only` over over-composition.

This is the **broad derived law layer** for review and UI. It includes definitions, procedural context, and cross-reference wiring rows.

## D. Beatrice law candidates (`beatrice_law_candidates.json`)

Narrow **matching queue** for Beatrice, derived from effective law statements without mutating propositions or the broad law layer.

- Includes only `presentation_role == "guidance_matching_candidate"`, excluding `relationship_only` and `fragmentary` standalone statuses.
- Excludes direct candidates for context connectors, supporting definitions, procedural/enforcement context, and debug rows.
- `context_dependent` and `partially_resolved` guidance candidates are included with `usable_with_context` status and risk flags when appropriate.
- Context connectors and definitions may still appear in `supporting_proposition_ids`, `required_context`, and `evidence` on included candidates.
- Beatrice should match human-readable guidance claims against **candidates**, not raw `propositions.json`.
- `statement_text` is the canonical derived law statement (do not rewrite for matching).
- `matching_text` is deterministic retrieval/matching text assembled from title, statement, source/locator, territory (when determinable), required context, and connector context — no LLM paraphrase.
- Each candidate carries raw territorial metadata (`jurisdiction`, `source_jurisdiction`, `extent`, `territorial_application` when available) plus display fields `territory_labels` and `jurisdiction_label`. Display labels are derived deterministically from proposition territory fields, source inventory metadata, and conservative citation/title heuristics (for example WSI → Wales, SSI → Scotland). Labels may be omitted when evidence is weak.
- Beatrice should use `territory_labels` / `jurisdiction_label` for retrieval and matching filters, and `matching_text` for candidate retrieval against GOV.UK guidance, but present `statement_text` as the legal statement to users. Provenance and source linkage remain source-based.
- `required_context` distinguishes **internal** locators (`regulation`, `schedule`, `paragraph` within the instrument) from **external** technical references (`BS 5502`, `RB209`, Fertiliser Manual, FACTS). External references use `resolution_status: external_reference` and are not resolved to propositions in Judit (no web fetch). A bare `paragraph 7` parsed from BS wording is not emitted as an unresolved internal locator when it is part of an external standard cite.
- Beatrice may use external references for matching and display; they are not missing internal law. Malformed external cites may still surface `needs_review`.
- **Same-source duplicates** (soft dedupe, Option B): candidates that share one `source_record_id`, the same normalized locator, and the same `normalized_statement_text` are grouped with `dedupe` metadata (`duplicate_group_id`, `is_canonical`, `canonical_candidate_id`). All rows are retained; Beatrice/UI should prefer `dedupe.is_canonical == true` for matching to avoid double-counting the same provision. Canonical selection is stable (best `candidate_status`, then lowest `bcand:` id).
- **Cross-jurisdiction equivalence** is not deduped: identical statement text across UKSI/WSI/SSI sources remains separate candidates for divergence and territory-aware matching.
- Top-level `duplicate_summary` reports how many same-source duplicate groups exist in the export.

### Beatrice / guidance handoff

| Proposition kind | Typical `presentation_role` | Match to guidance? |
| --- | --- | --- |
| Obligation, prohibition, permission | `guidance_matching_candidate` | Yes, when `standalone_status` is `standalone` |
| Definition | `supporting_definition` | Supporting context only |
| Cross-reference / `relationship_reference` | `context_connector` | No — wiring only |
| Citation, commencement, extent | `procedural_or_enforcement_context` | Usually no |

Example: a proposition at `regulation 9(3)` that only states factors under `regulation 9(2)` include those in `regulation 4(2)` is a **context connector**, not a primary standalone law for guidance matching.

## Producer

`judit_pipeline.effective_law.attach_effective_law_artifacts` runs during `export_bundle` (before `export_static_bundle`). It builds proposition relationships, effective law statements, then Beatrice law candidates. Re-exporting a persisted run regenerates all three artefacts without mutating `propositions.json`.
