# Baseline prompt-lab failure analysis (Prompt 34b)

Frontier baseline runs under `runs/prompt-lab/` — classification and recommended next actions.
**No extraction prompt changes were made in this pass.**

## Summary

| Fixture | Pre-fix eval | Root cause class | Fix applied |
|---------|--------------|------------------|-------------|
| `slurry-bad-diffuse-2018-reg-1-boilerplate` | PASS 4/4 | — | None needed |
| `slurry-good-simple-prohibition-spread-buffer` | FAIL 0/1 | workbench parse/capture + JSON repair gap | `_parse_json` repair fallback + workbench salvage |
| `slurry-bad-unless-except-organic-manure-250kg` | FAIL 1/2 | fixture expectation issue (+ minor over-compression) | Analysis only — fixture update pending |
| `slurry-ugly-schedule-livestock-manure-table` | FAIL 0/1 | post-extraction validation / evidence matching | Table-aware numeric token matching |

---

## 1. `slurry-good-simple-prohibition-spread-buffer`

**Classification:** workbench parse/capture issue (downstream of JSON parse failure, not LLM extraction failure)

| Metric | Value |
|--------|-------|
| Expected count | 1 |
| Raw model row count | 8 |
| Parsed extraction row count (pre-fix) | 0 |
| Raw proposition count (pre-fix) | 0 |
| Normalised proposition count (pre-fix) | 0 |
| Eval actual count (pre-fix) | 0 |

**Reason for mismatch**

- `raw_model_output.txt` contains 8 valid-looking proposition objects including the expected prohibition at regulation 17(1).
- Live extraction failed at JSON parse: row 6 `evidence_text` for `"anaerobic digestate"` used unescaped interior double quotes (`""anaerobic digestate" means…"`), causing `Expecting ',' delimiter` at line 103.
- `extraction_trace.json` records `failure_type: non_json_response`, `validated_row_count: 0`.
- Existing `repair_extraction_json_text` / `fix_unescaped_quotes` could salvage the payload, but `_parse_json` did not invoke repair; workbench wrote empty `parsed_extraction.json` / proposition files despite non-empty raw capture.

**Recommended next action**

- ✅ Wire JSON repair into `_parse_json` and salvage empty validated rows from saved raw output in workbench.
- Re-run dry workbench on saved `raw_model_output.txt` (no frontier re-call).
- Re-eval; expect prohibition match. Extra rows (exceptions, definitions) are legally valid — fixture expects only the headline prohibition, not a strict count cap.
- Prompt tuning **not required** for this failure mode.

---

## 2. `slurry-bad-unless-except-organic-manure-250kg`

**Classification:** fixture expectation issue (+ legal design ambiguity on granularity)

| Metric | Value |
|--------|-------|
| Expected count | 2 |
| Raw model row count | 8 |
| Parsed extraction row count | 8 |
| Raw proposition count | 8 |
| Normalised proposition count | 8 |
| Eval actual count | 8 |
| Matched expected (pre-fix) | 1/2 |

**Reason for mismatch**

- Model correctly extracted the core 250 kg obligation (expected[0] matched).
- Expected[1] — single permission to exceed the limit if three compost conditions are met — was split into separate propositions for Condition 1, 2(a), 2(b), and 3. Eval treats these as extras and cannot match the compressed permission row.
- Paragraph 8(7)(a)–(d) low-intensity grassland requirements were merged into one proposition (index 6); eval flags **over-compression** (4 modal verbs).
- Additional rows (reg 8(6) derogation, reg 8(10) greenhouse carve-out) are legally useful but not in the 2-row fixture.

**Gold standard decision (for future fixture update — not implemented yet)**

| Question | Recommended gold standard |
|----------|---------------------------|
| Split 8(7)(a)–(d)? | **Yes** — four checkable substantive propositions (80% grass, 100 kg N organic cap, 90 kg manufactured N cap, no imported organic manure). |
| Condition thresholds as separate propositions? | **Yes** — Conditions 1–3 and 2(a)/2(b) should be separate checkable rows when within cap; the permission in reg 8(2) may remain one row cross-referencing them. |
| Permission to exceed 250 kg | **One permission row** (reg 8(2)) **plus** separate condition rows (regs 8(3)–(5)) — not one merged row, not condition-only rows without the permission anchor. |
| Target expected count | **~8–10** substantive rows (obligation + permission + 3 conditions + 4× para 7 + derogation + greenhouse exclusion), excluding definitional reg 8(11) terms unless testing definitions. |

**Recommended next action**

- Update fixture `expected_propositions` to reflect granular gold standard above.
- Optionally add eval note that reg 8(11) defined terms are out of scope unless explicitly tested.
- Prompt tuning **not required** — model output is largely **better** than the 2-row fixture.

---

## 3. `slurry-ugly-schedule-livestock-manure-table`

**Classification:** post-extraction validation / evidence matching issue

| Metric | Value |
|--------|-------|
| Expected count | 1 (loose — schedule overview) |
| Raw model row count | 8 |
| Parsed extraction row count (pre-fix) | 2 |
| Raw proposition count (pre-fix) | 2 |
| Normalised proposition count (pre-fix) | 2 |
| Eval actual count (pre-fix) | 2 |
| Matched expected (pre-fix) | 0/1 |

**Reason for mismatch**

- Model returned 8 table/footnote rows; JSON parsed successfully.
- Six table rows dropped in `_validate_v2_items` with `evidence_text not traceable to source`.
- Flattened Schedule 1 source concatenates row labels and numeric columns without spaces (e.g. `9000 litres64315142` vs evidence `9000 litres 64 315 142`).
- Footnote rows matched verbatim; table rows did not.

**Recommended next action**

- ✅ Add `table_numeric_token_match` strategy (label token subsequence + numeric column match / concatenated digit match).
- Mark salvaged table rows `context_dependent` with trace warning when matched via table strategy.
- Re-run dry workbench on saved raw output; re-eval.
- Consider updating fixture to expect **representative table rows + footnotes** rather than a single vague “Schedule 1 sets out…” row — the model’s per-category rows are more checkable.
- Prompt tuning **not required** for evidence matching failures.

---

## 4. Passing control: `slurry-bad-diffuse-2018-reg-1-boilerplate`

PASS 4/4 — no action. Confirms eval + workbench path works for non-table, non-JSON-repair cases.

---

## Prompt tuning recommendation

**Defer prompt tuning.** Failures were dominated by:

1. Deterministic JSON repair not wired into the live parse path.
2. Table evidence normalisation too strict for flattened schedule text.
3. Under-specified fixture expectations for unless/except/condition fragments.

Re-score after infrastructure fixes before changing extraction prompts.

---

## After-fix re-score (dry re-parse of saved raw output, no frontier re-call)

| Fixture | parsed / raw / normalised (before → after) | matched expected (before → after) | eval pass (before → after) |
|---------|---------------------------------------------|-------------------------------------|----------------------------|
| good-prohibition | 0 / 0 / 0 → **8 / 8 / 8** | 0/1 → **1/1** | FAIL → FAIL (non-strict count: 8 vs 1 expected) |
| unless-except | 8 / 8 / 8 (unchanged) | 1/2 → 1/2 | FAIL → FAIL |
| livestock-table | 2 / 2 / 2 → **8 / 8 / 8** | 0/1 → 0/1 | FAIL → FAIL (fixture expects overview row; model outputs per-category rows) |

Infrastructure fixes are working. Remaining eval failures are **fixture/eval policy** (count semantics, gold-standard granularity), not extraction quality.
