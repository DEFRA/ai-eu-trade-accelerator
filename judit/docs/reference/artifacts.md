# Artifact map

This page maps key run/export artifacts to producer/consumer surfaces and stability level.

Stability labels:

- `stable/operator-facing`
- `evolving`
- `internal`
- `domain/profile-specific`
- `compatibility`

![Artifact map](../assets/generated/diagrams/artifact-map.svg)

Diagram source: `docs/assets/diagrams/artifact-map.d2` (generated SVG may be absent in a clean checkout; run `just diagrams`).

| Artifact | Purpose | Producer | Consumer | Stability |
| --- | --- | --- | --- | --- |
| `sources.json` | Source records used/produced by run. | `judit_exporters.static_bundle.export_static_bundle` | `OperationalStore` (`/ops/sources`), UI source inspectors | stable/operator-facing |
| `source_fragments.json` | Fragment rows for extraction targeting and traceability. | exporter static bundle | `OperationalStore` (`/ops/source-fragments`, source detail) | stable/operator-facing |
| `propositions.json` | Proposition review dataset for analysis/divergence. | pipeline runner + exporter | proposition explorer, `OperationalStore` | stable/operator-facing |
| `proposition_extraction_traces.json` | Per-proposition extraction provenance/diagnostics. | pipeline runner + exporter | `OperationalStore`, quality/inspection flows | stable/operator-facing |
| `proposition_extraction_jobs.json` | Per-source/fragment extraction job outcomes (selection/fallback/repairability). | pipeline runner + exporter | repair hints, quality metrics, operator inspection | evolving |
| `extraction_llm_call_traces.json` | LLM/chunk call diagnostics and context-window metadata. | extraction flow + exporter | run quality metrics, repairability analysis | evolving |
| `proposition_extraction_failures.json` | Extraction failure rows for fail-closed/repair analysis. | pipeline runner + exporter | repair workflow, metrics/readouts | evolving |
| `run_quality_summary.json` | Aggregate lint/quality gate status and metrics. | `judit_pipeline.run_quality` + exporter | `/ops/run-quality-summary`, proposition explorer status | stable/operator-facing |
| `source_family_candidates.json` | Candidate related instruments discovered/registered around target source. | runner/source family workflows + exporter | operations inspector registry workflows | compatibility |
| `equine_source_coverage.json` | Source coverage matrix for equine corpus run/profile. | `write_equine_coverage_artifacts` | `/ops/corpus-coverage/equine`, equine coverage panel | domain/profile-specific |
| `equine_proposition_coverage.json` | Proposition coverage matrix for equine corpus run/profile. | `write_equine_coverage_artifacts` | `/ops/corpus-coverage/equine`, equine coverage panel | domain/profile-specific |
| `equine_corpus_readiness.json` | Equine corpus readiness summary (review material by default; guidance-ready is promoted after governance). | `write_equine_coverage_artifacts` | `/ops/corpus-coverage/equine` | domain/profile-specific |
| `runs/<run>/traces/*.json` | Stage trace files (`source intake`, extraction, pairing, etc). | exporter `_write_stage_traces` | run trace inspection UI/API | stable/operator-facing |
| `runs/<run>/artifacts/*.json` | Run-scoped artifact payload files indexed by run artifact rows. | exporter `_write_run_artifacts` | `OperationalStore` run artifact lookup | compatibility |
| `runs/_jobs/<job>/job.json` | Async run job status/summary/metrics snapshot. | `RunJobStore` (`pipeline_run_jobs.py`) | `/ops/run-jobs`, progress UI | internal |
| `runs/_jobs/<job>/events.json` | Async run job event timeline and per-stage updates. | `RunJobStore` + `PersistingPipelineProgress` | `/ops/run-jobs/{job_id}/events`, progress UI | internal |
