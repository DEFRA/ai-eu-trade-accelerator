# Judit pipeline overview

This pipeline transforms legal text into structured, reviewable legal propositions.

![Pipeline overview v2](../assets/generated/diagrams/pipeline-overview-v2.svg)

Diagram source: `docs/assets/diagrams/pipeline-overview-v2.d2` (generated SVG may be absent in a clean checkout; run `just diagrams`).

## Summary

Judit processes legal text through a **source-first, proposition-first**, **deterministic-first** pipeline with **controlled model usage**. Language models run only in explicitly defined stages; assisted work is traceable in run artifacts.

At the product level, analysis proceeds **Source → Propositions → Context → Comparison → Review**: ingest and anchor **sources**, extract and inventory **propositions**, use completeness and source categorisation to establish **context**, run **divergence** classification to **compare** aligned propositions, then assemble templated outputs for human **review**.

Stages build on each other: intake and proposition work establish comparable units grounded in sources; middle stages classify structure, completeness, and divergence; the final stage turns structured results into templated narratives suitable for review.

## Pipeline stages

| Stage | What it does | Model usage |
| ----- | ------------ | ----------- |
| Source intake | Fetch, parse, and register sources | No model |
| Proposition extraction | Extract candidate legal propositions | Local model (controlled, cached) |
| Proposition inventory and pairing | Group and align propositions | No model |
| Proposition completeness | Assess whether propositions are self-contained | Deterministic rules |
| Source categorisation | Assign roles and relationships between sources | Deterministic rules |
| Divergence classification | Classify differences between propositions | Optional frontier model (traceable) |
| Narrative output | Generate reports using templates | No model |

## Design principles

- **Deterministic-first pipeline** with **controlled model usage**, per [ADR-0019: Model usage strategy](../decisions/adr-0019-model-usage-strategy.md)
- Source intake, proposition inventory and pairing, proposition completeness, source categorisation, and narrative output are deterministic or template-based
- Proposition extraction may use a local model; divergence reasoning may optionally use a frontier model; both are traceable
- All outputs are source-backed
- Human review is first-class
