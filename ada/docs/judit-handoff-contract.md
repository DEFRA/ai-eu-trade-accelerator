# Judit handoff contract

Ada exports **selected** (human-accepted) source register entries for **Judit** to consume later. This document defines the JSON contract only — Ada has **no runtime dependency** on Judit and is **not part of the Judit monorepo**.

## Purpose

Judit turns accepted Ada sources into source-backed legal propositions. Beatrice compares guidance against those propositions. Ada's job ends at curated discovery and handoff.

## Export rules

- Include **`accepted_sources`** from the register only
- Exclude `parked_sources`, `rejected_sources`, and any non-accepted review status
- Do not imply completeness or legal correctness — handoff is a curated candidate set
- Human review is required before export

## Payload shape

```json
{
  "export_type": "ada_selected_sources_for_judit",
  "export_version": "0.1",
  "category_id": "equine_identification",
  "created_at": "2026-05-26T00:00:00Z",
  "sources": []
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `export_type` | string | yes | Constant: `ada_selected_sources_for_judit` |
| `export_version` | string | yes | Contract version, e.g. `"0.1"` |
| `category_id` | string | yes | Category brief id from Ada |
| `created_at` | ISO-8601 datetime | yes | Export timestamp (UTC recommended) |
| `sources` | JuditSource[] | yes | Accepted sources (may be empty) |

## JuditSource

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_id` | string | yes | Ada source id from the register |
| `title` | string | yes | Source title |
| `citation` | string | no | Citation for Judit display and fetching |
| `source_type` | string | yes | Ada source type enum value |
| `canonical_uri` | string | no | Canonical URI |
| `source_system` | string | yes | Originating system (e.g. `lex`, `gov_uk`) |
| `relationship_to_category` | string | yes | Ada relationship enum |
| `confidence` | string | yes | Ada confidence enum |
| `ada_review_status` | string | yes | Always `"accepted"` in handoff |
| `evidence` | EvidenceSnippet[] | yes | Structured evidence from Ada |

Judit should treat this payload as **input material to analyse**, not as legal conclusions from Ada.

## Versioning

- `export_version` uses `major.minor`
- Breaking changes increment major; Judit should reject unknown major versions
- `export_type` distinguishes this contract from other Ada exports

## Ada CLI

```bash
uv run ada export-for-judit examples/equine-identification.source-register.example.json \
  --output handoff.json
```

## Example

See [examples/selected-sources-for-judit.example.json](../examples/selected-sources-for-judit.example.json).

## Relationship diagram

```
Ada repository (standalone)
    │
    │  JSON file: ada_selected_sources_for_judit
    ▼
Judit (separate product)
    │
    │  propositions
    ▼
Beatrice (separate product)
```
