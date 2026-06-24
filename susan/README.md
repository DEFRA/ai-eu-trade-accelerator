# Susan

Susan reads the full body of a GOV.UK guidance page and decomposes it into
atomic propositions — single, self-contained statements of what the guidance
asserts.

Each proposition carries contextual metadata fields that keep it useful
when lifted out of its surrounding page:

| Field | Purpose |
|---|---|
| `proposition_text` | The atomic, self-contained proposition itself. |
| `subject_area` | The broad domain (e.g. "nitrate vulnerable zones"). |
| `instrument` | The specific named licence, permit, regulation, scheme, form, or online service the obligation arises under. |
| `actor` | The role on which the obligation falls (e.g. "licence holder"). |
| `source_paragraphs` | The verbatim paragraphs or list-items the proposition was drawn from. |
| `regulatory_kind` | A controlled-vocabulary tag for the KIND of regulatory artefact: `statutory_obligation`, `permit_requirement`, `grant_condition`, `code_of_practice`, `procedural_step`, `factual_statement`, or `definition`. |
| `derives_from` | A list of verbatim citations to underlying legislation when the page names a legal basis. Empty when the page does not cite — Susan never guesses. |

## What it bakes in

One candidate configuration, proven over pairwise human comparison:

- Anthropic, `claude-sonnet-4-6`
- Structured adapter (page sections prefixed with their H2 locator)
- A single prompt: the "tightened + metadata" prompt
- Contextual metadata fields per proposition: `subject_area`, `instrument`,
  `actor`, `source_paragraphs`, `regulatory_kind`, `derives_from` (see the
  field table above for what each one carries)
- **No fallback.** JSON parse failures and API errors raise loudly; the
  caller decides what to do. Susan never substitutes a heuristic result for
  a failed extraction.
- **No Ollama, no flat adapter, no alternative prompts.** Other shapes lost
  the pairwise contest and are out of scope for the production package.

## Usage

```bash
uv sync
export ANTHROPIC_API_KEY=sk-...

# Run against a pages.json (the validator-shared shape; see below)
uv run susan run path/to/pages.json path/to/output_dir/
```

`pages.json` is either:

- the validator-shared shape:
  ```json
  {"category": "guidance-pages", "pages": [{"url": "...", "title": "..."}]}
  ```
- or a plain list of `{url, title}` objects.

`output_dir/` ends up containing:

- `output.json` — a list of `{url, meta_data: {title, candidate_id,
  candidate_name, propositions: [...]}}` entries. This is the shape the
  validators score against.
- `MODEL.md` — model name, prompt hash, started_at, n_pages, n_propositions.

## What this isn't

- Not multi-provider. Single API, single model.
- Not wired into any orchestrator. Susan is standalone — wiring is a later,
  separate decision.
- Not a database. JSON in, JSON out.
- Not a workspace member of any monorepo. Standalone `pyproject.toml`.
