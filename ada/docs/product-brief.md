# Ada product brief

## What Ada is

Ada is a **standalone** legal source discovery workbench. It lives in its own repository and has **no dependency on Judit or Beatrice code**.

Given a **category brief** — label, description, optional synonyms, exclusions, and jurisdiction hints — Ada finds **candidate** UK legal instruments and interpretive materials, explains why they may be relevant, and helps a human curate a **source register** for downstream use.

Ada outputs a reviewable register. It does **not** guarantee complete legal coverage.

## Product boundaries

| Product | Responsibility |
|---------|----------------|
| **Ada** | Find candidate legal sources; score and explain possible relevance; support human curation; export register and Judit handoff JSON |
| **Judit** | Turn **accepted** Ada sources into source-backed legal propositions |
| **Beatrice** | Check guidance against Judit propositions |

```
Category brief → Ada (discovery + curation) → Source register
                                                    ↓
                                    Selected sources (JSON handoff)
                                                    ↓
                                         Judit (propositions)
                                                    ↓
                                    Beatrice (guidance vs law)
```

## What Ada does

- Accept a category brief as structured input
- Generate a deterministic query plan from synonyms and exclusions
- Search Lex API / Lex Graph as the first candidate discovery substrate
- Normalise candidate sources into a common shape
- Apply simple deterministic scoring; optionally enrich with Pydantic AI
- Record human review status per entry
- Export a JSON source register
- Export selected sources for Judit via a versioned JSON contract

## What Ada does not do

- Guarantee completeness of legal coverage
- Draw final legal conclusions or provide legal advice
- Extract legal propositions (Judit)
- Compare guidance against law (Beatrice)
- Persist data in a database (V1)
- Provide a web UI (V1)

## AI usage

When configured, Ada may use **Pydantic AI** for:

- Category expansion (additional synonyms, search terms)
- Candidate relevance assessment (structured rationale)

All AI calls route through a **LiteLLM OpenAI-compatible proxy**. Deterministic commands work without any AI configuration.

## Primary artefacts

| Artefact | Description |
|----------|-------------|
| Category brief | Input definition for a discovery topic |
| Query plan | Deterministic search plan derived from the brief |
| Discovery run | Candidates returned from Lex plus scores |
| Source register | Curated, reviewable list of sources |
| Judit handoff | JSON export of accepted sources only |

See [v1-process.md](v1-process.md) for the V1 workflow and [source-register-schema.md](source-register-schema.md) for output shapes.

## Users

- Legal researchers scoping sources for a policy category
- Analysts reviewing and accepting/rejecting candidates
- Engineers integrating Ada into discovery workflows ahead of Judit ingestion
