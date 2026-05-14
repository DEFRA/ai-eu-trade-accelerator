# Infographic Evidence Pack

*Audit date: 2026-05-12. Read-only analysis of `/Users/chris/defra/ai-eu-trade-accelerator` (git main).*

---

## 1. Project Identity

| Field | Evidence |
|---|---|
| **Repo name** | `ai-eu-trade-accelerator` |
| **Product name** | **Beatrice** |
| **One-sentence summary** | Beatrice automatically checks whether GOV.UK guidance accurately reflects the law, highlighting discrepancies directly on the live page. |
| **Evidence paths** | `beatrice/README.md:1-3`, `beatrice/apps/guidance-explorer/app/page.tsx:449-452` |

**Plain-English explanation (3–5 sentences):**
Beatrice fetches any GOV.UK guidance page and breaks it into discrete legal obligations—things the guidance says importers, operators, or members of the public *must*, *shall*, or *are required to* do. It then compares each obligation against a separately-supplied set of law propositions (extracted from legislation) using two layers of AI: semantic embeddings to find candidate matches, and a large language model to judge whether each guidance statement is confirmed, outdated, incomplete, over-specified, or contradicts the law. Each proposition receives a relationship classification and a 0–1 correctness score. Results are displayed in a web tool and can also be overlaid directly on the live GOV.UK page as colour-coded markers (green / amber / red). A CSV export is also available for reporting.

**Target audience / stakeholders:**
Policy analysts, legal teams, and compliance officers who need to audit whether published government guidance is up-to-date with legislation. The repo is under DEFRA and the script examples reference SPS (Sanitary and Phytosanitary) agreements and agricultural trade, suggesting a post-Brexit EU–UK trade compliance context.

**Current maturity:** Active prototype — all core pipeline stages are implemented and runnable; the domain package contains more elaborate data models than are currently wired up, suggesting planned future expansion.

**What the system is definitely NOT:**
- Not a legal advice service
- Not a document management or publishing system
- Not a batch ingestion pipeline (no database, no scheduler, no persistent store beyond JSON caches in `/tmp`)
- Not a cross-jurisdiction divergence finder (that data model exists in `domain/` but is not wired to any active pipeline)

**Evidence paths:** `beatrice/README.md`, `beatrice/packages/domain/src/beatrice_domain/models.py`, `beatrice/packages/domain/src/beatrice_domain/run_comparison.py`

---

## 2. High-Level System Narrative

### Stage 1 — Source Material (Law Propositions)
**Explanation:** A user or analyst prepares a JSON file of law propositions extracted from legislation (e.g. a UK statutory instrument or retained EU regulation). Each proposition is a structured statement of what the law requires. This file is uploaded once and reused across many guidance checks.

- **Input:** `.json` file of `Proposition` objects (fields: `proposition_text`, `article_reference`, `jurisdiction`, etc.)
- **Output:** Embedded law corpus stored in-memory and cached to `/tmp/beatrice/law-embeddings-cache.json`
- **Relevant paths:** `beatrice/scripts/extract_law_propositions_from_text.py`, `beatrice/packages/domain/src/beatrice_domain/models.py:300-331`, `beatrice/apps/guidance-api/src/beatrice_guidance_api/main.py:439-467`
- **Status:** **Implemented** — `/embed-law` API endpoint fully functional; script for generating law propositions from `.txt` files also present

---

### Stage 2 — Guidance Ingestion
**Explanation:** A GOV.UK guidance URL is submitted. The system calls the live GOV.UK Content API (`https://www.gov.uk/api/content/...`) and parses the HTML response into sections by H2 heading. Supports both single-page guidance (`details.body`) and multi-part guides (`details.parts`). Alternatively, a local `.txt` file (e.g. a Word document export) can be processed for unpublished guidance.

- **Input:** GOV.UK URL (string) or local `.txt` file
- **Output:** List of `(locator, text)` fragment pairs, e.g. `("section:notify-apha-about-imports", "You must notify...")`
- **Relevant paths:** `beatrice/packages/guidance/src/beatrice_guidance/adapter.py`, `beatrice/apps/guidance-api/src/beatrice_guidance_api/main.py:146-159`
- **Status:** **Implemented**

---

### Stage 3 — Proposition Extraction
**Explanation:** Each text fragment is processed to extract discrete legal obligations. Two strategies are tried in order: (1) LLM extraction via Claude Sonnet (preferred) — an LLM reads the guidance text and returns structured JSON of propositions; (2) heuristic extraction (fallback) — regex/trigger-word matching on normative words ("must", "shall", "required", etc.). Results are cached by URL+section+method.

- **Input:** `(locator, text)` fragments + optional LLM client
- **Output:** List of `GuidanceProposition` objects (`id`, `section_locator`, `proposition_text`, `legal_subject`, `action`, `conditions`, `required_documents`, `source_paragraphs`)
- **Relevant paths:** `beatrice/packages/guidance/src/beatrice_guidance/extract.py`, `beatrice/packages/guidance/src/beatrice_guidance/models.py`
- **Status:** **Implemented** — both LLM and heuristic paths functional; extract cache at `/tmp/beatrice/extract-cache.json`

---

### Stage 4 — Semantic Matching
**Explanation:** Both the guidance propositions and the law propositions are converted to vector embeddings using a locally-run `nomic-embed-text:v1.5` model (via Ollama). For each guidance proposition, cosine similarity is computed against all law embeddings to find the top-K nearest candidates above a threshold. Candidates are then re-ranked by BERTScore F1 using a fine-tuned NLI model (`microsoft/deberta-xlarge-mnli`).

- **Input:** `GuidanceProposition` list + cached law embedding index
- **Output:** Per-guidance-proposition list of `MatchEntry` objects with `similarity_score` and `bert_score_f1`
- **Relevant paths:** `beatrice/packages/matching/src/beatrice_matching/embeddings.py`, `beatrice/packages/matching/src/beatrice_matching/bert_score.py`, `beatrice/apps/guidance-api/src/beatrice_guidance_api/main.py:533-570`
- **Status:** **Implemented**

---

### Stage 5 — LLM Classification
**Explanation:** An LLM (Claude Sonnet by default) reads each guidance–law pair together with the similarity score and judges the relationship. Six possible classifications: `confirmed`, `outdated`, `guidance omits detail`, `guidance contains additional detail`, `contradicts`, `does not match`. The LLM also produces a 0–1 `correctness_score` and an explanation. Results are cached by SHA256 of the prompt.

- **Input:** `GuidanceProposition` + matched `Proposition` + scores
- **Output:** `GuidanceMatch` objects with `relationship`, `confidence`, `explanation`, `correctness_score`
- **Relevant paths:** `beatrice/packages/matching/src/beatrice_matching/classify.py`, `beatrice/packages/matching/src/beatrice_matching/models.py`
- **Status:** **Implemented** — with classify cache at `/tmp/beatrice/classify-cache.json`

---

### Stage 6 — Summarisation
**Explanation:** For each guidance proposition that has at least one non-`does not match` classification, an LLM generates a 2–3 sentence plain-English compliance summary explaining how the guidance aligns with or diverges from the relevant law. Focuses on practical implications.

- **Input:** `GuidanceProposition` + classified matches (excluding "does not match")
- **Output:** Free-text compliance summary string (≤100 words), cached at `/tmp/beatrice/summarise-cache.json`
- **Relevant paths:** `beatrice/apps/guidance-api/src/beatrice_guidance_api/main.py:573-617`
- **Status:** **Implemented**

---

### Stage 7 — Outputs
**Explanation:** Three output channels: (a) The **Guidance Explorer** web UI shows proposition cards with match scores, classification badges, correctness bars, and summaries. An overall "page correctness score" (average across classified propositions) is displayed. (b) The **Overlay view** loads the live GOV.UK page in an iframe with injected JavaScript that colour-codes text passages and adds clickable numbered markers; clicking a marker shows a tooltip with classification details. (c) **CSV export** downloads all propositions, summaries, classifications, citations, and scores.

- **Relevant paths:** `beatrice/apps/guidance-explorer/app/page.tsx`, `beatrice/apps/guidance-explorer/app/overlay/page.tsx`, `beatrice/apps/guidance-api/src/beatrice_guidance_api/main.py:359-394`
- **Status:** **Implemented** — all three outputs functional

---

## 3. Core Concepts and Vocabulary

| Canonical term | Simple definition | Where in code | Infographic label |
|---|---|---|---|
| **Proposition** | A single, atomic legal requirement extracted from a document — what someone must/shall/may do | `beatrice_domain/models.py:300`, `beatrice_guidance/models.py:4` | Legal Rule |
| **Guidance Proposition** | A proposition extracted from a GOV.UK guidance page | `beatrice_guidance/models.py:4`, `beatrice_guidance/extract.py` | Guidance Statement |
| **Law Proposition** | A proposition extracted from legislation | `beatrice_domain/models.py:300` (the `Proposition` model) | Legal Requirement |
| **Fragment** | A section-level chunk of text from a guidance page, identified by a section locator | `beatrice_guidance/adapter.py`, `beatrice_domain/models.py:69` | Text Section |
| **Relationship** | The classification of how a guidance statement relates to a law proposition: confirmed / outdated / omits detail / additional detail / contradicts / does not match | `beatrice_matching/models.py:3-10` | Compliance Status |
| **Correctness Score** | A 0–1 score indicating how accurately guidance reflects the law (1=confirmed, 0=contradicts/no match) | `beatrice_matching/models.py:20` | Accuracy Score |
| **Match** / **MatchSet** | The set of candidate law propositions found for a single guidance proposition, with scores | `beatrice_matching/models.py:25` | Law Match |
| **Overlay** | The live GOV.UK page with injected colour-coded markers showing classification results | `beatrice_guidance_api/main.py:162-355`, `overlay/page.tsx` | Live Annotation |
| **Extraction Method** | Either LLM (Claude Sonnet) or heuristic (trigger-word) | `beatrice_guidance/extract.py` | AI / Rule-based |
| **Source Record** | A registered legislative instrument (broader domain model, not active in current UI) | `beatrice_domain/models.py:31` | Legal Source *(planned)* |
| **Divergence** | A difference between two versions of legislation (e.g. UK vs EU) — domain model present but no active pipeline | `beatrice_domain/models.py:385`, `beatrice_domain/enums.py:4` | Law Gap *(planned)* |
| **ComparisonRun** | A recorded execution comparing propositions across source documents | `beatrice_domain/models.py:425` | Analysis Run *(planned)* |

---

## 4. Inputs and Outputs

### Inputs

| Name | Examples | Format | Where it enters | Evidence paths |
|---|---|---|---|---|
| GOV.UK guidance URL | `https://www.gov.uk/guidance/import-food-and-animal-feed-from-the-eu` | URL string | Frontend form → `/extract` API | `main.py:494-530`, `page.tsx:459-464` |
| Local guidance text file | A `.txt` export of a Word document | Plain text file (UTF-8) | CLI script | `scripts/extract_guidance_from_text.py` |
| Law propositions JSON | JSON file containing array of `Proposition` objects | `.json` (array or `{propositions:[...]}`) | Frontend file upload → `/embed-law` API | `main.py:439-467`, `page.tsx:142-183` |
| Local law text file | Plain-text legal document | Plain text | CLI script | `scripts/extract_law_propositions_from_text.py` |

### Outputs

| Name | Examples | Format | Where produced | Who consumes it | Evidence paths |
|---|---|---|---|---|---|
| Guidance Explorer UI | Proposition cards with scores, badges, summaries | Next.js web app (port 3001) | `guidance-explorer` app | Policy analysts, legal teams | `apps/guidance-explorer/app/page.tsx` |
| Page correctness score | `0.74` average across 12 propositions | Float 0–1, displayed as a coloured progress bar | Guidance Explorer UI | Analysts | `page.tsx:596-618` |
| Live GOV.UK Overlay | Green/amber/red markers on the real guidance page | HTML + injected JS via proxied iframe | Overlay page + API `/proxy` | Policy teams, reviews | `overlay/page.tsx`, `main.py:162-384` |
| CSV export | `guidance-classifications.csv` with proposition text, law citation, BERT score, relationship, explanation, correctness score | CSV | Browser download, client-side | Reporting, audit trail | `page.tsx:396-440` |
| Extracted propositions JSON | LLM-extracted list of `GuidanceProposition` objects | JSON (cached at `/tmp/beatrice/extract-cache.json`) | API `/extract` response | Frontend, downstream scripts | `main.py:494-530` |
| Law embeddings cache | `law-embeddings-cache.json` | JSON | `/tmp/beatrice/` | API server on restart | `main.py:68-102` |

---

## 5. Architecture / Pipeline Map

### Architecture Summary

Beatrice is a three-service local application: a Next.js frontend, a FastAPI backend, and a LiteLLM proxy. The backend orchestrates all AI operations via the LiteLLM proxy (which routes to Ollama for embeddings and local models, or Anthropic/OpenAI for frontier models). All results are cached to JSON files in `/tmp/beatrice/`. There is no database.

### Main Components

- **`apps/guidance-explorer`** — Next.js 14 / TypeScript / Tailwind frontend (port 3001). Two pages: `page.tsx` (explorer/analysis UI) and `overlay/page.tsx` (GOV.UK iframe overlay).
- **`apps/guidance-api`** — FastAPI Python backend (port 8011). Exposes REST endpoints: `/extract`, `/embed-law`, `/match`, `/classify`, `/summarise`, `/proxy`, `/content`, `/embedded-laws`, `/health`.
- **`config/litellm.yaml`** — LiteLLM proxy (port 4000) routing model aliases (`claude_sonnet`, `local_embed`, `local_classify`, etc.) to Anthropic, OpenAI, or Ollama.
- **`packages/guidance`** — GOV.UK Content API parser (`adapter.py`) and proposition extractor (`extract.py`).
- **`packages/matching`** — Embeddings (`embeddings.py`), BERTScore (`bert_score.py`), LLM classification (`classify.py`).
- **`packages/llm`** — `BeatriceLLMClient` wrapping the OpenAI SDK against the LiteLLM proxy endpoint.
- **`packages/domain`** — Pydantic data models (`Proposition`, `GuidanceMatch`, `MatchSet`, divergence models, source record models). Many domain models are present but not yet wired to active pipelines.

### Actual Data Flow (Mermaid)

```mermaid
flowchart LR
    subgraph Inputs
        A[GOV.UK URL\nor .txt file]
        B[Law Propositions\nJSON file]
    end

    subgraph guidance-api ["FastAPI: guidance-api (port 8011)"]
        C["/extract\nContent API → parse → LLM/heuristic extract"]
        D["/embed-law\nEmbed law corpus → cache"]
        E["/match\nEmbed guidance → cosine sim → BERTScore re-rank"]
        F["/classify\nLLM: judge relationship per pair"]
        G["/summarise\nLLM: 2–3 sentence compliance summary"]
        H["/proxy\nServe proxied GOV.UK page + inject Beatrice JS"]
    end

    subgraph litellm ["LiteLLM proxy (port 4000)"]
        I[nomic-embed-text\nvia Ollama]
        J[Claude Sonnet\nvia Anthropic API]
    end

    subgraph Outputs
        K[Guidance Explorer UI\nlocalhost:3001]
        L[Live Overlay\ncolour-coded GOV.UK page]
        M[CSV Export\nguidance-classifications.csv]
    end

    A --> C
    B --> D
    C --> E
    D --> E
    E --> F
    F --> G
    C & D & E --> litellm
    G --> K
    G --> L
    F --> M
    H --> L
```

**Evidence paths:** `beatrice/apps/guidance-api/src/beatrice_guidance_api/main.py` (all endpoints), `beatrice/config/litellm.yaml` (model routing), `beatrice/apps/guidance-explorer/app/page.tsx` (frontend flow)

---

## 6. Existing Diagrams / Visuals / Assets

### Search results

- **No image files** found (PNG, SVG, JPG, PDF, D2, Excalidraw): `Glob **/*.{png,svg,jpg,jpeg,pdf,d2}` → no results
- **No slide decks, Figma, or Mermaid source files** found
- **Architecture ASCII diagram** in `beatrice/README.md:16-28` — a simple directory tree listing, not a flow diagram. Current and accurate.
- **No infographic, screenshot, or generated asset directory** found in the repo

### What this means for the infographic

There is no existing visual material to preserve, reuse, or be consistent with. The infographic should be created from scratch. No existing logo, brand colour palette, or design system is present. The UI uses Tailwind's default neutral palette (white, gray borders) with green/amber/red semantic colours for status.

---

## 7. Claims vs Implementation Check

| Claim / topic | Where claimed | Implementation evidence | Status | Notes |
|---|---|---|---|---|
| "Checks whether GOV.UK guidance accurately reflects the law" | `beatrice/README.md:3` | Full pipeline implemented: extract → match → classify → overlay | **accurate** | Core value proposition is real |
| Extract via GOV.UK Content API | `README.md:7` | `adapter.py` calls `https://www.gov.uk/api/content{path}`, `main.py:146-159` | **accurate** | |
| LLM extraction preferred, heuristics as fallback | `README.md:7`, `extract.py:1-9` | `extract_propositions()` tries LLM first, falls back to regex | **accurate** | |
| "Match" step uses cosine similarity then BERTScore | `README.md:8` | `embeddings.py` + `bert_score.py` both implemented | **accurate** | |
| Six relationship types | `README.md:9` | `RELATIONSHIP_TYPES` tuple in `matching/models.py:3-10` | **accurate** | |
| Colour-coded overlay (green/amber/red) | `README.md:11` | `highlightColour()` in injected JS, `overlay/page.tsx` status logic | **accurate** | |
| CSV export | `README.md:12` | `handleExportCsv()` in `page.tsx:396-440` | **accurate** | |
| Default model `claude_sonnet` | `README.md:88`, `.env.example` | `litellm.yaml` maps `claude_sonnet` → `anthropic/claude-sonnet-4-6` | **accurate** | |
| BERTScore model `microsoft/deberta-xlarge-mnli` | `README.md:91`, `.env.example` | Referenced in `bert_score.py` via env var `MODEL_BERT_SCORE` | **accurate** | |
| "Highlights discrepancies on the live GOV.UK page" | `beatrice/README.md:3` | Implemented via `/proxy` endpoint + postMessage JS bridge | **accurate** | Works by proxying real page through the API |
| Domain models suggest cross-jurisdiction divergence analysis | `domain/models.py` — `DivergenceObservation`, `ComparisonRun`, `SourceRecord` etc. | These models exist but no endpoint/script actively uses them in the current Beatrice app | **aspirational** | Significant planned capability, not yet surfaced |
| Source Record / Snapshot / Fragment ingestion pipeline | `domain/models.py:31-113` | Models defined; `SourceRecord` is used as type for law propositions in old alias (`SourceDocument = SourceRecord`) but no fetch/parse pipeline is wired | **aspirational** | Possible future automated ingestion layer |
| `run_comparison.py` RunComparisonSummary | `domain/run_comparison.py` | Model defined; no endpoint produces or consumes it | **aspirational** | |

---

## 8. Recommended Infographic Story

### Recommended title
**Does your guidance match the law?**

### Recommended subtitle
**Beatrice uses AI to automatically check GOV.UK guidance against legislation — finding gaps, outdated rules, and contradictions in seconds.**

### Visual Stages (left-to-right flow)

| # | Stage label | Short copy | Icon / metaphor |
|---|---|---|---|
| 1 | **Upload the Law** | Load a structured set of legal obligations from legislation | Book / scroll with a tick |
| 2 | **Point at Guidance** | Enter any GOV.UK guidance URL — or paste in unpublished draft text | GOV.UK crown logo / browser address bar |
| 3 | **AI Reads & Extracts** | An AI model reads each section and pulls out what operators *must*, *shall*, or *are required to* do | Magnifying glass over paragraph |
| 4 | **Smart Matching** | Each guidance statement is compared against hundreds of legal obligations using semantic similarity and NLI scoring | Two speech bubbles connected by a dotted line / Venn diagram |
| 5 | **LLM Judges the Relationship** | A large language model decides: is this guidance confirmed, outdated, incomplete, or does it contradict the law? | Scales of justice / traffic light |
| 6 | **Colour-coded Results** | Every statement gets a status: 🟢 Confirmed · 🟡 Outdated / Incomplete · 🔴 Contradicts | Three coloured chips |
| 7 | **See it on the Live Page** | Results appear directly on the GOV.UK page as clickable markers — click any marker for the full law citation and explanation | Browser with highlight markers / tooltip |

### Important callouts

- **Correctness Score (0–1):** An overall accuracy rating for the whole guidance page — powerful for executive reporting
- **CSV Export:** Every finding is downloadable for audit trail, policy review, or stakeholder presentation
- **Works on draft guidance too:** Upload a Word document export before anything is published
- **All results cached:** Repeat analysis at zero cost; only new content triggers AI calls

### Things to avoid showing
- The LiteLLM proxy architecture (too technical)
- BERTScore / DeBERTa — too technical for stakeholders
- The domain model hierarchy (SourceRecord / SourceSnapshot etc.) — not active and confusing
- The divergence/comparison models — not yet wired up
- Cosine similarity numbers

### Terminology to avoid
- "embedding", "cosine similarity", "BERTScore", "deberta", "Ollama", "LiteLLM"
- "heuristic extraction" (use "rule-based" only if needed)
- "proposition" on its own — say "legal obligation" or "requirement"
- "locator", "fragment", "snapshot"

---

## 9. Suggested Visual Hierarchy

- **Primary flow:** Left-to-right linear pipeline (7 stages, roughly equal weight)
- **Secondary callout panels:**
  - Bottom-left: "Works on live pages *and* draft documents"
  - Bottom-right: "CSV export for audit & reporting"
  - Centre-top or top band: page correctness score meter (as a graphic element — e.g. a gauge or progress bar)
- **Footer / outcome band:** The three relationship outcomes in coloured chips with short definitions: Green = Confirmed | Amber = Gaps/Outdated | Red = Contradicts
- **Orientation:** Left-to-right (horizontal flow) — matches the natural reading direction and pipeline metaphor
- **Analysis/comparison:** Stage 5 (LLM classification) should be the visual centrepiece — it is the differentiated AI capability
- **Input documents:** Show TWO inputs (law JSON + guidance URL) flowing into the pipeline from the top-left — makes the dual-input model clear
- **Branching outputs:** Show three outputs at the end (Explorer UI card, Live Overlay, CSV) as a small fan-out

---

## 10. Design Cues from Repo

- **No brand colour palette or design system defined** — Tailwind defaults only
- **Semantic traffic-light colours (implemented in code):**
  - Green: `rgba(34,197,94,0.30)` / `bg-green-100 text-green-800 border-green-200` → confirmed
  - Amber: `rgba(251,191,36,0.30)` / `bg-yellow-100` → outdated
  - Red: `rgba(239,68,68,0.25)` / `bg-red-100 text-red-800 border-red-200` → contradicts
  - Blue: `bg-blue-100 text-blue-800 border-blue-200` → guidance omits detail
  - Purple: `bg-purple-100 text-purple-800 border-purple-200` → additional detail
  - Gray: `bg-gray-100 text-gray-700` → does not match
- **Typography:** Standard Tailwind sans-serif; monospace font (`font-mono`) for scores
- **UI style:** Clean, minimal, card-based — no GOV.UK design system (this is an internal tool, not a published service)
- **GOV.UK crown:** Referenced in CSS class `gov-content` for rendered HTML. The crown/GOV.UK branding is the *subject* of the tool, not the tool's own brand.
- **Tone:** Functional, professional, enterprise — not playful. Suitable for civil service/policy audience.
- **No existing logo or icon** for Beatrice itself.

Evidence: `beatrice/apps/guidance-explorer/app/globals.css`, `beatrice/apps/guidance-api/src/beatrice_guidance_api/main.py:200-335`

---

## 11. Stakeholder Copy Block

**Title:**
> Does your guidance match the law?

**Subtitle:**
> Beatrice automatically checks GOV.UK guidance against legislation — surfacing gaps, outdated rules, and contradictions with AI.

**Stage labels and one-line captions:**

| Stage | Label | Caption |
|---|---|---|
| 1 | Upload the Law | Load structured legal obligations from any act, regulation, or statutory instrument |
| 2 | Point at Guidance | Enter any GOV.UK URL — or upload a draft document before it's published |
| 3 | Extract Requirements | AI reads every section and identifies what the guidance says people must do |
| 4 | Find the Best Matches | Each guidance statement is automatically matched to the most relevant legal obligations |
| 5 | AI Judges the Gap | A large language model decides whether the guidance confirms, understates, overstates, or contradicts the law |
| 6 | Instant Colour-Coded Results | Every statement is flagged Green, Amber, or Red — with a plain-English explanation |
| 7 | See it on the Live Page | Results appear directly on the GOV.UK page as clickable markers — or export to CSV for your audit trail |

**Benefit chips (3–5):**

- Audit guidance compliance in minutes, not weeks
- Works on live pages *and* unpublished drafts
- Every finding backed by the specific law it references
- An overall accuracy score for the whole page at a glance
- Exportable CSV for reporting and governance

**Core principles:**

1. **Evidence-based** — every classification cites the specific legal provision it draws from
2. **Transparent** — AI provides plain-English explanations alongside every decision
3. **Actionable** — results appear directly on the guidance page, making gaps impossible to miss

**Footer sentence:**
> Built by DEFRA to keep government guidance aligned with the law — automatically.

---

## 12. Regeneration / Repo Integration Recommendation

- **Proposed source brief path:** `beatrice/docs/infographic-brief.md`
- **Proposed generated asset path:** `beatrice/docs/assets/beatrice-infographic.svg` (or `.png`)
- **Proposed deterministic diagram source:** `beatrice/docs/pipeline.d2` (D2 is ideal — it produces styled SVG, supports themes, and is deterministic from source)
- **Best format:** D2 (for the pipeline flow diagram embedded in docs) + AI-generated PNG (for the polished stakeholder one-pager). Mermaid is also viable if D2 is not available.
- **Suggested script target:** Add to `package.json` as `"diagram": "d2 docs/pipeline.d2 docs/assets/pipeline.svg"` (or a `Makefile` target `make diagram`)
- **Docs pages to embed:**
  - `beatrice/README.md` — replace the plain ASCII architecture block with the rendered diagram
  - A new `beatrice/docs/overview.md` for stakeholder-facing description

---

## 13. Missing Information / Questions

1. **DEFRA branding:** Does DEFRA or the AI trade accelerator programme have an approved colour palette, logo, or design template that the infographic should follow? None is present in the repo.
2. **Beatrice name origin:** Is "Beatrice" an acronym or a project name? This affects whether it should appear with a defined treatment (e.g. *BEATRICE*) or as a plain word.
3. **Audience specificity:** Is the infographic for senior civil servants (Secretary of State level), technical reviewers, or external partners (e.g. industry bodies, devolved administrations)?
4. **Law proposition source:** How are law propositions currently being prepared? Is there a separate tool, a manual process, or a planned automated pipeline using the `SourceRecord`/`SourceSnapshot` domain models? This affects whether the "law upload" stage should look manual or automated.
5. **Divergence / cross-jurisdiction angle:** The domain models and repo name ("ai-eu-trade-accelerator") suggest a planned UK–EU law divergence use case. Should this be hinted at in the infographic or kept hidden until implemented?
6. **GOV.UK API scope:** Does the team have any concerns about the overlay feature implying official endorsement by GOV.UK? This could affect messaging.
7. **Scale / volume claims:** Are there any real performance numbers available (e.g. "processes a 50-section guidance page in under 2 minutes")? These would strengthen stakeholder copy.
8. **Intended deployment:** Is this a local developer tool only, or is there a plan to host it as a web service? The infographic narrative changes significantly between these two cases.