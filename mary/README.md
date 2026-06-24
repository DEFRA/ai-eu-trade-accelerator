# Mary

**Download all DEFRA guidance gov.uk pages.** Mary is the pipeline entry point for
the guidance branch: it discovers
every in-scope gov.uk page and fetches each one's full body text, producing the
page corpus that **Radia** then narrows.

Mary is two stages, copied near-verbatim from the original `guidance_catalogue`
scripts. It does **no** topic-aware filtering — that is Radia's job downstream.

## The two dials — `src/filters.py`

Mary's entire selection logic is two static lists, and they live at the top of
[`src/filters.py`](src/filters.py):

- **`ORGANISATIONS`** — the ~45 gov.uk publishing bodies in scope.
- **`DOCUMENT_TYPES`** — the 13 content-store document types that count as guidance.

There is no query string, no relevance ranking, no date or popularity gate. A page
is pulled if **its org is in `ORGANISATIONS` AND its type is in `DOCUMENT_TYPES`** —
then *every* matching page is fetched in full. Widen the lists for a bigger corpus
(longer fetch, higher recall ceiling); narrow them for a smaller, faster one. These
two lists are the only levers on what the corpus contains.

`filters.py` is *what* we pull. The *how* — endpoints, page size, rate limits,
timeouts, default paths — is named in [`src/config.py`](src/config.py); the page
shape both stages share is in [`src/page.py`](src/page.py). No magic numbers live
in the stage scripts.

## Pipeline

```
  ① discovery (fast)              ② body fetch (the wait)
  get_defra_guidance_pages.py     fetch_body.py
  Search API, metadata only  -->  Content API, one call per URL  -->  Radia
  ~19 paged calls, seconds        ~18.5k calls @ 10 req/s ≈ 30 min
```

Mary uses [`uv`](https://docs.astral.sh/uv/) — the same tool as the other steps.
`uv` reads `pyproject.toml`, installs the right dependencies automatically, and
runs the scripts; you don't need to set up Python yourself. The whole corpus build
is one command:

```bash
./run.sh                                       # stage 1 then stage 2, default paths
```

Or run the stages individually. Both default their paths from `config.py`, so the
no-argument runs chain end-to-end. **Stage 1 — `src/get_defra_guidance_pages.py`**
queries the GOV.UK Search API using the two dials and writes pipeline-format JSON
(metadata only, **no** body text). It fails loudly: a non-200 response or a result
count that disagrees with the API's reported total aborts rather than writing a
truncated corpus.

```bash
uv run python src/get_defra_guidance_pages.py  # -> output/search_api.json
```

**Stage 2 — `src/fetch_body.py`** takes that file, calls the GOV.UK Content API once
per URL, and adds `meta_data.body_text`. Restartable — already-fetched URLs are
skipped on re-run — and its final summary loudly counts any pages whose fetch
failed (`body_text` null), since those are invisible to Radia:

```bash
uv run python src/fetch_body.py                # output/search_api.json -> output/with_body.json
# or override: uv run python src/fetch_body.py IN.json OUT.json --concurrency 3 --rate 10.0
```

The search is cheap (~19 requests); the cost is stage 2 fetching every body
one-by-one at a polite rate — that is the "sit and wait", and `--rate` /
`--concurrency` are the speed-vs-politeness dials.

### Outputs / caching

The two JSON files **are** the cache: stage 2 reads its own output and skips URLs
already fetched, so a re-run only retries failures. They're run artifacts (the
real home for them is `content-audit-data-assets`), so `output/` is git-ignored.

## Output shape

Stage 2's output is exactly Radia's expected input — discovery metadata plus
`body_text`:

```json
[
  {
    "url": "https://www.gov.uk/...",
    "content_id": "...",
    "meta_data": {
      "title": "...",
      "description": "...",
      "updated_at": "...",
      "document_type": "...",
      "view_count": 0,
      "body_text": "..."
    }
  }
]
```

## Tests

```bash
uv run pytest
```

A small suite over the pure logic in each stage: stage 1's Search-hit → page
contract mapping (`to_pipeline_item`) and stage 2's body-text extraction
(`_extract_body_text`). They run offline — no network calls.

## Dependencies

Just `httpx` and `typer` at runtime, declared in `pyproject.toml` and pinned in
`uv.lock` (`pytest` is a dev-only dependency). Mary needs no API keys or `.env` —
both stages only hit public gov.uk endpoints.
