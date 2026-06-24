"""Susan CLI.

``susan run <pages.json> <out_dir>`` extracts propositions for every page
and writes the validator-compatible run artefacts to ``out_dir``.

Resumable: if ``out_dir/output.json`` already has entries, those URLs are
skipped on the next invocation.
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

import typer
from dotenv import load_dotenv

from .extract import (
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL,
    DEFAULT_TIMEOUT_SECONDS,
    extract_propositions,
)
from .fetch import fetch
from .prompt import PROMPT

load_dotenv()

app = typer.Typer(help="Susan — atomic proposition extraction from GOV.UK guidance.")

CANDIDATE_ID = "susan-default"
CANDIDATE_NAME = "susan production (sonnet-4-6 + tightened + metadata)"


def _load_pages(path: Path) -> list[dict]:
    raw = json.loads(path.read_text())
    if isinstance(raw, dict) and "pages" in raw:
        return raw["pages"]
    if isinstance(raw, list):
        return raw
    raise typer.BadParameter(
        f"{path} is not a recognised pages.json shape "
        "(expected {'pages': [...]} or a flat list)"
    )


def _existing(out_path: Path) -> dict[str, dict]:
    if not out_path.exists():
        return {}
    try:
        return {row["url"]: row for row in json.loads(out_path.read_text())}
    except (json.JSONDecodeError, KeyError):
        return {}


def _write_output(out_path: Path, rows: dict[str, dict], page_order: list[str]) -> None:
    ordered = [rows[u] for u in page_order if u in rows]
    out_path.write_text(json.dumps(ordered, indent=2))


def _prompt_hash() -> str:
    return hashlib.sha256(PROMPT.encode()).hexdigest()[:12]


def _write_model_card(
    out_dir: Path, *, model: str, started_at: str, n_pages: int, n_propositions: int
) -> None:
    body = f"""# susan — {out_dir.name}

## Description

A Susan run. Reads the body of each gov.uk page in the input and decomposes it
into atomic propositions carrying contextual metadata (subject_area,
instrument, actor).

## Provenance

- **Models used:** Anthropic `{model}`, hosted on the Anthropic API.
- **Prompt hash:** `{_prompt_hash()}`
- **Candidate:** `{CANDIDATE_ID}` — {CANDIDATE_NAME}

## Started

{started_at}

## Output shape

A list keyed by page URL, each with `meta_data.propositions[]`. Each proposition
has `proposition_text`, `subject_area`, `instrument`, `actor`, and
`source_paragraphs[]`. Scorable by the property-checks and pairwise-judgement
validators at `draft-pipelines/susan/validation/`.

## Headline

- **Pages:** {n_pages}
- **Propositions:** {n_propositions}
"""
    (out_dir / "MODEL.md").write_text(body)


@app.command()
def run(
    pages_path: Path = typer.Argument(..., exists=True, help="JSON of pages to extract"),
    out_dir: Path = typer.Argument(..., help="Output directory for output.json and MODEL.md"),
    model: str = typer.Option(DEFAULT_MODEL, help="Anthropic model name"),
    max_tokens: int = typer.Option(DEFAULT_MAX_TOKENS, help="Max output tokens"),
    timeout: int = typer.Option(DEFAULT_TIMEOUT_SECONDS, help="Per-page timeout seconds"),
    cache: Path | None = typer.Option(None, help="Optional disk cache dir for fetched pages"),
) -> None:
    """Extract propositions for every page; write a validator-compatible run."""
    pages = _load_pages(pages_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / "output.json"
    rows = _existing(out_json)
    page_order = [p["url"] for p in pages]
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    failures: list[tuple[str, str]] = []

    for i, page_meta in enumerate(pages, 1):
        url = page_meta["url"]
        if url in rows:
            print(f"[{i}/{len(pages)}] cached: {url}")
            continue
        print(f"[{i}/{len(pages)}] fetching + extracting: {url}")
        try:
            fetched = fetch(url, cache_dir=cache)
            propositions = extract_propositions(
                fetched, model=model, max_tokens=max_tokens, timeout=timeout
            )
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"[FAILED] {url} -- {msg}", file=sys.stderr)
            failures.append((url, msg))
            continue
        rows[url] = {
            "url": url,
            "meta_data": {
                "title": page_meta.get("title", fetched.title),
                "candidate_id": CANDIDATE_ID,
                "candidate_name": CANDIDATE_NAME,
                "propositions": [p.model_dump() for p in propositions],
            },
        }
        _write_output(out_json, rows, page_order)

    _write_output(out_json, rows, page_order)
    total_props = sum(
        len(r["meta_data"].get("propositions", [])) for r in rows.values()
    )
    _write_model_card(
        out_dir,
        model=model,
        started_at=started_at,
        n_pages=len(rows),
        n_propositions=total_props,
    )

    print()
    print(f"Done. {len(rows)} pages, {total_props} propositions -> {out_json}")
    if failures:
        print(f"\n{len(failures)} failure(s):")
        for url, msg in failures:
            print(f"  {url}\n    {msg}")
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
