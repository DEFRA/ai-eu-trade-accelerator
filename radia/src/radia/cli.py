"""Radia CLI — narrow a guidance corpus to the pages on-topic for the category.

    radia run <input.json> <output_dir/> [--shard-name s1] [--model ...]

Input is Mary's output page shape: a list of ``{url, content_id,
meta_data: {title, body_text, ...}}`` objects (pages must carry ``body_text``).
Output is written to ``<output_dir>/output.json`` plus a ``MODEL.md``.
"""
from __future__ import annotations

import json
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from .classify import DEFAULT_MODEL, classify

load_dotenv()

app = typer.Typer(add_completion=False, help=__doc__)


@app.callback()
def _main() -> None:
    """Radia — narrow a guidance corpus to the pages on-topic for the category."""


def _load_baked_in() -> tuple[list[dict], dict[str, list[str]]]:
    """The current version bakes in the slurry category + inclusion lexicon."""
    data = files("radia.data")
    categories = json.loads((data / "categories.json").read_text())
    lexicons = json.loads((data / "lexicon.json").read_text())
    return categories, lexicons


@app.command()
def run(
    input_path: Annotated[Path, typer.Argument(help="Pages JSON (list with meta_data.body_text)")],
    output_dir: Annotated[Path, typer.Argument(help="Directory to write output.json + MODEL.md")],
    model: Annotated[str, typer.Option(help="Anthropic model id")] = DEFAULT_MODEL,
    max_words: Annotated[
        int, typer.Option(help="Body-truncation budget for oversized-page rescue")
    ] = 60000,
    shard_name: Annotated[
        str | None, typer.Option(help="Optional per-row source_shard provenance tag")
    ] = None,
) -> None:
    categories, lexicons = _load_baked_in()
    items = json.loads(input_path.read_text())

    started = datetime.now().isoformat(timespec="seconds")
    results, stats = classify(
        items, categories, lexicons,
        model=model, max_words=max_words, shard_name=shard_name,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "output.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False) + "\n"
    )

    cat = categories[0]["name"]
    n_true = sum(1 for r in results if r["meta_data"]["labels"].get(cat))
    base_rate = (n_true / len(results) * 100) if results else 0.0
    model_card = f"""# radia — {output_dir.name}

## Description

Radia run from the standalone `radia` package (wordsearch-binary routing + Claude
Haiku batch). Word-search prunes the corpus to pages that hit the inclusion
lexicon; only those reach the LLM. The prompt is the slurry description with the
Exclusion Criteria section removed.

## Started

{started}

## Provenance

- **Input:** {input_path.name}{f" (shard `{shard_name}`)" if shard_name else ""}
- **Models used:** {model} via the Anthropic batch API; routing uses an
  NLTK Porter-stemmed inclusion lexicon (no model).

## Notes on settings

- **Strategy:** `wordsearch_binary` — lexicon hit routes to the LLM, otherwise
  recorded `{cat}: false` without an LLM call.
- **max_tokens:** 512. **Oversized rescue:** bodies over {max_words} words are
  truncated for a single Messages-API retry; the trim is stamped into the reason.
- **Output shape:** `meta_data.labels.{cat}` (bool), plus `scores`, `reasons`,
  and `selection_*_body` provenance. `body_text` is dropped.

## Headline output

{len(results)} pages, **{n_true}** with `{cat}: true` ({base_rate:.2f}% base rate).
{stats['n_routed']} routed to the LLM, {stats['n_skipped']} skipped by word-search,
{stats['n_rescued']} rescued after batch drop.
"""
    (output_dir / "MODEL.md").write_text(model_card)

    typer.echo(
        f"Wrote {output_dir}\n"
        f"{len(results)} pages, {n_true} {cat}-true ({base_rate:.2f}%); "
        f"routed={stats['n_routed']} skipped={stats['n_skipped']} "
        f"rescued={stats['n_rescued']}"
    )


if __name__ == "__main__":
    app()
