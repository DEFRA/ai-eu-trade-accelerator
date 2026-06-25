"""Esther CLI — shape the upstream pipeline outputs into the content-audit
frontend's data files.

    esther build \
        --beatrice-run   <beatrice/runs/NAME> \
        --input-pair     <beatrice/inputs/PAIR> \
        --radia-run      <radia/runs/NAME/output.json> \
        --seeds          <dir with the category + legislation seed files> \
        --dest           <ai-sdlc-content-audit/.../audit/data/data>

Reads Beatrice's matched output (results.json, which carries the native guidance
ids), the input pair's ``law.json`` (Judit law propositions), and Radia's run —
which supplies both the audited pages (content_id + page metadata + analytics)
and per-page relevance scores + corpus size. Reading age is recomputed per page
via the gov.uk content API (cached on disk). Writes the frontend data files in
pipeline-native shape.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated

import typer

from . import build as build_mod
from . import reading_age as ra

app = typer.Typer(add_completion=False, help=__doc__)


@app.callback()
def _main() -> None:
    """Esther — shape the audit for the content-audit frontend."""


def _load(path: Path):
    with path.open() as f:
        return json.load(f)


def _load_judit_propositions(data) -> list[dict]:
    """Accept either a bare list or Judit's ``{run_id, propositions:[…]}`` wrap."""
    if isinstance(data, dict) and "propositions" in data:
        return data["propositions"]
    return data


def _write(path: Path, data) -> None:
    with path.open("w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _resolve_category(seeds_dir: Path, slug: str) -> dict:
    cats = _load(seeds_dir / "categories.json")
    seed = next((c for c in cats if c.get("id") == slug or c.get("name") == slug), None)
    if seed is None and len(cats) == 1:
        seed = cats[0]
    if seed is None:
        raise typer.BadParameter(
            f"no category matching {slug!r} in {seeds_dir / 'categories.json'}"
        )
    return {
        "id": slug,
        "title": seed.get("title") or seed.get("name"),
        "description": seed["description"],
    }


@app.command()
def build(
    beatrice_run: Annotated[Path, typer.Option(help="Beatrice run dir (results.json)")],
    input_pair: Annotated[Path, typer.Option(help="Beatrice input pair dir (law.json)")],
    radia_run: Annotated[Path, typer.Option(help="Radia output.json (pages, relevance, corpus)")],
    seeds: Annotated[
        Path,
        typer.Option(help="Dir with categories/legislation/legislation-propositions seeds"),
    ],
    dest: Annotated[Path, typer.Option(help="Frontend data dir to write into")],
    category_slug: Annotated[str, typer.Option(help="Category slug (native id)")] = "slurry",
    reading_age_cache: Annotated[
        Path | None, typer.Option(help="Reading-age content-API cache dir (reused across runs)")
    ] = None,
    dry_run: Annotated[bool, typer.Option(help="Compute and report, but write nothing")] = False,
) -> None:
    output_path = beatrice_run / "results.json"
    for p in (output_path, input_pair / "law.json", radia_run):
        if not p.exists():
            raise typer.BadParameter(f"missing input: {p}")
    if not dry_run and not dest.exists():
        raise typer.BadParameter(f"destination data dir not found: {dest}")

    cache_dir = reading_age_cache or (beatrice_run.parent.parent.parent / "esther" / "cache")

    beatrice_output = _load(output_path)
    law_input = _load_judit_propositions(_load(input_pair / "law.json"))
    radia_output = _load(radia_run)
    legislation_seed = _load(seeds / "legislation.json")
    legacy_law_props = _load(seeds / "legislation-propositions.json")
    category = _resolve_category(seeds, category_slug)

    # Reading age is recomputed per audited page (cached on disk -> cheap re-runs).
    # Only the audited pages — those Beatrice produced guidance for — are scored;
    # build() consumes reading age for exactly that set, so scoring the whole
    # Radia corpus would fetch thousands of pages whose scores are never read.
    audited_urls = {e["url"] for e in beatrice_output if e.get("url")}
    page_urls = sorted(
        {p["url"] for p in radia_output if p.get("url") and p["url"] in audited_urls}
    )
    print(f"Scoring reading age for {len(page_urls)} pages (cache: {cache_dir})", file=sys.stderr)
    reading_age_by_url = ra.score_urls(page_urls, cache_dir, log=sys.stderr)

    files, warnings = build_mod.build(
        beatrice_output=beatrice_output,
        law_input=law_input,
        radia_output=radia_output,
        legislation_seed=legislation_seed,
        legacy_law_props=legacy_law_props,
        category=category,
        reading_age_by_url=reading_age_by_url,
    )

    print("\nCounts:", file=sys.stderr)
    for name, data in files.items():
        n = len(data) if isinstance(data, list) else 1
        print(f"  {name:<32} {n}", file=sys.stderr)
    summary = files["subject-summary.json"][0]  # type: ignore[index]
    print(f"  status counts: {summary['proposition_status_counts']}", file=sys.stderr)

    if warnings:
        print(f"\n{len(warnings)} warning(s):", file=sys.stderr)
        for w in warnings:
            print(f"  WARN: {w}", file=sys.stderr)

    if dry_run:
        print("\nDry run — no files written.", file=sys.stderr)
        return

    for name, data in files.items():
        _write(dest / name, data)
    print(f"\nWrote {len(files)} files to {dest}", file=sys.stderr)


if __name__ == "__main__":
    app()
