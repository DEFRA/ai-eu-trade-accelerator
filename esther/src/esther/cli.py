"""Esther CLI — shape the upstream pipeline outputs into the content-audit
frontend's data files.

    esther build \
        --beatrice-run   <beatrice/runs/NAME> \
        --input-pair     <beatrice/inputs/PAIR> \
        --radia-run      <radia/runs/NAME/output.json> \
        --seeds          <dir with categories.json + legislation.json + legislation-propositions.json> \
        --dest           <ai-sdlc-content-audit/.../audit/data/data>

Reads Beatrice's matched output, the input pair it ran on (for guidance ids and
law propositions), Beatrice's content intermediate (page metadata + analytics),
and Radia's run (relevance scores + audited-corpus size). Reading age is
recomputed per page via the gov.uk content API (cached on disk). Writes the
frontend data files in pipeline-native shape.
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
        raise typer.BadParameter(f"no category matching {slug!r} in {seeds_dir / 'categories.json'}")
    return {"id": slug, "title": seed.get("title") or seed.get("name"), "description": seed["description"]}


@app.command()
def build(
    beatrice_run: Annotated[Path, typer.Option(help="Beatrice run dir (output.json + intermediates/)")],
    input_pair: Annotated[Path, typer.Option(help="Beatrice input pair dir (guidance.json + law.json)")],
    radia_run: Annotated[Path, typer.Option(help="Radia output.json (relevance + corpus size)")],
    seeds: Annotated[Path, typer.Option(help="Dir with categories/legislation/legislation-propositions seeds")],
    dest: Annotated[Path, typer.Option(help="Frontend data dir to write into")],
    category_slug: Annotated[str, typer.Option(help="Category slug (native id)")] = "slurry",
    reading_age_cache: Annotated[
        Path | None, typer.Option(help="Reading-age content-API cache dir (reused across runs)")
    ] = None,
    dry_run: Annotated[bool, typer.Option(help="Compute and report, but write nothing")] = False,
) -> None:
    output_path = beatrice_run / "output.json"
    content_path = beatrice_run / "intermediates" / "01-content.json"
    for p in (output_path, content_path, input_pair / "guidance.json", input_pair / "law.json", radia_run):
        if not p.exists():
            raise typer.BadParameter(f"missing input: {p}")
    if not dry_run and not dest.exists():
        raise typer.BadParameter(f"destination data dir not found: {dest}")

    cache_dir = reading_age_cache or (beatrice_run.parent.parent.parent / "esther" / "cache")

    beatrice_output = _load(output_path)
    beatrice_content = _load(content_path)
    guidance_input = _load(input_pair / "guidance.json")
    law_input = _load_judit_propositions(_load(input_pair / "law.json"))
    radia_output = _load(radia_run)
    legislation_seed = _load(seeds / "legislation.json")
    legacy_law_props = _load(seeds / "legislation-propositions.json")
    category = _resolve_category(seeds, category_slug)

    # Reading age is recomputed per audited page (cached on disk -> cheap re-runs).
    page_urls = sorted({p["url"] for p in beatrice_content if p.get("url")})
    print(f"Scoring reading age for {len(page_urls)} pages (cache: {cache_dir})", file=sys.stderr)
    reading_age_by_url = ra.score_urls(page_urls, cache_dir, log=sys.stderr)

    files, warnings = build_mod.build(
        beatrice_output=beatrice_output,
        guidance_input=guidance_input,
        law_input=law_input,
        beatrice_content=beatrice_content,
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
