"""Anna CLI — re-adjudicate Beatrice's flagged findings with page context.

    anna readjudicate \
        --beatrice-run <beatrice/runs/NAME> \
        --dest         <anna/runs/NAME>

Reads Beatrice's ``results.json``, re-judges every top match flagged CONFLICTS or
GUIDANCE_INCOMPLETE against the rest of its page's guidance via the Anthropic
Message Batches API (50% cost), and writes a corrected run in Beatrice's own shape
(``results.json`` + a passed-through ``intermediates/``) so Esther can consume it
unchanged:

    esther build --beatrice-run <anna/runs/NAME> ...

The decision is committed: a flagged finding either stays flagged or becomes
GROUNDED. ``anna-report.json`` records every change for audit.
"""

from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path
from typing import Annotated

import typer

from .cost import cost_usd
from .readjudicate import (
    collect_flagged,
    make_anthropic_batch_judge,
    page_siblings,
    readjudicate,
)

app = typer.Typer(add_completion=False, help=__doc__)


@app.callback()
def _main() -> None:
    """Anna — re-adjudicate Beatrice's flagged findings before Esther renders them."""


def _load(path: Path):
    with path.open() as f:
        return json.load(f)


def _write(path: Path, data) -> None:
    with path.open("w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _write_model_card(dest: Path, m: dict, started_at: str) -> None:
    t = m["tokens"]
    (dest / "MODEL.md").write_text(f"""# anna run — {dest.name}

## Description

Anna re-adjudicates Beatrice's flagged top-matches (CONFLICTS / GUIDANCE_INCOMPLETE)
against the rest of each page's guidance, via the Anthropic Message Batches API
(50% cost). Output keeps Beatrice's schema so Esther consumes it unchanged.

## Provenance

- **Beatrice run:** `{m['beatrice_run']}`
- **Model:** Anthropic `{m['model']}` via the Message Batches API (50% cost).

## Started

{started_at}

## Headline

- **Guidance propositions:** {m['n_guidance_propositions']}
- **Flagged findings reviewed (LLM calls):** {m['n_flagged_reviewed']}
- **Findings changed:** {m['n_changed']}

## Cost

- **Tokens:** {t['input']:,} in / {t['output']:,} out
- **Cost:** **${m['cost_usd']}** (batch-discounted)

Full machine-readable breakdown in `metrics.json`; per-change audit in `anna-report.json`.
""")


@app.command(name="readjudicate")
def readjudicate_cmd(
    beatrice_run: Annotated[Path, typer.Option(
        "--beatrice-run", help="Beatrice run dir (results.json + intermediates/)")],
    dest: Annotated[Path, typer.Option(
        "--dest", help="Anna run dir to write (Beatrice-shaped: results.json + intermediates/)")],
    model: Annotated[str, typer.Option(help="Anthropic model")] = "claude-sonnet-4-6",
    poll_seconds: Annotated[int, typer.Option(help="Batch poll interval (s)")] = 30,
    dry_run: Annotated[bool, typer.Option(
        help="Count flagged findings without submitting the batch")] = False,
) -> None:
    output_path = beatrice_run / "results.json"
    if not output_path.exists():
        sys.exit(f"no results.json in Beatrice run: {output_path}")

    beatrice_output = _load(output_path)

    if dry_run:
        # No API spend: count what the batch would judge, then stop.
        flagged = collect_flagged(beatrice_output, page_siblings(beatrice_output))
        print(f"[dry-run] would submit {len(flagged)} flagged findings to the "
              f"batch API ({model}); stopping before any spend.")
        return

    started_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    judge = make_anthropic_batch_judge(model, poll_seconds=poll_seconds)
    corrected, report = readjudicate(beatrice_output, judge)
    report["model"] = model
    report["beatrice_run"] = str(beatrice_run)

    usage = dict(judge.usage)
    cost = round(cost_usd(usage, model, batch=True), 4)
    report["tokens"] = usage
    report["cost_usd"] = cost
    metrics = {
        "model": model,
        "beatrice_run": str(beatrice_run),
        "n_guidance_propositions": report["n_guidance_propositions"],
        "n_flagged_reviewed": report["n_flagged_reviewed"],
        "n_changed": report["n_changed"],
        "tokens": usage,
        "cost_usd": cost,
        "started_at": started_at,
    }

    print(
        f"reviewed {report['n_flagged_reviewed']} flagged findings; "
        f"changed {report['n_changed']} "
        f"({report['n_cleared_to_grounded']} cleared to GROUNDED). "
        f"by old status: {report['by_old_status'] or '{}'} "
        f"| ${cost}"
    )

    dest.mkdir(parents=True, exist_ok=True)
    _write(dest / "results.json", corrected)
    _write(dest / "anna-report.json", report)
    _write(dest / "metrics.json", metrics)
    _write_model_card(dest, metrics, started_at)

    # Pass Beatrice's intermediates through so the dest is a complete drop-in run.
    src_intermediates = beatrice_run / "intermediates"
    if src_intermediates.is_dir():
        shutil.copytree(src_intermediates, dest / "intermediates", dirs_exist_ok=True)
    else:
        print(f"warning: no intermediates/ in {beatrice_run} — Esther will need it from elsewhere")

    print(f"wrote {dest / 'results.json'} and {dest / 'anna-report.json'}")


if __name__ == "__main__":
    app()
