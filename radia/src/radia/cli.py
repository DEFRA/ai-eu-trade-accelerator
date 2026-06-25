"""Radia CLI — narrow a guidance corpus to the pages on-topic for the category.

    radia run <input.json> <output_dir/> [--shard-name s1] [--model ...]

Radia runs two passes, both on the Anthropic batch API: a recall-maximising
classifier (pass 1) followed by a precision adjudicator (pass 2) that drops the
false positives pass 1 over-includes. Input is Mary's output page shape: a list
of ``{url, content_id, meta_data: {title, body_text, ...}}`` objects (pages must
carry ``body_text``). Output is written to ``<output_dir>/output.json`` plus
``adjudication.json`` (the pass-1-vs-pass-2 audit trail) and a ``MODEL.md``.
"""
from __future__ import annotations

import json
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from .adjudicate import DEFAULT_ADJUDICATOR_MODEL, adjudicate
from .classify import DEFAULT_MODEL, classify
from .cost import cost_usd

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
    model: Annotated[str, typer.Option(help="Pass-1 classifier model id")] = DEFAULT_MODEL,
    adjudicator_model: Annotated[
        str, typer.Option(help="Pass-2 adjudicator model id")
    ] = DEFAULT_ADJUDICATOR_MODEL,
    max_words: Annotated[
        int, typer.Option(help="Body-truncation budget for oversized pages")
    ] = 60000,
    shard_name: Annotated[
        str | None, typer.Option(help="Optional per-row source_shard provenance tag")
    ] = None,
) -> None:
    categories, lexicons = _load_baked_in()
    cat = categories[0]["name"]
    items = json.loads(input_path.read_text())

    started = datetime.now().isoformat(timespec="seconds")
    results, stats = classify(
        items, categories, lexicons,
        model=model, max_words=max_words, shard_name=shard_name,
    )
    n_pass1 = sum(1 for r in results if r["meta_data"]["labels"].get(cat))
    results, adj, audit = adjudicate(
        items, results, categories, model=adjudicator_model, max_words=max_words,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "output.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False) + "\n"
    )
    (output_dir / "adjudication.json").write_text(
        json.dumps(audit, indent=2, ensure_ascii=False) + "\n"
    )

    # Token cost — recorded per pass (Haiku pass 1 + Sonnet pass 2), batch-discounted;
    # an oversized-page rescue is the one full-price call. Written after the run's
    # real outputs so a pricing gap can never cost the results.
    p1u, p2u = stats["usage"], adj["usage"]
    c1_batch = cost_usd(p1u["batch"]["input"], p1u["batch"]["output"], p1u["model"], batch=True)
    c1_rescue = cost_usd(p1u["rescue"]["input"], p1u["rescue"]["output"], p1u["model"], batch=False)
    c2 = cost_usd(p2u["batch"]["input"], p2u["batch"]["output"], p2u["model"], batch=True)

    def _sum(*vals: float | None) -> float | None:
        return None if any(v is None for v in vals) else round(sum(vals), 4)

    c1 = _sum(c1_batch, c1_rescue)
    total = _sum(c1, c2)
    p1_in = p1u["batch"]["input"] + p1u["rescue"]["input"]
    p1_out = p1u["batch"]["output"] + p1u["rescue"]["output"]
    p2_in, p2_out = p2u["batch"]["input"], p2u["batch"]["output"]
    _usd = lambda v: f"${v}" if v is not None else "not priced (unrecognised model)"  # noqa: E731

    (output_dir / "metrics.json").write_text(json.dumps({
        "pass1": {
            "model": p1u["model"], "batch": p1u["batch"],
            "rescue": p1u["rescue"], "cost_usd": c1,
        },
        "pass2": {"model": p2u["model"], "batch": p2u["batch"], "cost_usd": c2},
        "tokens": {"input": p1_in + p2_in, "output": p1_out + p2_out},
        "cost_usd": total,
        "started_at": started,
    }, indent=2) + "\n")

    n_true = sum(1 for r in results if r["meta_data"]["labels"].get(cat))
    base_rate = (n_true / len(results) * 100) if results else 0.0
    model_card = f"""# radia — {output_dir.name}

## Description

Radia run from the standalone `radia` package: two passes on the Anthropic batch
API. **Pass 1** (`{model}`) is recall-maximising — word-search prunes the corpus
to pages that hit the inclusion lexicon, and only those reach the LLM, whose
prompt is the slurry description with the Exclusion Criteria removed. **Pass 2**
(`{adjudicator_model}`) re-reads each page pass 1 labelled `{cat}: true` and, with
the Exclusion Criteria re-introduced, drops the false positives (homonyms, sewage,
incidental mentions). Pass 2 can only turn a TRUE into a FALSE, so it costs pass 1
no recall.

## Started

{started}

## Provenance

- **Input:** {input_path.name}{f" (shard `{shard_name}`)" if shard_name else ""}
- **Models used:** pass 1 `{model}`, pass 2 `{adjudicator_model}`, both via the
  Anthropic batch API; routing uses an NLTK Porter-stemmed inclusion lexicon (no model).

## Notes on settings

- **Pass 1 strategy:** `wordsearch_binary` — a lexicon hit routes the page to the
  LLM, otherwise it is recorded `{cat}: false` without an LLM call.
- **Pass 2:** decisive keep/drop over the pass-1 positives; an unparseable verdict
  leaves the page as pass 1 labelled it (kept), counted as `unadjudicated`.
- **Oversized pages:** bodies over {max_words} words are truncated; the trim is
  stamped into the reason, never silent.
- **Output shape:** `meta_data.labels.{cat}` (final, post-pass-2 bool), plus
  `scores`, `reasons`, and `selection_*_body` provenance. `body_text` is dropped.
  The pass-1-vs-pass-2 trail is in `adjudication.json`.

## Headline output

{len(results)} pages. Pass 1 routed {stats['n_routed']}, skipped {stats['n_skipped']},
rescued {stats['n_rescued']} → **{n_pass1}** `{cat}: true`. Pass 2 kept {adj['n_kept']}
and dropped {adj['n_dropped']} ({adj['n_unadjudicated']} unadjudicated) →
**{n_true}** final `{cat}: true` ({base_rate:.2f}% base rate).

## Cost

Both passes bill on the Anthropic batch API at 0.5×; an oversized-page rescue (if
any) is the one full-price call. Full machine-readable breakdown in `metrics.json`.

- **Pass 1** (`{model}`): {p1_in:,} in / {p1_out:,} out → {_usd(c1)}
- **Pass 2** (`{adjudicator_model}`): {p2_in:,} in / {p2_out:,} out → {_usd(c2)}
- **Total: {_usd(total)}** (batch-discounted)
"""
    (output_dir / "MODEL.md").write_text(model_card)

    typer.echo(
        f"Wrote {output_dir}\n"
        f"{len(results)} pages; pass1 {cat}-true={n_pass1} "
        f"(routed={stats['n_routed']} skipped={stats['n_skipped']} rescued={stats['n_rescued']}); "
        f"pass2 kept={adj['n_kept']} dropped={adj['n_dropped']} "
        f"unadjudicated={adj['n_unadjudicated']}; final {cat}-true={n_true} ({base_rate:.2f}%); "
        f"cost={_usd(total)}"
    )


if __name__ == "__main__":
    app()
