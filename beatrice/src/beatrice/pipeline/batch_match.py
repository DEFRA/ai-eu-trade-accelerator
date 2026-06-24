"""Beatrice batch matching runner (phase 2 of Radia -> Susan -> Beatrice).

Reads a Susan run (guidance) and a Judit export (law) BY PATH, embeds + retrieves
locally (Ollama), then group-reranks each guidance proposition via the Anthropic
Message Batches API (50% cost, async): a SINGLE request per guidance proposition
sees all of its surviving law candidates and labels each one. Records exact
tokens, cost, and wall-clock time into ``MODEL.md`` (human) and ``metrics.json``
(machine).

Prompts are shared with the in-process matcher via ``beatrice.matching.prompts``,
so the two paths cannot drift.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from openai import OpenAI

from beatrice.domain import Proposition
from beatrice.matching.embeddings import find_candidates
from beatrice.matching.prompts import (
    GROUP_RERANK_SYSTEM,
    LABEL_PRIORITY,
    SUMMARISE_SYSTEM,
    build_group_rerank_prompt,
    build_summarise_prompt,
    law_citation,
    parse_group_rerank_json,
)

from .mapping import guidance_from_susan_output

# Per-1M-token rates (input, output), full price; the Batch API bills at 0.5x.
RATES = {
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5": (1.0, 5.0),
}
BATCH_DISCOUNT = 0.5
CLASSIFY_MAX_TOKENS = 2048
SUMMARISE_MAX_TOKENS = 512


# ── local embeddings (Ollama, reuses beatrice's cosine matcher) ─────────────────

def _embed(texts: list[str], base_url: str, model: str) -> list[list[float]]:
    client = OpenAI(base_url=base_url, api_key="ollama", max_retries=3, timeout=120)
    out: list[list[float]] = []
    for i in range(0, len(texts), 128):
        resp = client.embeddings.create(model=model, input=texts[i : i + 128])
        out.extend(d.embedding for d in sorted(resp.data, key=lambda x: x.index))
    return out


# ── law loading + enrichment ──────────────────────────────────────────────────

def _load_law(path: Path) -> list[Proposition]:
    """Load Judit law propositions, lifting the extraction-quality flags Judit
    stashes in ``notes.judit_extraction_meta`` onto the top-level fields the
    group-rerank confidence sidebar reads."""
    raw = json.loads(path.read_text())
    records = raw if isinstance(raw, list) else raw.get("propositions", [])
    out: list[Proposition] = []
    for rec in records:
        lp = Proposition.model_validate(rec)
        meta = _judit_meta(rec)
        if meta:
            if lp.model_confidence is None:
                lp.model_confidence = meta.get("model_confidence")
            if lp.completeness_status is None:
                lp.completeness_status = meta.get("completeness_status")
            if lp.fallback_policy is None:
                lp.fallback_policy = meta.get("fallback_policy")
            if lp.fallback_used is None:
                lp.fallback_used = meta.get("fallback_used")
        out.append(lp)
    return out


def _judit_meta(rec: dict) -> dict:
    """Best-effort lift of Judit's extraction meta blob from a raw record."""
    notes = rec.get("notes")
    if isinstance(notes, dict):
        meta = notes.get("judit_extraction_meta")
        if isinstance(meta, dict):
            return meta
    meta = rec.get("judit_extraction_meta")
    return meta if isinstance(meta, dict) else {}


def _apply_cache(items: list, cache_path: Path | None, field: str, *, key: str = "id") -> int:
    """Merge a ``{id: value}`` (or ``{id: {field: value}}``) cache onto a list of
    pydantic objects by id. Returns the number of records enriched."""
    if cache_path is None or not cache_path.exists():
        return 0
    cache = json.loads(cache_path.read_text())
    n = 0
    for obj in items:
        entry = cache.get(getattr(obj, key))
        if entry is None:
            continue
        value = entry.get(field) if isinstance(entry, dict) else entry
        if value:
            setattr(obj, field, value)
            n += 1
    return n


# ── batch helper ────────────────────────────────────────────────────────────────

def _submit_and_collect(client, requests, poll_seconds: int, label: str):
    """Submit one batch, poll to completion, return (id, {custom_id: message},
    usage totals, errored count, wall-clock seconds)."""
    t0 = time.time()
    batch = client.messages.batches.create(requests=requests)
    print(f"[{label}] submitted {batch.id} ({len(requests)} requests)", flush=True)
    while True:
        b = client.messages.batches.retrieve(batch.id)
        if b.processing_status == "ended":
            break
        c = b.request_counts
        print(f"[{label}] {b.processing_status}: proc={c.processing} ok={c.succeeded} err={c.errored}", flush=True)
        time.sleep(poll_seconds)

    messages: dict = {}
    usage = {"input": 0, "output": 0, "cache_read": 0}
    errored = 0
    for r in client.messages.batches.results(batch.id):
        if r.result.type != "succeeded":
            errored += 1
            continue
        msg = r.result.message
        usage["input"] += msg.usage.input_tokens
        usage["output"] += msg.usage.output_tokens
        usage["cache_read"] += getattr(msg.usage, "cache_read_input_tokens", 0) or 0
        messages[r.custom_id] = msg
    return batch.id, messages, usage, errored, time.time() - t0


def _text(message) -> str:
    return next((b.text for b in message.content if getattr(b, "type", None) == "text"), "")


def _cost(usage: dict, model: str) -> float:
    rin, rout = RATES.get(model, RATES["claude-sonnet-4-6"])
    billable_in = usage["input"] + usage["cache_read"] * 0.1
    return (billable_in / 1e6 * rin + usage["output"] / 1e6 * rout) * BATCH_DISCOUNT


# ── CSV (mirrors the explorer export) ────────────────────────────────────────────

CSV_HEADERS = [
    "URL", "Guidance Proposition", "Summary", "Law Proposition", "Law Citation",
    "Relationship", "Explanation", "Confidence", "Correctness Score",
]


def _write_csv(records: list[dict], path: Path) -> None:
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(CSV_HEADERS)
        for rec in records:
            url, gp_text, summary = rec["url"], rec["proposition_text"], rec.get("summary", "")
            if not rec["matches"]:
                w.writerow([url, gp_text, summary, "", "", "", "", "", ""])
            for m in rec["matches"]:
                lp = m["law_proposition"]
                w.writerow([
                    url, gp_text, summary, lp.get("proposition_text", ""), law_citation(lp),
                    m["relationship"], m["explanation"], m["confidence"], f"{m['correctness_score']:.2f}",
                ])


def main() -> None:
    ap = argparse.ArgumentParser(description="Beatrice batch matching: Susan guidance vs Judit law (group-rerank).")
    ap.add_argument("--guidance", type=Path, required=True, help="Susan run output.json")
    ap.add_argument("--law", type=Path, required=True, help="Judit propositions.json")
    ap.add_argument("--out", type=Path, required=True, help="Output run directory")
    ap.add_argument("--model", default="claude-sonnet-4-6")
    ap.add_argument("--top-k", type=int, default=15, help="Hard cap on survivors per guidance proposition.")
    ap.add_argument("--threshold", type=float, default=0.65, help="Minimum cosine similarity for a survivor.")
    ap.add_argument("--score-gap", type=float, default=0.04, help="Adaptive-size window below the top score.")
    ap.add_argument("--no-sidebar", action="store_true", help="Omit Judit's per-candidate confidence sidebar.")
    ap.add_argument("--clause-functions", type=Path, default=None, help="Optional {law_id: clause_function} cache.")
    ap.add_argument("--law-topics", type=Path, default=None, help="Optional {law_id: topic} cache.")
    ap.add_argument("--guidance-topics", type=Path, default=None, help="Optional {guidance_id: topic} cache.")
    ap.add_argument("--embed-base-url", default=os.getenv("EMBED_BASE_URL", "http://127.0.0.1:11434/v1"))
    ap.add_argument("--embed-model", default=os.getenv("EMBED_MODEL", "nomic-embed-text:v1.5"))
    ap.add_argument("--poll-seconds", type=int, default=30)
    ap.add_argument("--dry-run", action="store_true", help="Embed + retrieve + build requests, but do not submit batches.")
    args = ap.parse_args()

    render_conf_sidebar = not args.no_sidebar

    import anthropic
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming as Params
    from anthropic.types.messages.batch_create_params import Request

    args.out.mkdir(parents=True, exist_ok=True)
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S")

    # ── load inputs (by path) ───────────────────────────────────────────────────
    g_raw = json.loads(args.guidance.read_text())
    guidance = guidance_from_susan_output(g_raw if isinstance(g_raw, list) else g_raw.get("pages", []))
    law_props = _load_law(args.law)
    law_by_id = {lp.id: lp for lp in law_props}

    # ── optional typed-tag enrichment (clause_function / topic) ──────────────────
    n_cf = _apply_cache(law_props, args.clause_functions, "clause_function")
    n_lt = _apply_cache(law_props, args.law_topics, "topic")
    n_gt = _apply_cache(guidance, args.guidance_topics, "topic")
    print(f"guidance: {len(guidance)} propositions | law: {len(law_props)} propositions", flush=True)
    print(f"enrichment: clause_function {n_cf}/{len(law_props)} | law topic {n_lt}/{len(law_props)} | guidance topic {n_gt}/{len(guidance)}", flush=True)

    # ── embed + retrieve (local, timed) ─────────────────────────────────────────
    t_local = time.time()
    law_index = list(zip([lp.id for lp in law_props], _embed([lp.proposition_text for lp in law_props], args.embed_base_url, args.embed_model), strict=False))
    g_embs = _embed([gp.proposition_text for gp in guidance], args.embed_base_url, args.embed_model)
    survivors_per_gp: list[list[tuple[str, float]]] = []
    for g_emb in g_embs:
        survivors_per_gp.append(
            find_candidates(g_emb, law_index, top_k=args.top_k, threshold=args.threshold, score_gap=args.score_gap)
        )
    local_seconds = time.time() - t_local

    # One group-rerank request per guidance proposition that has survivors.
    # Propositions with no survivor above the threshold cost no LLM call.
    rerank_reqs = []
    matched_gis = []
    for gi, survivors in enumerate(survivors_per_gp):
        if not survivors:
            continue
        cands = [(law_by_id[law_id], score) for law_id, score in survivors]
        prompt = build_group_rerank_prompt(guidance[gi], cands, render_conf_sidebar=render_conf_sidebar)
        rerank_reqs.append(
            Request(
                custom_id=f"g-{gi}",
                params=Params(model=args.model, max_tokens=CLASSIFY_MAX_TOKENS, system=GROUP_RERANK_SYSTEM,
                              messages=[{"role": "user", "content": prompt}]),
            )
        )
        matched_gis.append(gi)
    n_producing = len(matched_gis)
    n_empty = len(guidance) - n_producing
    print(f"retrieved: {n_producing} guidance props with survivors ({n_empty} empty, free) | local embed+retrieve {local_seconds:.0f}s", flush=True)

    if args.dry_run:
        print(f"[dry-run] would submit {len(rerank_reqs)} group-rerank requests; stopping before any API spend.")
        return

    client = anthropic.Anthropic()

    # ── group-rerank batch ───────────────────────────────────────────────────────
    cls_id, cls_msgs, cls_usage, cls_err, cls_secs = _submit_and_collect(client, rerank_reqs, args.poll_seconds, "group-rerank")
    classified: dict[int, list[dict]] = {}
    for gi in matched_gis:
        survivors = survivors_per_gp[gi]
        sim_by_id = {law_id: score for law_id, score in survivors}
        msg = cls_msgs.get(f"g-{gi}")
        if msg is None:
            classified[gi] = []
            continue
        try:
            verdict = parse_group_rerank_json(_text(msg))
        except Exception:
            classified[gi] = []
            continue
        id_by_suffix = {lid.removeprefix("prop:"): lid for lid in sim_by_id}
        rows = []
        for m in verdict["matches"]:
            raw_id = m["law_id"]
            canonical = raw_id if raw_id in sim_by_id else id_by_suffix.get(raw_id.removeprefix("prop:"), raw_id)
            if canonical not in law_by_id:
                continue
            rows.append({
                "law_proposition": law_by_id[canonical].model_dump(),
                "similarity_score": round(float(sim_by_id.get(canonical, 0.0)), 4),
                "relationship": m["relationship"],
                "confidence": m["confidence"],
                "explanation": m["explanation"],
                "correctness_score": m["correctness_score"],
            })
        rows.sort(key=lambda r: LABEL_PRIORITY.get(r["relationship"], 99))
        classified[gi] = rows

    # ── summarise batch (only gps with matches) ─────────────────────────────────
    summ_reqs = [
        Request(
            custom_id=f"s-{gi}",
            params=Params(model=args.model, max_tokens=SUMMARISE_MAX_TOKENS, system=SUMMARISE_SYSTEM,
                          messages=[{"role": "user", "content": build_summarise_prompt(guidance[gi], rows)}]),
        )
        for gi, rows in classified.items() if rows
    ]
    if summ_reqs:
        summ_id, summ_msgs, summ_usage, summ_err, summ_secs = _submit_and_collect(client, summ_reqs, args.poll_seconds, "summarise")
    else:
        summ_id, summ_msgs, summ_usage, summ_err, summ_secs = None, {}, {"input": 0, "output": 0, "cache_read": 0}, 0, 0.0

    # ── assemble results ────────────────────────────────────────────────────────
    records = []
    for gi, gp in enumerate(guidance):
        records.append({
            "url": gp.source_url,
            "id": gp.id,
            "proposition_text": gp.proposition_text,
            "summary": _text(summ_msgs[f"s-{gi}"]) if f"s-{gi}" in summ_msgs else "",
            "matches": classified.get(gi, []),
        })
    (args.out / "results.json").write_text(json.dumps(records, indent=2))
    _write_csv(records, args.out / "results.csv")

    # ── metrics + model card ────────────────────────────────────────────────────
    cls_cost, summ_cost = _cost(cls_usage, args.model), _cost(summ_usage, args.model)
    total_seconds = local_seconds + cls_secs + summ_secs
    metrics = {
        "model": args.model, "strategy": "group_rerank",
        "top_k": args.top_k, "threshold": args.threshold, "score_gap": args.score_gap,
        "render_conf_sidebar": render_conf_sidebar,
        "guidance_propositions": len(guidance), "law_propositions": len(law_props),
        "guidance_props_with_survivors": n_producing, "guidance_props_empty": n_empty,
        "enrichment": {"clause_function": n_cf, "law_topic": n_lt, "guidance_topic": n_gt},
        "group_rerank": {"requests": len(rerank_reqs), "errored": cls_err, "batch_id": cls_id, **cls_usage, "cost_usd": round(cls_cost, 4)},
        "summarise": {"requests": len(summ_reqs), "errored": summ_err, "batch_id": summ_id, **summ_usage, "cost_usd": round(summ_cost, 4)},
        "tokens": {"input": cls_usage["input"] + summ_usage["input"], "output": cls_usage["output"] + summ_usage["output"]},
        "cost_usd_total": round(cls_cost + summ_cost, 4),
        "time_seconds": {"local_embed_retrieve": round(local_seconds, 1), "group_rerank_batch": round(cls_secs, 1), "summarise_batch": round(summ_secs, 1), "total": round(total_seconds, 1)},
    }
    (args.out / "metrics.json").write_text(json.dumps(metrics, indent=2))
    _write_model_card(args.out, metrics, started_at, args.guidance, args.law)

    print(f"\n[done] {len(records)} guidance props | ${metrics['cost_usd_total']} | {total_seconds:.0f}s -> {args.out}", flush=True)
    if cls_err or summ_err:
        print(f"  {cls_err} group-rerank + {summ_err} summarise requests errored", file=sys.stderr)


def _write_model_card(out: Path, m: dict, started_at: str, guidance: Path, law: Path) -> None:
    def mins(s: float) -> str:
        return f"{int(s) // 60}m{int(s) % 60:02d}s"
    t = m["time_seconds"]
    (out / "MODEL.md").write_text(f"""# beatrice run — {out.name}

## Description

Beatrice batch matching run (group-rerank): each Susan guidance proposition is
classified against its surviving Judit law candidates in a single LLM call, with
a compliance summary per producing proposition. Prompts are shared with the
in-process matcher (`beatrice.matching.prompts`).

## Provenance

- **Guidance (Susan):** `{guidance}`
- **Law (Judit):** `{law}`
- **Model:** Anthropic `{m['model']}` via the Message Batches API (50% cost).
- **Strategy:** group-rerank — local nomic embeddings + cosine retrieval
  (threshold {m['threshold']}, gap {m['score_gap']}, top-{m['top_k']}), then one
  grouped LLM call per guidance proposition.

## Started

{started_at}

## Headline

- **Guidance propositions:** {m['guidance_propositions']}
- **Law propositions:** {m['law_propositions']}
- **Guidance props with survivors (LLM calls):** {m['guidance_props_with_survivors']}
- **Guidance props empty (no candidate, free):** {m['guidance_props_empty']}
- **Typed-tag enrichment:** clause_function {m['enrichment']['clause_function']}, law topic {m['enrichment']['law_topic']}, guidance topic {m['enrichment']['guidance_topic']}

## Cost & runtime

- **Tokens:** {m['tokens']['input']:,} in / {m['tokens']['output']:,} out
- **Cost:** **${m['cost_usd_total']}** total — group-rerank ${m['group_rerank']['cost_usd']}, summarise ${m['summarise']['cost_usd']} (batch-discounted)
- **Time (wall-clock):**
  - local (embed + retrieve): {mins(t['local_embed_retrieve'])}
  - group-rerank batch (submit → ended, incl. queue wait): {mins(t['group_rerank_batch'])}
  - summarise batch (incl. queue wait): {mins(t['summarise_batch'])}
  - **total: {mins(t['total'])}**

Full machine-readable breakdown in `metrics.json`.
""")


if __name__ == "__main__":
    main()
