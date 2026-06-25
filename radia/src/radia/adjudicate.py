r"""Radia's pass 2: precision adjudication of pass-1 positives via the batch API.

Pass 1 is recall-maximised — its prompt drops the Exclusion Criteria and forces
the label TRUE on any slurry mention, even an incidental one. That over-includes:
homonyms (coal / laboratory / NORM slurry), sewage sludge, and pages that merely
name slurry once in a list. Pass 2 re-reads the body of every page pass 1 labelled
TRUE and, with the team's Exclusion Criteria re-introduced, drops those false
positives.

Because it only ever turns a TRUE into a FALSE, the exclusion criteria cost pass 1
no recall: the ~98% of pages the word-search never routed are untouched. The
prompt is tuned to the recall-priority operating point — it removes only
unambiguous non-slurry noise and keeps anything with real slurry-audit value
(rules, grants/funding, infrastructure, feedstock).

No silent fallbacks: a page whose verdict cannot be parsed is left exactly as
pass 1 labelled it (kept) and counted in ``n_unadjudicated`` — never quietly
dropped. The full pass-1-vs-pass-2 trail is returned for a side audit file, kept
off the pipeline output.
"""
from __future__ import annotations

import json
import os

import anthropic

from ._batch import submit_and_collect

DEFAULT_ADJUDICATOR_MODEL = os.getenv("RADIA_ADJUDICATOR_MODEL", "claude-sonnet-4-6")
MAX_TOKENS = 500


def full_spec(category: dict) -> str:
    """The complete category definition — inclusion description + exclusion criteria.

    Pass 1 uses only ``description`` (no exclusion); pass 2 needs both halves so the
    adjudicator can reason about what is out of scope.
    """
    spec = category["description"]
    exclusion = category.get("exclusion_criteria", "").strip()
    return f"{spec}\n\n{exclusion}" if exclusion else spec


def build_prompt(spec: str, title: str, body: str, pass1_reason: str) -> str:
    """The recall-priority adjudication prompt: drop only unambiguous non-slurry noise."""
    title_line = f"Page title: {title}\n" if title else ""
    return (
        "You curate a DEFRA guidance corpus for a SLURRY policy/legislation audit. A "
        "fast first pass flagged this page because a slurry keyword appears; it forces "
        "TRUE on any mention. Remove ONLY pages that are clearly not about slurry at "
        "all. Missing a genuinely slurry-relevant page is unacceptable; keeping a "
        "borderline one is fine. Default to KEEP.\n\n"
        f"=== SLURRY CATEGORY SPEC (meaning of in-scope slurry) ===\n{spec}\n"
        "=== END SPEC ===\n\n"
        "KEEP (about_slurry = true) — keep if ANY of these is even briefly present, "
        "regardless of the page's main topic:\n"
        "- Any mention of slurry/manure storage, handling, processing, spreading, land "
        "application, tanks, lagoons, stores, runoff, ammonia or emissions.\n"
        "- Any RULE, regulation, prohibition, permit, inspection or risk assessment that "
        "touches slurry — including a single citation of SSAFO / NVZ rules, a rule that "
        "slurry spreading is prohibited in a disease zone, or a 'slurry management' "
        "section even if brief.\n"
        "- Any GRANT or FUNDING that can apply to slurry infrastructure/equipment, even "
        "named once.\n"
        "- Animal slurry/manure named as a FEEDSTOCK or input (e.g. anaerobic digestion).\n\n"
        "DROP (about_slurry = false) — ONLY when slurry has zero audit value here:\n"
        "- HOMONYM / non-farming slurry: bentonite, coal/mining, ceramic, drilling, "
        "engineering slurry; laboratory 'slurrying' of samples.\n"
        "- SEWAGE sludge / biosolids / septic-tank sludge as the subject.\n"
        "- Slurry named ONLY as a disease-transmission medium, ONLY as a vehicle/"
        "equipment-wash or biosecurity step, or ONLY as a dilutant — with NO rule, "
        "funding, infrastructure or management content about slurry itself.\n"
        "If you are unsure, KEEP.\n\n"
        f"For context, the first pass wrote this note (it may be wrong): {pass1_reason!r}\n\n"
        f"{title_line}\nPage content:\n{body}\n\n"
        "Return ONLY a JSON object:\n"
        "{\n"
        '  "about_slurry": true|false,\n'
        '  "exclusion_hit": "none"|"incidental"|"homonym"|"sewage",\n'
        '  "verdict_reason": "1-2 sentences citing the specific evidence"\n'
        "}"
    )


def parse_verdict(text: str) -> dict | None:
    """Parse an adjudicator reply into ``{about_slurry, exclusion_hit, verdict_reason}``.

    Returns None for an unparseable reply or one missing the verdict — the caller
    keeps such a page rather than guessing.
    """
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
        text = text.rsplit("```", 1)[0].strip()
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if "about_slurry" not in parsed:
        return None
    return {
        "about_slurry": bool(parsed["about_slurry"]),
        "exclusion_hit": str(parsed.get("exclusion_hit", "none")),
        "verdict_reason": str(parsed.get("verdict_reason", "")),
    }


def adjudicate(
    items: list[dict],
    results: list[dict],
    categories: list[dict],
    *,
    model: str = DEFAULT_ADJUDICATOR_MODEL,
    max_words: int = 60000,
) -> tuple[list[dict], dict, list[dict]]:
    """Re-adjudicate pass-1 positives in place. Returns (results, stats, audit).

    ``results`` is pass 1's labelled output (``body_text`` already dropped); bodies
    are joined back from ``items`` by URL. Each positive's ``labels``/``reasons`` are
    updated to the pass-2 verdict; everything else is left untouched.
    """
    cat = categories[0]["name"]
    spec = full_spec(categories[0])
    bodies = {it["url"]: it["meta_data"].get("body_text", "") for it in items}

    positives = [r for r in results if r["meta_data"]["labels"].get(cat) is True]
    stats = {
        "n_positives": len(positives),
        "n_kept": 0,
        "n_dropped": 0,
        "n_unadjudicated": 0,
        "model": model,
        "usage": {"model": model, "batch": {"input": 0, "output": 0}},
    }
    audit: list[dict] = []
    if not positives:
        return results, stats, audit

    requests: list = []
    truncated: dict[str, tuple[int, int]] = {}
    for i, r in enumerate(positives):
        md = r["meta_data"]
        body = bodies.get(r["url"], "")
        words = body.split()
        if len(words) > max_words:
            truncated[r["url"]] = (max_words, len(words))
            body = " ".join(words[:max_words])
        prompt = build_prompt(spec, md.get("title", ""), body, md.get("reasons", {}).get(cat, ""))
        requests.append({
            "custom_id": str(i),
            "params": {
                "model": model,
                "max_tokens": MAX_TOKENS,
                "messages": [{"role": "user", "content": prompt}],
            },
        })

    client = anthropic.Anthropic()
    raw, batch_usage = submit_and_collect(client, requests, label="adjudicate")
    stats["usage"] = {"model": model, "batch": batch_usage}

    for i, r in enumerate(positives):
        md = r["meta_data"]
        pass1_reason = md.get("reasons", {}).get(cat, "")
        rec = {
            "url": r["url"],
            "pass1_score": md.get("scores", {}).get(cat),
            "pass1_reason": pass1_reason,
        }
        verdict = parse_verdict(raw.get(str(i), ""))

        if verdict is None:
            stats["n_unadjudicated"] += 1
            md["reasons"][cat] = (
                "[pass-2 adjudication unavailable — kept as pass-1 labelled it] "
                + pass1_reason
            )
            audit.append({**rec, "pass2_keep": True, "exclusion_hit": "unadjudicated",
                          "pass2_reason": "adjudication unavailable"})
            continue

        note = verdict["verdict_reason"]
        if r["url"] in truncated:
            mw, orig = truncated[r["url"]]
            note = f"[body truncated to {mw} words from {orig}] " + note
        md["labels"][cat] = verdict["about_slurry"]
        md["reasons"][cat] = note
        if verdict["about_slurry"]:
            stats["n_kept"] += 1
        else:
            stats["n_dropped"] += 1
        audit.append({**rec, "pass2_keep": verdict["about_slurry"],
                      "exclusion_hit": verdict["exclusion_hit"], "pass2_reason": note})

    return results, stats, audit
