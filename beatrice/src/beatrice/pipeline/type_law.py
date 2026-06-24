"""Classify each law proposition into one of nine legal-drafting functions
(``clause_function``) — the law-side tag the group-rerank typed rules key off.

The categories come from legal-drafting literature, not from any application's
use-case: definition, operative_obligation, ceiling, floor, scope_clause,
procedure, delegation_of_power, transitional, exception.

One Haiku call per law proposition. Results are cached on disk keyed by law id;
re-runs skip cached ids. The output cache is a
``{law_id: {clause_function, rationale}}`` JSON map consumed by ``beatrice run``
via ``--clause-functions``.

Usage:
    uv run beatrice type-law judit-propositions.json --out clause-functions.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

from beatrice.domain import Proposition

from beatrice.matching.prompts import law_citation

DEFAULT_MODEL = "claude-haiku-4-5-20251001"

CLAUSE_FUNCTIONS = (
    "definition", "operative_obligation", "ceiling", "floor", "scope_clause",
    "procedure", "delegation_of_power", "transitional", "exception",
)

TYPING_PROMPT = """You are classifying a single clause from UK legislation into one of nine drafting-function categories. Read the clause and pick the SINGLE dominant function it serves in the statute or statutory instrument.

The categories are drawn from legal-drafting literature, not from any application's use-case. Pick exactly one:

- definition: the clause defines a term used elsewhere in the regulation. Look for phrases like "means", "is defined as", "for the purposes of regulation X", "the X period is".
- operative_obligation: the clause imposes a positive duty on someone. Look for "must", "shall", "is required to", "the operator shall".
- ceiling: the clause caps what the regulator may demand from the operator. Common shape: "the X need not have greater capacity than Y", "no requirement under this regulation exceeds Z". A maximum the regulator can demand, not a maximum the operator may meet.
- floor: the clause sets a minimum standard the operator must meet. The natural opposite of ceiling: "at least X", "not less than Y", a minimum the operator must reach.
- scope_clause: the clause describes which territory or persons the regulation applies to. "These regulations extend to England and Wales", "applies in England only", "applies to holdings within a nitrate vulnerable zone".
- procedure: the clause specifies how to do something procedurally. The form to submit, the body to notify, the manner in which a thing is done.
- delegation_of_power: the clause confers a power on a body, Minister, or named person. "The Secretary of State may by regulations", "the Agency may direct".
- transitional: an interim, savings, or commencement provision. "Anything done under the 2008 regulations is treated as done under these", commencement dates, sunset clauses.
- exception: a carve-out from a general rule. "This regulation does not apply to X", "except where Y", "save that Z".

If a clause hybridises (e.g. definition plus exception), pick the DOMINANT function: the one a legal draftsperson would say the clause is for.

CLAUSE
Citation: {citation}
Text: {proposition_text}

Return JSON only, exactly this shape:
{{
  "clause_function": "<one of the nine values above>",
  "rationale": "<one short sentence explaining which features of the clause drove the choice>"
}}
"""


def _parse_json_obj(raw: str) -> dict:
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError("no JSON object in typing response")
    return json.loads(raw[start:end])


def _type_one(client, model: str, law: Proposition) -> dict:
    prompt = TYPING_PROMPT.format(citation=law_citation(law), proposition_text=law.proposition_text)
    response = client.messages.create(
        model=model,
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = next((b.text for b in response.content if getattr(b, "type", None) == "text"), "")
    data = _parse_json_obj(raw)
    cf = str(data.get("clause_function", "")).strip()
    if cf not in CLAUSE_FUNCTIONS:
        raise ValueError(f"unexpected clause_function {cf!r}")
    return {"clause_function": cf, "rationale": str(data.get("rationale", ""))[:300]}


def main() -> None:
    ap = argparse.ArgumentParser(description="Type law propositions by drafting function (clause_function).")
    ap.add_argument("law", type=Path, help="Judit propositions.json")
    ap.add_argument("--out", type=Path, required=True, help="Output {law_id: {clause_function, rationale}} cache JSON")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--limit", type=int, default=None, help="Cap props typed this run (for testing).")
    args = ap.parse_args()

    raw = json.loads(args.law.read_text())
    records = raw if isinstance(raw, list) else raw.get("propositions", [])
    law_props = [Proposition.model_validate(r) for r in records]

    cache: dict[str, dict] = json.loads(args.out.read_text()) if args.out.exists() else {}
    print(f"corpus: {len(law_props)} law props | cached: {len(cache)}", flush=True)

    import anthropic
    client = anthropic.Anthropic()

    to_do = [p for p in law_props if p.id not in cache]
    if args.limit:
        to_do = to_do[: args.limit]
    print(f"to type: {len(to_do)}", flush=True)
    t0 = time.time()
    for i, p in enumerate(to_do, 1):
        try:
            cache[p.id] = _type_one(client, args.model, p)
        except Exception as e:
            print(f"  [{i}/{len(to_do)}] {p.id}: FAILED {type(e).__name__}: {e}", file=sys.stderr)
            continue
        if i % 25 == 0 or i == len(to_do):
            args.out.write_text(json.dumps(cache, indent=2, sort_keys=True))
            rate = i / (time.time() - t0) if time.time() > t0 else 0
            print(f"  [{i}/{len(to_do)}] {p.id} -> {cache[p.id]['clause_function']}  ({rate:.1f} props/s)", flush=True)

    args.out.write_text(json.dumps(cache, indent=2, sort_keys=True))
    print(f"\ndone. {len(cache)} total types -> {args.out}")
    print("clause_function distribution:")
    for cf, n in Counter(v["clause_function"] for v in cache.values()).most_common():
        print(f"  {cf}: {n} ({100 * n / len(cache):.1f}%)" if cache else f"  {cf}: {n}")


if __name__ == "__main__":
    main()
