"""Tag every proposition in a corpus with one topic id from the closed taxonomy
(``data/topic_taxonomy.json``).

This produces the ``topic`` tags the group-rerank prompt keys off on both sides.
Tags name the regulatory THING a proposition is about, independent of whether
it's a law clause or a guidance statement — so the same tag fires on a statutory
ceiling and a grant requirement about the same subject.

One Haiku tool-use call per proposition (constrained to the taxonomy enum). Tags
are cached on disk keyed by proposition id; re-runs skip cached ids. The output
cache is a ``{id: topic}`` JSON map consumed by ``beatrice run`` via
``--guidance-topics`` / ``--law-topics``.

Usage:
    uv run beatrice tag-topics guidance susan-output.json --out guidance-topics.json
    uv run beatrice tag-topics law judit-propositions.json --out law-topics.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from importlib.resources import files
from pathlib import Path

from beatrice.domain import Proposition

from .mapping import guidance_from_susan_output

DEFAULT_MODEL = "claude-haiku-4-5-20251001"


def _taxonomy() -> dict:
    return json.loads((files("beatrice.pipeline.data") / "topic_taxonomy.json").read_text())


def _taxonomy_block(taxonomy: dict) -> str:
    lines = []
    for tag in taxonomy["tags"]:
        lines.append(f"- {tag['id']}: {tag['description']}")
        for ex in tag.get("examples", [])[:2]:
            lines.append(f"    e.g. {ex}")
    return "\n".join(lines)


def _prompt(prop_text: str, taxonomy_block: str) -> str:
    return f"""Tag the following proposition with ONE topic id from the closed vocabulary below.

Pick the single best fit. The topic should name the regulatory THING the
proposition is about, independent of whether it's a law clause, a guidance
statement, a definition, a duty, or a fact. A 4-month statutory storage
ceiling and a 6-month grant storage requirement share the same topic
(slurry_storage_capacity) — they are ABOUT the same thing.

If multiple topics seem to apply, pick the one that captures the
DOMINANT regulatory subject. Use 'other' only when no taxonomy tag fits.

Taxonomy:
{taxonomy_block}

Proposition:
{prop_text}

Call the `assign_topic` tool with the single best topic id."""


def _tool(tag_ids: list[str]) -> dict:
    return {
        "name": "assign_topic",
        "description": "Assign exactly one topic id from the closed taxonomy.",
        "input_schema": {
            "type": "object",
            "properties": {"topic_id": {"type": "string", "enum": tag_ids}},
            "required": ["topic_id"],
        },
    }


def _call(client, model: str, prompt: str, tool: dict) -> str:
    response = client.messages.create(
        model=model,
        max_tokens=128,
        tools=[tool],
        tool_choice={"type": "tool", "name": "assign_topic"},
        messages=[{"role": "user", "content": prompt}],
    )
    tool_block = next((b for b in response.content if getattr(b, "type", None) == "tool_use"), None)
    if tool_block is None:
        raise RuntimeError(f"no tool call. stop_reason={response.stop_reason!r}")
    topic = tool_block.input.get("topic_id")
    if not isinstance(topic, str):
        raise RuntimeError(f"tool call missing 'topic_id'. Got: {tool_block.input!r}")
    return topic


def _load_props(side: str, path: Path):
    raw = json.loads(path.read_text())
    if side == "guidance":
        return guidance_from_susan_output(raw if isinstance(raw, list) else raw.get("pages", []))
    records = raw if isinstance(raw, list) else raw.get("propositions", [])
    return [Proposition.model_validate(r) for r in records]


def main() -> None:
    ap = argparse.ArgumentParser(description="Tag guidance/law propositions with a closed-vocab topic.")
    ap.add_argument("side", choices=["guidance", "law"])
    ap.add_argument("input", type=Path, help="Susan output.json (guidance) or Judit propositions.json (law)")
    ap.add_argument("--out", type=Path, required=True, help="Output {id: topic} cache JSON")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--limit", type=int, default=None, help="Cap props tagged this run (for testing).")
    args = ap.parse_args()

    taxonomy = _taxonomy()
    tag_ids = [t["id"] for t in taxonomy["tags"]]
    tool = _tool(tag_ids)
    tax_block = _taxonomy_block(taxonomy)

    cache: dict[str, str] = json.loads(args.out.read_text()) if args.out.exists() else {}
    props = _load_props(args.side, args.input)
    print(f"corpus: {len(props)} {args.side} props | cached: {len(cache)}", flush=True)

    import anthropic
    client = anthropic.Anthropic()

    to_do = [p for p in props if p.id not in cache]
    if args.limit:
        to_do = to_do[: args.limit]
    print(f"to tag: {len(to_do)}", flush=True)
    t0 = time.time()
    for i, p in enumerate(to_do, 1):
        try:
            cache[p.id] = _call(client, args.model, _prompt(p.proposition_text, tax_block), tool)
        except Exception as e:
            print(f"  [{i}/{len(to_do)}] {p.id}: FAILED {type(e).__name__}: {e}", file=sys.stderr)
            continue
        if i % 25 == 0 or i == len(to_do):
            args.out.write_text(json.dumps(cache, indent=2, sort_keys=True))
            rate = i / (time.time() - t0) if time.time() > t0 else 0
            print(f"  [{i}/{len(to_do)}] {p.id} -> {cache[p.id]}  ({rate:.1f} props/s)", flush=True)

    args.out.write_text(json.dumps(cache, indent=2, sort_keys=True))
    print(f"\ndone. {len(cache)} total tags -> {args.out}")
    print("topic distribution:")
    for tag, n in Counter(cache.values()).most_common():
        print(f"  {tag}: {n} ({100 * n / len(cache):.1f}%)" if cache else f"  {tag}: {n}")


if __name__ == "__main__":
    main()
