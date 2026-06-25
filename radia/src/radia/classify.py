r"""Radia's production recipe: word-search routing + Claude Haiku batch.

Trimmed to the single strategy Radia bakes in:

    page body --hits inclusion lexicon?--> no  --> recorded FALSE, no LLM call
                                       \-> yes --> Claude Haiku 4.5 (batch API)

No Ollama, no tf-idf, no all-categories prompt: the alternatives that were
trialled are out of scope here. The prompt is the slurry description with the
Exclusion Criteria section removed — word-search already does the exclusion.

No silent fallbacks anywhere:
- A page the lexicon routes away is recorded FALSE with an explicit reason.
- A page the batch drops (too large) is rescued via the immediate Messages API
  with its body truncated, and the truncation is stamped into the reason.
- A page that cannot be classified even after rescue is collected and raised
  loudly — never quietly defaulted to FALSE.
"""
from __future__ import annotations

import json
import os

import anthropic
from anthropic.types.messages.batch_create_params import Request

from ._batch import submit_and_collect, text_of
from .narrowing import select_categories

DEFAULT_MODEL = os.getenv("LLM_MODEL_ANTHROPIC", "claude-haiku-4-5-20251001")
MAX_TOKENS = 512


class ClassificationIncomplete(RuntimeError):
    """Raised when one or more routed pages could not be honestly classified."""


def build_prompt(title: str, body_text: str, categories: list[dict]) -> str:
    category_list = "\n".join(f"- {c['name']}: {c['description']}" for c in categories)
    title_line = f"Page title: {title}\n" if title else ""
    return (
        "You are classifying web page content against DEFRA policy categories.\n\n"
        "Task:\n"
        "For each category, determine whether the page contains any discussion, "
        "mention or other content related to that category.\n\n"
        "Count both direct references and clearly related concepts. A category "
        "does not need to be the primary focus of the page to be considered "
        "relevant.\n\n"
        f"Categories:\n{category_list}\n\n"
        f"{title_line}\n"
        f"Page content:\n{body_text}\n\n"
        "Scoring guidance:\n"
        "- 0.0: No evidence of relevance\n"
        "- 0.1–0.3: Mentioned briefly or indirectly\n"
        "- 0.4–0.6: Discussed to a moderate extent\n"
        "- 0.7–0.8: Clearly relevant and discussed in detail\n"
        "- 0.9–1.0: Major focus of the page\n\n"
        "Return ONLY a valid JSON object.\n\n"
        "For each category return:\n"
        "{\n"
        '  "<category>": true|false,\n'
        '  "<category>_score": float,\n'
        '  "<category>_reason": "1-2 sentence explanation"\n'
        "}\n\n"
        'Set "<category>" to true whenever there is any meaningful evidence that '
        "the category is mentioned or discussed, even briefly.\n"
        'Set "<category>" to false only when there is no evidence of relevance.'
    )


def _repair_truncated_json(text: str) -> str:
    """Close a JSON object cut off mid-string or mid-value."""
    if not text.startswith("{"):
        return text
    in_string = False
    i = 0
    while i < len(text):
        c = text[i]
        if c == "\\" and in_string:
            i += 2
            continue
        if c == '"':
            in_string = not in_string
        i += 1
    if in_string:
        text += '"'
    if not text.rstrip().endswith("}"):
        text = text.rstrip().rstrip(",") + "}"
    return text


def parse_response(
    text: str, categories: list[dict]
) -> tuple[dict[str, bool], dict[str, float], dict[str, str]] | None:
    """Parse a model reply into (labels, scores, reasons), or None if unparseable."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
        text = text.rsplit("```", 1)[0].strip()
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        try:
            parsed = json.loads(_repair_truncated_json(text))
        except (json.JSONDecodeError, ValueError):
            return None
    labels = {c["name"]: bool(parsed.get(c["name"], False)) for c in categories}
    scores = {
        c["name"]: float(parsed.get(f"{c['name']}_score", 1.0 if labels[c["name"]] else 0.0))
        for c in categories
    }
    reasons = {
        c["name"]: parsed.get(f"{c['name']}_reason", parsed.get("reason", ""))
        for c in categories
    }
    return labels, scores, reasons


def _meta(
    item: dict,
    labels: dict[str, bool],
    scores: dict[str, float],
    reasons: dict[str, str],
    sel_meta: dict,
    selected_names: list[str],
    shard_name: str | None,
) -> dict:
    """Build the output meta_data for a page (drops body_text)."""
    base = {k: v for k, v in item["meta_data"].items() if k != "body_text"}
    meta = {
        **base,
        "labels": labels,
        "scores": scores,
        "reasons": reasons,
        "selection_strategy_body": "wordsearch_binary",
        "selected_categories_body": selected_names,
        "selection_meta_body": sel_meta,
    }
    if shard_name:
        meta["source_shard"] = shard_name
    return {**item, "meta_data": meta}


def _excluded_meta(
    item: dict, categories: list[dict], sel_meta: dict, shard_name: str | None
) -> dict:
    names = [c["name"] for c in categories]
    return _meta(
        item,
        labels={n: False for n in names},
        scores={n: 0.0 for n in names},
        reasons={
            n: "excluded by wordsearch_binary (no cats selected; LLM not called)"
            for n in names
        },
        sel_meta=sel_meta,
        selected_names=[],
        shard_name=shard_name,
    )


def classify(
    items: list[dict],
    categories: list[dict],
    lexicons: dict[str, list[str]],
    *,
    model: str = DEFAULT_MODEL,
    max_words: int = 60000,
    shard_name: str | None = None,
) -> tuple[list[dict], dict]:
    """Classify every page with a body. Returns (results, stats).

    Word-search routes pages; lexicon hits go to the Anthropic batch API, the
    rest are recorded FALSE. Batch-dropped pages are rescued one-by-one with a
    truncated body. Raises ClassificationIncomplete if any routed page is still
    unclassified after rescue.
    """
    items = [it for it in items if it["meta_data"].get("body_text", "").strip()]

    routed: list[dict] = []
    routed_meta: dict[str, dict] = {}
    results: dict[str, dict] = {}
    for it in items:
        sel_cats, sel_meta = select_categories(
            it["meta_data"]["body_text"], categories, lexicons
        )
        if sel_cats:
            routed.append(it)
            routed_meta[it["url"]] = sel_meta
        else:
            results[it["url"]] = _excluded_meta(it, categories, sel_meta, shard_name)

    stats = {
        "n_pages": len(items),
        "n_routed": len(routed),
        "n_skipped": len(items) - len(routed),
        "n_rescued": 0,
        "model": model,
    }

    client = anthropic.Anthropic()

    raw: dict[str, str] = {}
    batch_usage = {"input": 0, "output": 0}
    rescue_usage = {"input": 0, "output": 0}
    if routed:
        requests: list[Request] = [
            {
                "custom_id": str(i),
                "params": {
                    "model": model,
                    "max_tokens": MAX_TOKENS,
                    "messages": [
                        {
                            "role": "user",
                            "content": build_prompt(
                                it["meta_data"].get("title", ""),
                                it["meta_data"]["body_text"],
                                categories,
                            ),
                        }
                    ],
                },
            }
            for i, it in enumerate(routed)
        ]
        raw, batch_usage = submit_and_collect(client, requests, label="classify")

    unclassified: list[dict] = []
    for i, it in enumerate(routed):
        sel_meta = routed_meta[it["url"]]
        selected_names = [c["name"] for c in categories]
        text = raw.get(str(i), "")
        parsed = parse_response(text, categories) if text else None

        if parsed is None:
            # Batch dropped or unparseable — rescue with a truncated body.
            rescued = _rescue(client, it, categories, model, max_words)
            if rescued is None:
                unclassified.append(it)
                continue
            stats["n_rescued"] += 1
            labels, scores, reasons, sel_meta, ru = rescued
            rescue_usage["input"] += ru["input"]
            rescue_usage["output"] += ru["output"]
        else:
            labels, scores, reasons = parsed

        results[it["url"]] = _meta(
            it, labels, scores, reasons, sel_meta, selected_names, shard_name
        )

    if unclassified:
        urls = "\n".join(f"  - {it['url']}" for it in unclassified)
        raise ClassificationIncomplete(
            f"{len(unclassified)} routed page(s) could not be classified even "
            f"after rescue — refusing to default them to FALSE:\n{urls}"
        )

    stats["usage"] = {"model": model, "batch": batch_usage, "rescue": rescue_usage}

    ordered = [results[it["url"]] for it in items]
    return ordered, stats


def _rescue(
    client: anthropic.Anthropic,
    item: dict,
    categories: list[dict],
    model: str,
    max_words: int,
) -> tuple[dict[str, bool], dict[str, float], dict[str, str], dict, dict[str, int]] | None:
    """Re-classify one batch-dropped page with a truncated body. None on failure.

    Only an Anthropic API error counts as a recoverable failure (the page ends up
    in ``ClassificationIncomplete``, never a silent FALSE). Any other exception is
    a bug and propagates loudly rather than being mistaken for "unclassifiable".
    """
    body = item["meta_data"]["body_text"]
    words = body.split()
    orig_n = len(words)
    truncated = orig_n > max_words
    if truncated:
        body = " ".join(words[:max_words])
    prompt = build_prompt(item["meta_data"].get("title", ""), body, categories)
    try:
        resp = client.messages.create(
            model=model, max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.APIError:
        return None
    parsed = parse_response(text_of(resp), categories)
    if parsed is None:
        return None
    labels, scores, reasons = parsed
    if truncated:
        for c in categories:
            reasons[c["name"]] = (
                f"[body truncated to {max_words} words from {orig_n}] "
                + reasons[c["name"]]
            )
    sel_meta = {
        "strategy": "rescue_oversized",
        "max_words": max_words,
        "original_words": orig_n,
    }
    usage = {"input": resp.usage.input_tokens, "output": resp.usage.output_tokens}
    return labels, scores, reasons, sel_meta, usage
