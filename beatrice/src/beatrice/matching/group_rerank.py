"""Group-rerank matcher: one LLM call per guidance proposition.

Given a guidance proposition and the survivor set of law candidates that cleared
retrieval, a single LLM call labels every candidate's relationship at once. The
model compares candidates against each other and declines to anchor when nothing
fits — so it returns mostly UNGROUNDED (kept for audit) plus the one or two
substantive matches.

Results are cached on disk keyed by the SHA256 of the rendered prompt, so a
re-run with the same survivors + prompt is free.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from beatrice.guidance import GuidanceProposition

from .models import GuidanceMatch
from .prompts import (
    LABEL_PRIORITY,
    build_group_rerank_prompt,
    parse_group_rerank_json,
)

if TYPE_CHECKING:
    from beatrice.llm import BeatriceLLMClient

# ── Prompt-level group-rerank cache ───────────────────────────────────────────

_CACHE_PATH = Path(
    os.getenv("GROUP_RERANK_CACHE_PATH", "/tmp/beatrice/group-rerank-cache.json")
)
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()


def _load_cache() -> None:
    if not _CACHE_PATH.exists():
        return
    try:
        _cache.update(json.loads(_CACHE_PATH.read_text()))
    except Exception:
        pass


def _save_cache() -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CACHE_PATH.write_text(json.dumps(_cache))


_load_cache()

# ─────────────────────────────────────────────────────────────────────────────


def _matches_from_verdict(
    guidance_id: str,
    verdict: dict,
    sim_by_id: dict[str, float],
) -> list[GuidanceMatch]:
    """Map a parsed group-rerank verdict onto GuidanceMatch objects, normalising
    ids (the LLM sometimes drops the ``prop:`` prefix) and sorting useful labels
    first."""
    id_by_suffix = {lid.removeprefix("prop:"): lid for lid in sim_by_id}
    matches: list[GuidanceMatch] = []
    for m in verdict.get("matches", []) or []:
        raw_id = str(m.get("law_id", "")).strip()
        canonical = (
            raw_id
            if raw_id in sim_by_id
            else id_by_suffix.get(raw_id.removeprefix("prop:"), raw_id)
        )
        matches.append(
            GuidanceMatch(
                guidance_proposition_id=guidance_id,
                law_proposition_id=canonical,
                relationship=m["relationship"],
                confidence=m["confidence"],
                explanation=m["explanation"],
                similarity_score=round(float(sim_by_id.get(canonical, 0.0)), 4),
                correctness_score=m["correctness_score"],
            )
        )
    matches.sort(key=lambda gm: LABEL_PRIORITY.get(gm.relationship, 99))
    return matches


def group_rerank_match(
    guidance_prop: GuidanceProposition,
    candidates: list[tuple[Any, float]],
    llm_client: "BeatriceLLMClient",
    *,
    render_conf_sidebar: bool = True,
) -> list[GuidanceMatch]:
    """Classify every surviving law candidate for one guidance proposition in a
    single LLM call. ``candidates`` is a list of ``(law_proposition, score)``
    ordered by descending similarity. Returns an empty list when there are no
    candidates (the caller never pays for an LLM call in that case)."""
    if not candidates:
        return []

    prompt = build_group_rerank_prompt(
        guidance_prop, candidates, render_conf_sidebar=render_conf_sidebar
    )
    sim_by_id = {law.id: score for law, score in candidates}
    cache_key = hashlib.sha256(prompt.encode()).hexdigest()

    with _cache_lock:
        cached = _cache.get(cache_key)

    if cached is not None:
        return _matches_from_verdict(guidance_prop.id, cached, sim_by_id)

    raw = llm_client.guidance_classify_text(prompt)
    verdict = parse_group_rerank_json(raw)

    with _cache_lock:
        _cache[cache_key] = verdict
        _save_cache()

    return _matches_from_verdict(guidance_prop.id, verdict, sim_by_id)
