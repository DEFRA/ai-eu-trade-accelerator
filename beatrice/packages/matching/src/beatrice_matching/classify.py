import hashlib
import json
import os
import re
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from beatrice_domain import Proposition
from beatrice_guidance import GuidanceProposition

from .models import GuidanceMatch, RELATIONSHIP_TYPES

if TYPE_CHECKING:
    from beatrice_llm import BeatriceLLMClient

# ── Prompt-level classify cache ───────────────────────────────────────────────

_CLASSIFY_CACHE_PATH = Path(
    os.getenv("CLASSIFY_CACHE_PATH", "/tmp/beatrice/classify-cache.json")
)
_classify_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()


def _load_classify_cache() -> None:
    if not _CLASSIFY_CACHE_PATH.exists():
        return
    try:
        _classify_cache.update(json.loads(_CLASSIFY_CACHE_PATH.read_text()))
    except Exception:
        pass


def _save_classify_cache() -> None:
    _CLASSIFY_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CLASSIFY_CACHE_PATH.write_text(json.dumps(_classify_cache))


_load_classify_cache()

# ─────────────────────────────────────────────────────────────────────────────


def classify_match(
    guidance_prop: GuidanceProposition,
    law_prop: Proposition,
    similarity_score: float,
    llm_client: "BeatriceLLMClient",
    bert_score_f1: float = 0.0,
) -> GuidanceMatch:
    """
    Use the LLM to classify the relationship between a guidance proposition
    and a candidate law proposition.
    """
    prompt = f"""
You are assessing whether a GOV.UK guidance proposition accurately reflects a law proposition.

Classify the relationship using exactly one of these values:
- confirmed   : guidance accurately reflects the law
- outdated    : same intent but the law has since changed
- guidance omits detail               : guidance only covers part of what the law requires
- guidance contains additional detail : guidance adds requirements not stated in the law
- contradicts                         : guidance says the opposite of or conflicts with the law
- does not match                      : the guidance has no basis in this law proposition

Also provide a correctness_score (0.0–1.0) reflecting how correctly the guidance represents the law:
- 1.0 : confirmed
- 0.0 : does not match, contradicts, or outdated
- 0.0–1.0 : guidance omits detail or guidance contains additional detail — score the proportion of the law covered by the guidance (omits detail) or the proportion of the guidance grounded in the law (additional detail)

Return JSON only:
{{
  "relationship": "<one of the six values above>",
  "confidence": "<high|medium|low>",
  "explanation": "<2-3 sentences explaining your reasoning>",
  "correctness_score": <number between 0.0 and 1.0>
}}

Semantic similarity score (0-1): {similarity_score:.3f}

GUIDANCE PROPOSITION
Source: {guidance_prop.source_url}
Section: {guidance_prop.section_locator}
Text: {guidance_prop.proposition_text}

LAW PROPOSITION
Jurisdiction: {law_prop.jurisdiction}
Citation: {law_prop.article_reference or "—"}
Text: {law_prop.proposition_text}
""".strip()

    cache_key = hashlib.sha256(prompt.encode()).hexdigest()

    with _cache_lock:
        cached = _classify_cache.get(cache_key)

    if cached is not None:
        print(f"  [classify] cache hit for {guidance_prop.id} / {law_prop.id}", file=__import__("sys").stderr)
        relationship = cached.get("relationship", "does not match")
        if relationship not in RELATIONSHIP_TYPES:
            relationship = "does not match"
        return GuidanceMatch(
            guidance_proposition_id=guidance_prop.id,
            law_proposition_id=law_prop.id,
            relationship=relationship,
            confidence=cached.get("confidence", "low"),
            explanation=cached.get("explanation", ""),
            similarity_score=similarity_score,
            correctness_score=float(cached.get("correctness_score", 0.0)),
            bert_score_f1=bert_score_f1,
            classify_cached=True,
        )

    try:
        print(f"  [classify] LLM call for {guidance_prop.id} / {law_prop.id}", file=__import__("sys").stderr)
        raw = llm_client.guidance_classify_text(prompt)
        data = _parse_json(raw)
        relationship = data.get("relationship", "does not match")
        if relationship not in RELATIONSHIP_TYPES:
            relationship = "does not match"

        with _cache_lock:
            _classify_cache[cache_key] = data
            _save_classify_cache()

        return GuidanceMatch(
            guidance_proposition_id=guidance_prop.id,
            law_proposition_id=law_prop.id,
            relationship=relationship,
            confidence=data.get("confidence", "low"),
            explanation=data.get("explanation", ""),
            similarity_score=similarity_score,
            correctness_score=float(data.get("correctness_score", 0.0)),
            bert_score_f1=bert_score_f1,
        )
    except Exception as exc:
        print(f"  Warning: classification failed: {exc}", file=__import__("sys").stderr)
        return GuidanceMatch(
            guidance_proposition_id=guidance_prop.id,
            law_proposition_id=law_prop.id,
            relationship="does not match",
            confidence="low",
            explanation=f"Classification failed: {exc}",
            similarity_score=similarity_score,
            bert_score_f1=bert_score_f1,
        )


def _parse_json(raw: str) -> Any:
    text = raw.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    # Extract first JSON object
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        text = match.group(0)
    return json.loads(text)
