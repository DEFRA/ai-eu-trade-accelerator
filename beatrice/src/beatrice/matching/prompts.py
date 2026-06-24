"""Shared prompt construction for the group-rerank matcher and summarisation.

Single source of truth for prompt text: the batch matching runner
(`beatrice.pipeline.batch_match`) and the in-process matcher
(`beatrice.matching.match`) both build their prompts here, so the two paths
cannot drift apart.

The matcher is *group-rerank*: a retrieval step surfaces a small survivor set of
candidate law propositions, then a SINGLE LLM call sees the guidance proposition
alongside all survivors and labels each one. This lets the model compare
candidates against each other and decline to anchor when nothing fits — the
failure mode that killed the older per-pair classifier.
"""

from __future__ import annotations

import json
from typing import Any

from .models import RELATIONSHIP_TYPES

SUMMARISE_SYSTEM = "You summarise legal compliance analysis concisely and clearly."

# The group-rerank user prompt carries the full instructions, so the system
# prompt only needs to set the role.
GROUP_RERANK_SYSTEM = (
    "You check whether GOV.UK guidance accurately reflects UK legal propositions, "
    "in precise, audit-friendly language."
)

# Useful labels first, UNGROUNDED last. A stable sort on this keeps the LLM's own
# within-bucket ordering while pushing UNGROUNDED matches (kept for audit) down.
LABEL_PRIORITY = {
    "GROUNDED": 0,
    "GUIDANCE_BROADER": 1,
    "CONFLICTS": 2,
    "GUIDANCE_INCOMPLETE": 3,
    "GUIDANCE_MISSING": 4,
    "UNGROUNDED": 5,
}


def law_citation(law: Any) -> str:
    """Human-readable citation. Judit's current export leaves article_reference
    null and carries the locator in fragment_locator instead."""
    if isinstance(law, dict):
        return law.get("article_reference") or law.get("fragment_locator") or "—"
    return getattr(law, "article_reference", None) or getattr(law, "fragment_locator", None) or "—"


GROUP_RERANK_PROMPT = """You are checking whether GOV.UK guidance accurately reflects UK legal propositions.

A retrieval step has surfaced the candidate law propositions below. They may not all be the right basis. Be willing to mark several as UNGROUNDED and anchor the guidance on at most one or two of them.

Inline tags carry structural metadata produced by upstream typing passes:

- The guidance line is prefixed with `[topic: <Y>] [regulatory_kind: <Z>]`. `regulatory_kind` is one of: grant_condition, statutory_obligation, factual_statement, or unknown when Susan's typing did not assign one.
- Each candidate law line is prefixed with `[<prop_id>]  [clause_function: <X>]  [topic: <Y>]  (<citation>)`. `clause_function` is one of: definition, operative_obligation, ceiling, floor, scope_clause, procedure, delegation_of_power, transitional, exception, or unknown when the typing cache did not cover that candidate.
- `topic` on both sides is drawn from a shared closed vocabulary; the literal value `unknown` means no topic was assigned.

When a tag reads `unknown`, the paired rules below do not fire — fall back to reading the candidate on its merits using the general guidance at the bottom of this section. Do NOT invent a type; treat unknown as missing information.

Typed pair rules (apply the first that matches; rules earlier in the list take precedence):

1. guidance.regulatory_kind = factual_statement, any law clause → UNGROUNDED. Factual or background guidance is not asserting a regulatory rule and cannot be anchored on a law clause.
2. guidance.topic ≠ law.topic, and neither value is `unknown` nor `other` → strong prior toward UNGROUNDED. Same-topic is a precondition for any non-UNGROUNDED label; only override if the law clause is plainly the substantive basis on a re-read.
3. guidance.regulatory_kind = grant_condition, law.clause_function = ceiling, guidance exceeds the ceiling → GUIDANCE_BROADER, NOT CONFLICTS. A ceiling caps what the regulator may DEMAND of the operator; a grant scheme asking for more raises a higher bar than the statutory minimum without contradicting the ceiling.
4. guidance.regulatory_kind = statutory_obligation, law.clause_function = definition → UNGROUNDED for that candidate. The operative obligation that USES the defined term is the better anchor; the definition itself is not. Exception: the guidance is restating the definition verbatim.
5. guidance.regulatory_kind = statutory_obligation, law.clause_function = operative_obligation, topics match → check substantive agreement and label GROUNDED, GUIDANCE_INCOMPLETE, GUIDANCE_BROADER, GUIDANCE_MISSING, or CONFLICTS based on how the guidance treats the duty. This is the natural anchor.
6. law.clause_function = scope_clause → UNGROUNDED unless the guidance is about THAT SAME instrument's scope (e.g. restating the same Act's territorial reach). Two scope clauses from different instruments (different Acts, different licences, different schemes) describe parallel territorial reach; they CANNOT CONFLICTS because each is true within its own instrument — they describe what their respective documents cover, not contradicting each other. Even when both are tagged `topic = scope_commencement`, two instruments' scope clauses are parallel facts, not contradictory ones.
7. any guidance, law.clause_function is one of `exception`, `transitional`, or `delegation_of_power` → prefer UNGROUNDED unless the guidance is specifically about the carve-out, the transition period, or the conferred power.

General drafting-role behaviour (use when no paired rule above fires, e.g. tags are `unknown`):

- Law typed as floor: guidance that meets the floor is GROUNDED; guidance that exceeds it is GUIDANCE_BROADER; guidance that falls short is CONFLICTS or GUIDANCE_INCOMPLETE depending on whether it positively asserts a weaker rule or merely omits the stronger one.
- Law typed as procedure: GROUNDED if the guidance describes the same procedural step; UNGROUNDED if the guidance is about substantive obligation rather than procedure.
- Cross-framework matches (guidance from one statutory framework, law from an unrelated framework) that share only surface vocabulary: prefer UNGROUNDED over CONFLICTS.

Reserve CONFLICTS for cases where both the guidance and the law sit within the same regulatory framework AND substantively oppose each other on the same point.

Self-check before emitting the JSON: for each match, does the relationship label you are about to emit actually match the typed rule you invoked in your explanation? If your explanation argues for one label (for example "GUIDANCE_BROADER under the ceiling rule"), emit that label. Do not write reasoning that justifies one label and then emit a different one.

Choose one label per candidate:
- GROUNDED: guidance accurately reflects this law.
- UNGROUNDED: this law is not the right basis for the guidance.
- CONFLICTS: guidance contradicts this law. Use sparingly — only when both are within the same regulatory framework and substantively oppose each other.
- GUIDANCE_INCOMPLETE: guidance covers only part of what this law requires.
- GUIDANCE_BROADER: guidance adds requirements beyond what this law states, or raises a higher bar than the statutory minimum without contradicting it (e.g. a grant scheme exceeding the statutory floor or sitting above a statutory ceiling on demanded capacity).
- GUIDANCE_MISSING: the law states a requirement the guidance touches but skips.

GUIDANCE
{guidance_text}

CANDIDATE LAW PROPOSITIONS
{candidate_block}

Return JSON only:
{
  "best_anchor_id": "<the law_id you think is the right basis, or null if none>",
  "anchor_rationale": "<1-2 sentences explaining why you picked that anchor, or why none>",
  "matches": [
    {
      "law_id": "<exact id from the candidates above>",
      "relationship": "<one of the six labels>",
      "confidence": "<high|medium|low>",
      "explanation": "<1-2 sentences>",
      "correctness_score": <float 0.0-1.0>
    }
  ]
}
"""


# Same prompt with an extra bullet describing the per-candidate `--- sidebar ---`
# block (Judit's extraction-quality flags). Built by string replacement so the
# two prompts cannot drift on the shared body.
GROUP_RERANK_CONF_SIDEBAR_PROMPT = GROUP_RERANK_PROMPT.replace(
    (
        "- Each candidate law line is prefixed with `[<prop_id>]  "
        "[clause_function: <X>]  [topic: <Y>]  (<citation>)`. "
        "`clause_function` is one of: definition, operative_obligation, "
        "ceiling, floor, scope_clause, procedure, delegation_of_power, "
        "transitional, exception, or unknown when the typing cache did not "
        "cover that candidate."
    ),
    (
        "- Each candidate law line is prefixed with `[<prop_id>]  "
        "[clause_function: <X>]  [topic: <Y>]  (<citation>)`. "
        "`clause_function` is one of: definition, operative_obligation, "
        "ceiling, floor, scope_clause, procedure, delegation_of_power, "
        "transitional, exception, or unknown when the typing cache did not "
        "cover that candidate.\n"
        "- Some candidate laws are followed by an indented sidebar block "
        "delimited by `--- sidebar ---` lines carrying Judit's extraction "
        "quality flags for that record: `model_confidence` (high / medium "
        "/ low — how confident Judit's upstream extractor was in this "
        "proposition); `completeness_status` (complete / partial / "
        "context_dependent — whether the extracted text stands on its own "
        "or needs surrounding context to be fully meaningful); "
        "`fallback_policy` (fail_closed / mark_needs_review — what Judit "
        "would do if extraction had failed for this record); and "
        "`fallback_used` (only present when true — meaning the record was "
        "derived through a fallback path rather than the primary extractor "
        "and is therefore less trustworthy). Treat the sidebar as a soft "
        "signal alongside the law text — a low-confidence or "
        "fallback-derived candidate is not automatically wrong, but where "
        "two candidates would otherwise be close, the higher-confidence "
        "one is the more reliable anchor. Absent fields were not recorded "
        "for that candidate; do not infer them."
    ),
)


def _cf_tag(law: Any) -> str:
    return f"[clause_function: {getattr(law, 'clause_function', '') or 'unknown'}]"


def _topic_tag(law: Any) -> str:
    return f"[topic: {getattr(law, 'topic', '') or 'unknown'}]"


def _conf_sidebar(law: Any) -> str:
    """Per-candidate confidence sidebar from Judit's extraction-quality flags.
    Skips absent fields; only surfaces ``fallback_used`` when truthy. Returns ""
    so the caller can omit the sidebar entirely — no silent fallback values."""
    lines: list[str] = []
    model_confidence = getattr(law, "model_confidence", None)
    completeness_status = getattr(law, "completeness_status", None)
    fallback_policy = getattr(law, "fallback_policy", None)
    fallback_used = getattr(law, "fallback_used", None)
    if model_confidence is not None and str(model_confidence).strip():
        lines.append(f"    model_confidence: {str(model_confidence).strip()}")
    if completeness_status is not None and str(completeness_status).strip():
        lines.append(f"    completeness_status: {str(completeness_status).strip()}")
    if fallback_policy is not None and str(fallback_policy).strip():
        lines.append(f"    fallback_policy: {str(fallback_policy).strip()}")
    if fallback_used is True:
        lines.append("    fallback_used: true")
    if not lines:
        return ""
    return "    --- sidebar ---\n" + "\n".join(lines) + "\n    --- /sidebar ---"


def build_group_rerank_prompt(
    guidance_prop,
    candidates: list[tuple[Any, float]],
    *,
    render_conf_sidebar: bool = False,
) -> str:
    """Build the single group-rerank prompt for one guidance proposition and its
    surviving law candidates.

    ``candidates`` is a list of ``(law_proposition, similarity_score)`` already
    ordered by descending similarity. The inline ``[clause_function: …]`` /
    ``[topic: …]`` tags degrade to ``unknown`` when the upstream typing/tagging
    caches did not cover a record, so the prompt's typed rules degrade
    gracefully without changing the surface format they pattern-match on.
    """
    if render_conf_sidebar:
        lines = []
        for law, _ in candidates:
            header = (
                f"[{law.id}]  {_cf_tag(law)}  {_topic_tag(law)}  "
                f"({law_citation(law)})  {law.proposition_text}"
            )
            sidebar = _conf_sidebar(law)
            lines.append(header + "\n" + sidebar if sidebar else header)
        candidate_block = "\n".join(lines)
        template = GROUP_RERANK_CONF_SIDEBAR_PROMPT
    else:
        candidate_block = "\n".join(
            f"[{law.id}]  {_cf_tag(law)}  {_topic_tag(law)}  "
            f"({law_citation(law)})  {law.proposition_text}"
            for law, _ in candidates
        )
        template = GROUP_RERANK_PROMPT

    topic = getattr(guidance_prop, "topic", "") or "unknown"
    regulatory_kind = getattr(guidance_prop, "regulatory_kind", "") or "unknown"
    guidance_block = (
        f"[topic: {topic}] [regulatory_kind: {regulatory_kind}]\n"
        f"{guidance_prop.proposition_text}"
    )
    # Plain .replace, not .format — the JSON template in the prompt body uses
    # literal { } braces.
    return template.replace("{guidance_text}", guidance_block).replace(
        "{candidate_block}", candidate_block
    )


def parse_group_rerank_json(raw: str) -> dict:
    """Parse a group-rerank response. Returns ``{"matches": [...]}`` with each
    match's relationship normalised to RELATIONSHIP_TYPES (fallback UNGROUNDED).
    Raises ValueError when no JSON object is present — the caller decides how to
    handle a failed proposition; we never silently fabricate matches."""
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError("no JSON object in group-rerank response")
    data = json.loads(raw[start:end])
    out_matches = []
    for m in data.get("matches", []) or []:
        relationship = str(m.get("relationship", "UNGROUNDED"))
        if relationship not in RELATIONSHIP_TYPES:
            relationship = "UNGROUNDED"
        out_matches.append(
            {
                "law_id": str(m.get("law_id", "")).strip(),
                "relationship": relationship,
                "confidence": str(m.get("confidence", "low")),
                "explanation": str(m.get("explanation", ""))[:500],
                "correctness_score": float(m.get("correctness_score", 0.0)),
            }
        )
    return {
        "best_anchor_id": data.get("best_anchor_id"),
        "anchor_rationale": str(data.get("anchor_rationale", "")),
        "matches": out_matches,
    }


def build_summarise_prompt(guidance_prop, classified_matches: list[dict]) -> str:
    """Prompt asking the model to summarise how a guidance proposition aligns
    with its classified law matches. Each classified match is a dict carrying
    ``relationship``, ``explanation`` and a ``law_proposition`` dict."""
    matches_text = "\n\n".join(
        f"- Relationship: {m.get('relationship')}\n"
        f"  Law: {m.get('law_proposition', {}).get('proposition_text', '')}\n"
        f"  Citation: {law_citation(m.get('law_proposition', {}))}\n"
        f"  Explanation: {m.get('explanation', '')}"
        for m in classified_matches
    )
    return f"""You are summarising how a piece of GOV.UK guidance relates to relevant law propositions.

GUIDANCE PROPOSITION
Source: {guidance_prop.source_url}
Section: {guidance_prop.section_locator}
Text: {guidance_prop.proposition_text}

RELEVANT LAW MATCHES
{matches_text if matches_text else "No relevant law matches found."}

Write a concise 2-3 sentence summary (maximum 100 words) of how the guidance aligns with (or diverges from) the relevant law. Focus on practical implications for compliance."""
