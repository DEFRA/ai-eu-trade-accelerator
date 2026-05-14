"""
Extract propositions from GOV.UK guidance text fragments.

Fragments come from ``adapter.parse_content_api_response()``.
Two strategies are tried in order:

1. LLM extraction (when ``llm_client`` is provided) — preferred for guidance prose.
2. Heuristic extraction — fallback using normative trigger-word matching.
"""

import json
import re
from hashlib import sha256
from typing import Any

from .models import GuidanceProposition


_TRIGGER_WORDS = (
    "must",
    "shall",
    "required",
    "prohibited",
    "you need to",
    "ensure",
)


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "item"


def _prop_id(locator: str, index: int) -> str:
    key = f"{locator}:{index}"
    digest = sha256(key.encode()).hexdigest()[:8]
    return f"guidance-{_slugify(locator)}-{digest}"


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences, treating newlines as sentence boundaries too."""
    parts: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Further split on sentence-ending punctuation
        segments = re.split(r"(?<=[.!?])\s+(?=[A-Z])", line)
        parts.extend(s.strip() for s in segments if s.strip())
    return parts


def _looks_normative(sentence: str) -> bool:
    lowered = sentence.lower()
    return any(trigger in lowered for trigger in _TRIGGER_WORDS)


def _guess_subject(text: str) -> str:
    lowered = text.lower()
    for candidate in ("importer", "operator", "keeper", "you", "authority"):
        if candidate in lowered:
            return candidate
    return "you"


def _guess_action(text: str) -> str:
    """Extract the modal + first verb phrase."""
    match = re.search(
        r"\b(must not|must|shall not|shall|need to|are required to|ensure)\b\s+(\w+)",
        text,
        re.IGNORECASE,
    )
    if match:
        return f"{match.group(1).lower()} {match.group(2).lower()}"
    return "must comply"


def _guess_conditions(text: str) -> list[str]:
    conditions: list[str] = []
    for marker in ("if ", "where ", "when ", "unless ", "before ", "after ", "provided that "):
        idx = text.lower().find(marker)
        if idx != -1:
            # Take up to the next comma or end of clause
            snippet = text[idx : idx + 80].split(",")[0].strip()
            conditions.append(snippet)
    return conditions


def _guess_required_documents(text: str) -> list[str]:
    docs: list[str] = []
    lowered = text.lower()
    for doc in ("health certificate", "passport", "certificate", "licence", "permit", "document", "notification"):
        if doc in lowered:
            docs.append(doc)
    return docs


def _heuristic_extraction(
    fragments: list[tuple[str, str]],
    source_url: str,
    limit: int,
) -> list[GuidanceProposition]:
    results: list[GuidanceProposition] = []
    for locator, text in fragments:
        sentences = _split_sentences(text)
        for sentence in sentences:
            if not _looks_normative(sentence):
                continue
            idx = len(results) + 1
            results.append(
                GuidanceProposition(
                    id=_prop_id(locator, idx),
                    section_locator=locator,
                    proposition_text=sentence,
                    legal_subject=_guess_subject(sentence),
                    action=_guess_action(sentence),
                    conditions=_guess_conditions(sentence),
                    required_documents=_guess_required_documents(sentence),
                    source_url=source_url,
                    extraction_method="heuristic",
                    source_paragraphs=[sentence],
                )
            )
            if len(results) >= limit:
                return results
    return results


def _try_llm_extraction(
    fragments: list[tuple[str, str]],
    topic: str,
    jurisdiction: str,
    source_url: str,
    llm_client: Any,
) -> list[GuidanceProposition]:
    combined_text = "\n\n".join(
        f"[{locator}]\n{text}" for locator, text in fragments
    )

    prompt = f"""
Extract legal propositions from this GOV.UK guidance text.

The guidance is addressed to importers/operators ("you"). Extract each distinct
requirement or obligation as a separate proposition.

For source_paragraphs: list each individual paragraph or list item (verbatim)
from the guidance that this proposition is drawn from. Each entry must be a
single <p> or <li> text — never combine a paragraph intro with its bullet
points. If a <p> introduces a <ul>, include the <p> text as one entry and each
relevant <li> as a separate entry.

Return JSON only with this shape:
{{
  "propositions": [
    {{
      "section_locator": "section:<id>",
      "source_paragraphs": ["verbatim paragraph or list-item 1", "verbatim paragraph or list-item 2"],
      "proposition_text": "string",
      "legal_subject": "string (e.g. 'importer', 'you', 'operator')",
      "action": "string (e.g. 'must ensure', 'must notify')",
      "conditions": ["string"],
      "required_documents": ["string"]
    }}
  ]
}}

Topic: {topic}
Jurisdiction: {jurisdiction}
Source: {source_url}

Guidance text:
{combined_text}
""".strip()

    try:
        raw = llm_client.guidance_extract_text(prompt)
        data = json.loads(raw[raw.find("{") : raw.rfind("}") + 1])
    except Exception:
        return []

    results: list[GuidanceProposition] = []
    for idx, item in enumerate(data.get("propositions", []), start=1):
        locator = item.get("section_locator", "section:unknown")
        results.append(
            GuidanceProposition(
                id=_prop_id(locator, idx),
                section_locator=locator,
                proposition_text=item["proposition_text"],
                legal_subject=item.get("legal_subject", "you"),
                action=item.get("action", "must comply"),
                conditions=item.get("conditions", []),
                required_documents=item.get("required_documents", []),
                source_url=source_url,
                extraction_method="llm",
                source_paragraphs=item.get("source_paragraphs", []),
            )
        )
    return results


def extract_propositions(
    fragments: list[tuple[str, str]],
    topic: str,
    source_url: str,
    jurisdiction: str = "UK",
    llm_client: Any = None,
    limit: int = 5,
) -> list[GuidanceProposition]:
    """
    Extract propositions from GOV.UK guidance text fragments.

    Args:
        fragments: ``(locator, text)`` pairs from ``parse_content_api_response()``.
        topic: Human-readable topic label (used in LLM prompt).
        source_url: The GOV.UK guidance URL (recorded on each proposition).
        jurisdiction: Defaults to ``"UK"``.
        llm_client: Optional LLM client (e.g. ``BeatriceLLMClient``). When provided,
            LLM extraction is attempted first and heuristics used as fallback.
        limit: Maximum number of propositions to return.

    Returns:
        List of ``GuidanceProposition`` objects.
    """
    if llm_client:
        results = _try_llm_extraction(
            fragments=fragments,
            topic=topic,
            jurisdiction=jurisdiction,
            source_url=source_url,
            llm_client=llm_client,
        )
        if results:
            return results[:limit]

    return _heuristic_extraction(fragments=fragments, source_url=source_url, limit=limit)
