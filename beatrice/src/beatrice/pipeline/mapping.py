"""Committed mapping: Susan run output -> beatrice GuidanceProposition.

Susan emits ``{url, meta_data: {propositions: [{proposition_text, actor,
regulatory_kind, source_paragraphs, ...}]}}``. The group-rerank matcher reads
``proposition_text`` / ``source_url`` and the ``regulatory_kind`` tag; the rest
are mapped to the nearest GuidanceProposition field so nothing is lost.
"""

from __future__ import annotations

import hashlib

from beatrice.guidance import GuidanceProposition


def _slug(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def guidance_from_susan_entry(entry: dict) -> list[GuidanceProposition]:
    url = entry["url"]
    md = entry.get("meta_data", {}) or {}
    out: list[GuidanceProposition] = []
    for i, sp in enumerate(md.get("propositions", []) or [], 1):
        paras = sp.get("source_paragraphs") or []
        if isinstance(paras, str):
            paras = [paras]
        out.append(
            GuidanceProposition(
                id=f"susan-{_slug(url)}-{i}",
                section_locator="",
                proposition_text=sp["proposition_text"],
                legal_subject=sp.get("actor") or "you",
                conditions=[],
                required_documents=[],
                source_url=url,
                extraction_method="susan",
                source_paragraphs=paras,
                regulatory_kind=sp.get("regulatory_kind") or "",
            )
        )
    return out


def guidance_from_susan_output(entries: list[dict]) -> list[GuidanceProposition]:
    """Flatten a whole Susan ``output.json`` into GuidanceProposition objects."""
    out: list[GuidanceProposition] = []
    for entry in entries:
        out.extend(guidance_from_susan_entry(entry))
    return out
