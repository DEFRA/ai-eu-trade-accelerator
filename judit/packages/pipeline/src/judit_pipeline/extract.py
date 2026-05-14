from __future__ import annotations

import json
import math
import re
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from judit_domain import Cluster, Proposition, ReviewStatus, SourceRecord, Topic
from judit_llm import JuditLLMClient

from .derived_cache import DerivedArtifactCache, build_proposition_extraction_chunk_cache_key
from .intake import content_hash, slugify


def _proposition_id_stem(source: SourceRecord) -> str:
    stem = slugify(source.id)
    if isinstance(source.metadata, dict):
        fid = source.metadata.get("extraction_fragment_id")
        if fid:
            stem = f"{stem}-{slugify(str(fid))}"
    return stem


_TRIGGER_WORDS = ("must", "shall", "may", "required", "prohibited", "must not", "shall not")

# Structured list rows prepend this header before JSON extraction metadata on ``notes``.
STRUCTURED_NOTE_PREFIX = "heuristic extraction:structured_list"

STRUCTURED_LIST_RULE_ID = "extract.heuristic.structured_list_items"

EXTRACTION_SCHEMA_VERSION_V2 = "v2"
EXTRACTION_PROMPT_VERSION_V2 = "v2"
JUDIT_EXTRACTION_META_PREFIX = "judit_extraction_meta:"

JUDIT_EXTRACTION_REUSE_PREFIX = "judit_extraction_reuse:"

_PROVISION_TYPES_V2 = frozenset(
    {"core", "definition", "exception", "transitional", "cross_reference"}
)
_COMPLETENESS_V2 = frozenset({"complete", "context_dependent", "fragmentary"})

_MIN_PRIMARY_SIBLINGS_FOR_LIST_MODE = 2

_V2_SYSTEM_PROMPT = "You extract legal propositions as strict JSON only."


def estimate_llm_input_tokens(*, prompt: str, system_prompt: str | None = None) -> int:
    """Conservative token estimate for context-window budgeting (over-estimates vs typical ~4 chars/token)."""
    joined = (prompt or "") + "\n" + (system_prompt or "")
    return max(1, math.ceil(len(joined) / 3.0))


_RE_ARTICLE_HEADING_LINE = re.compile(
    r"(?m)^(Article\s+\d+[a-z]?\b[^\n]*)$",
    re.IGNORECASE,
)
_RE_SECTION_HEADING_LINE = re.compile(
    r"(?m)^(Section\s+\d+[.:]?\s*[^\n]*)$",
    re.IGNORECASE,
)


@dataclass
class ExtractSourceResult:
    propositions: list[Proposition]
    extraction_mode: str
    model_alias: str | None
    fallback_policy: str
    fallback_used: bool
    validation_errors: list[str]
    prompt_version: str
    schema_version: str
    failed_closed: bool = False
    failure_reason: str | None = None
    validation_issue_records: list[dict[str, Any]] = field(default_factory=list)
    extraction_llm_call_traces: list[dict[str, Any]] = field(default_factory=list)
    repairable_extraction_halt: bool = False
    repairable_extraction_halt_reason: str | None = None
    fallback_strategy: str | None = None


def _looks_like_infra_llm_failure(message: str) -> bool:
    """Heuristic infra/provider failures — used for ``stop_repairable`` extraction policy."""
    blob = message.lower()
    if not blob.strip():
        return False
    keys = (
        "credit",
        "quota",
        "429",
        "rate limit",
        "ratelimit",
        "context length",
        "context window",
        "token limit",
        "max_tokens",
        "too large",
        "model",
        "overloaded",
        "unavailable",
        "api_error",
        "api error",
    )
    return any(k in blob for k in keys)


# Primary EU-style letter subparagraphs excluding single-letter "i" (roman (i)-(iv)).
_RE_PRIMARY_LETTER_BOL = re.compile(r"^\(([a-z])\)\s*(.*)$", re.IGNORECASE)
_RE_ROMAN_BOL = re.compile(r"^\(([ivxlcdm]+)\)\s*(.*)$", re.IGNORECASE)
_RE_NUMBER_PARA_BOL = re.compile(r"^(\d+)\.\s+(.*)$")


def extract_propositions(
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    llm_client: JuditLLMClient | None = None,
    limit: int = 3,
) -> list[Proposition]:
    """Legacy helper for unit tests and demos.

    Production / CLI runs use :func:`extract_propositions_from_source` (v2 schema, frontier/local/heuristic).
    """
    if llm_client and source.authoritative_text.strip():
        llm_result = _legacy_try_llm_extraction_pre_v2(
            source=source,
            topic=topic,
            cluster=cluster,
            llm_client=llm_client,
        )
        if llm_result:
            return llm_result[:limit]

    return _heuristic_extraction(source=source, topic=topic, cluster=cluster, limit=limit)


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.split()).strip()


_RE_DEFINITION_HEADER = re.compile(
    r"\b(?:for\s+the\s+purposes\s+of\s+this\s+(?:regulation|directive|decision|act)[^.:;\n]{0,200}\b)?"
    r"the\s+following\s+definitions\s+shall\s+apply\b",
    re.IGNORECASE,
)
_RE_QUOTED_TERM = re.compile(r"[\"'‘’“”]([^\"'‘’“”]{1,180})[\"'‘’“”]")
_RE_DEFINITION_ENTRY = re.compile(
    r"(?is)(?P<term>"
    r"(?:[\"'‘’“”][^\"'‘’“”]{1,180}[\"'‘’“”](?:\s+or\s+[\"'‘’“”][^\"'‘’“”]{1,180}[\"'‘’“”]){0,3})"
    r")\s+(?P<verb>means|shall\s+mean)\b"
)
_PRIORITY_DEFINITION_TERMS: tuple[str, ...] = (
    "equidae",
    "equine animal",
    "holding",
    "keeper",
    "owner",
    "registered equidae",
    "equidae for breeding and production",
    "equidae for slaughter",
    "competent authority",
    "transponder",
    "unique life number",
    "mark",
    "smart card",
    "veterinarian responsible",
    "official veterinarian",
    "appropriate authority",
)


def _normalize_definition_term_key(term: str) -> str:
    lowered = term.strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return " ".join(lowered.split())


def _is_definition_provision(text: str) -> bool:
    body = text.strip()
    if not body:
        return False
    if _RE_DEFINITION_HEADER.search(body):
        return True
    if re.search(r"(?i)\bshall\s+mean\b", body):
        return True
    if re.search(r"(?i)\bmeans\b", body) and _RE_QUOTED_TERM.search(body):
        return True
    return False


def _definition_term_display(term_blob: str) -> str:
    terms = [m.group(1).strip() for m in _RE_QUOTED_TERM.finditer(term_blob) if m.group(1).strip()]
    if not terms:
        return _normalize_whitespace(term_blob.strip(" ,.;:"))
    uniq: list[str] = []
    seen: set[str] = set()
    for t in terms:
        key = _normalize_definition_term_key(t)
        if key and key not in seen:
            seen.add(key)
            uniq.append(t)
    if not uniq:
        return _normalize_whitespace(term_blob.strip(" ,.;:"))
    if len(uniq) == 1:
        return uniq[0]
    return " / ".join(uniq)


def _definition_priority_rank(term_display: str) -> int:
    key = _normalize_definition_term_key(term_display)
    if not key:
        return len(_PRIORITY_DEFINITION_TERMS) + 100
    for idx, target in enumerate(_PRIORITY_DEFINITION_TERMS):
        tkey = _normalize_definition_term_key(target)
        if not tkey:
            continue
        if tkey in key or key in tkey:
            return idx
    return len(_PRIORITY_DEFINITION_TERMS) + 50


def _definition_fallback_extraction(
    *,
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    limit: int,
) -> tuple[list[Proposition], list[dict[str, Any]], list[str]]:
    text = source.authoritative_text or ""
    if not _is_definition_provision(text):
        return [], [], []
    matches = list(_RE_DEFINITION_ENTRY.finditer(text))
    if not matches:
        return [], [], []

    article_reference = _extract_reference_from_locator(source.authoritative_locator) or _extract_article_reference(
        text
    )
    if not article_reference:
        article_reference = "Article 2"
    m_art = re.match(r"(?i)^article\s+(\d+[a-z]?)$", article_reference.strip())
    if m_art:
        article_reference = f"Article {m_art.group(1)}"

    candidates: list[tuple[int, int, Proposition, dict[str, Any], str]] = []
    for idx, match in enumerate(matches):
        start = match.start("term")
        end = matches[idx + 1].start("term") if idx + 1 < len(matches) else len(text)
        span_raw = text[start:end]
        span = span_raw.strip()
        if not span:
            continue

        term_display = _definition_term_display(match.group("term"))
        if not term_display:
            continue
        verb = _normalize_whitespace(match.group("verb").lower())
        local_verb_index = span.lower().find(verb)
        if local_verb_index == -1:
            continue
        definition_tail = span[local_verb_index + len(verb) :].strip(" \n\t:;.")
        if not definition_tail:
            continue

        evidence_text = span
        ok, strategy, _diag = evidence_locates_verbatim_after_normalisation(evidence_text, text)
        confidence: Literal["high", "medium", "low"]
        if ok:
            confidence = "high"
        else:
            confidence = "medium"
        review_status = ReviewStatus.PROPOSED if confidence == "high" else ReviewStatus.NEEDS_REVIEW
        label = f"Definition — “{term_display}”"
        proposition_text = f"{term_display} means {definition_tail}"
        priority = _definition_priority_rank(term_display)

        proposition = Proposition(
            id="",
            proposition_key="",
            topic_id=topic.id,
            cluster_id=cluster.id,
            source_record_id=source.id,
            source_fragment_id=None,
            fragment_locator=source.authoritative_locator,
            source_snapshot_id=source.current_snapshot_id,
            jurisdiction=source.jurisdiction,
            article_reference=article_reference,
            proposition_text=proposition_text,
            label=label,
            short_name=label[:200],
            legal_subject=term_display,
            action="means" if "mean" in verb else "defines",
            conditions=[],
            authority=_guess_authority(definition_tail),
            required_documents=_guess_required_documents(definition_tail),
            affected_subjects=[term_display],
            notes="deterministic definition fallback extraction",
            review_status=review_status,
        )
        extra_meta: dict[str, Any] = {
            "provision_type": "definition",
            "completeness_status": "complete",
            "model_confidence": confidence,
            "evidence_quote": evidence_text,
            "evidence_match_strategy": strategy,
            "trace_warnings": [] if confidence == "high" else ["definition_evidence_requires_review"],
        }
        candidates.append((priority, idx, proposition, extra_meta, confidence))

    candidates.sort(key=lambda item: (item[0], item[1]))
    picked = candidates[: max(1, limit)]
    out_props: list[Proposition] = []
    out_meta: list[dict[str, Any]] = []
    warnings: list[str] = []
    slug_base = slugify(source.id.strip())
    for seq, (_prio, _idx, prop, meta, confidence) in enumerate(picked, start=1):
        term_slug = slugify(prop.legal_subject) or f"definition-{seq:03d}"
        prop.id = f"prop-{_proposition_id_stem(source)}-{seq:03d}"
        prop.proposition_key = f"{slug_base}:{term_slug}:p{seq:03d}"
        out_props.append(prop)
        out_meta.append(meta)
        if confidence != "high":
            warnings.append(f"definition_fallback_medium_confidence:{prop.legal_subject}")
    return out_props, out_meta, warnings


def _is_placeholder_locator(locator: str | None) -> bool:
    if locator is None:
        return True
    return locator.strip().lower() in {"", "document:full", "full", "document"}


def _extract_source_fragments(source: SourceRecord) -> list[tuple[str, str]]:
    lines = [line.strip() for line in source.authoritative_text.splitlines() if line.strip()]
    if not lines and source.authoritative_text.strip():
        lines = [source.authoritative_text.strip()]
    metadata = source.metadata if isinstance(source.metadata, dict) else {}
    locators_raw = metadata.get("fragment_locators")
    locators = (
        [str(item) for item in locators_raw if str(item).strip()]
        if isinstance(locators_raw, list)
        else []
    )
    default_locator = source.authoritative_locator or "document:full"

    if not lines:
        return [(default_locator, "No source text provided.")]
    if len(locators) == len(lines):
        return list(zip(locators, lines, strict=False))
    if len(locators) == 1:
        # One locator for the whole instrument fragment (e.g. entire article) — keep full text together
        # so structured list / multi-sentence parsing sees list markers and continuations.
        merged = "\n".join(lines)
        return [(locators[0], merged)]
    if not locators:
        merged = "\n".join(lines)
        return [(default_locator, merged)]
    return [(default_locator, line) for line in lines]


def _split_by_article_or_section_headings(text: str, base_locator: str) -> list[tuple[str, str]] | None:
    """Split consolidated instruments on Article/Section headings when clearly delineated."""
    stripped = text.strip()
    if not stripped:
        return None
    matches = list(_RE_ARTICLE_HEADING_LINE.finditer(stripped))
    if len(matches) < 2:
        matches = list(_RE_SECTION_HEADING_LINE.finditer(stripped))
    if len(matches) < 2:
        return None
    parts: list[tuple[str, str]] = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(stripped)
        heading = m.group(1).strip()
        body = stripped[start:end].strip()
        if not body:
            continue
        slug = re.sub(r"\s+", "_", heading[:96])
        loc_base = base_locator if not _is_placeholder_locator(base_locator) else heading
        parts.append((f"{loc_base}|{slug}", body))
    return parts if len(parts) >= 2 else None


def _hard_split_text(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    t = text.strip()
    if not t:
        return []
    if len(t) <= max_chars:
        return [t]
    out: list[str] = []
    ov = max(0, overlap_chars)
    step = max(256, max_chars - ov) if max_chars > ov else max_chars
    i = 0
    while i < len(t):
        out.append(t[i : i + max_chars])
        i += step
    return out


def _split_paragraph_overlap_chunks(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    paras = [p.strip() for p in text.replace("\r\n", "\n").split("\n\n") if p.strip()]
    if not paras:
        return [text.strip()] if text.strip() else []
    chunks: list[str] = []
    current_parts: list[str] = []
    current_len = 0
    ov = max(0, overlap_chars)

    def flush() -> None:
        nonlocal current_parts, current_len
        if not current_parts:
            return
        piece = "\n\n".join(current_parts)
        chunks.append(piece)
        current_parts = []
        current_len = 0
        if ov > 0 and len(piece) > ov:
            tail = piece[-ov:].strip()
            if tail:
                current_parts.append(tail)
                current_len = len(tail)

    for p in paras:
        if len(p) > max_chars:
            flush()
            chunks.extend(_hard_split_text(p, max_chars, ov))
            continue
        sep_len = 2 if current_parts else 0
        projected = current_len + sep_len + len(p)
        if current_parts and projected > max_chars:
            flush()
            sep_len = 2 if current_parts else 0
            projected = current_len + sep_len + len(p)
        current_parts.append(p)
        current_len = projected

    flush()
    return chunks


@dataclass(frozen=True)
class _PlannedExtractChunk:
    locator: str
    text: str
    split_strategy: str


@dataclass(frozen=True)
class PlannedExtractionRequest:
    request_id: str
    source_record_id: str
    source_snapshot_id: str | None
    source_fragment_id: str | None
    source_fragment_locator: str | None
    fragment_locator: str
    source_title: str
    model_alias: str
    prompt_version: str
    schema_version: str
    prompt_text: str
    system_text: str | None
    estimated_input_tokens: int
    cache_key: str | None
    focus_scopes: list[str]
    max_propositions_per_source: int
    metadata: dict[str, Any]


def _plan_llm_text_chunks(
    source: SourceRecord,
    *,
    max_body_chars: int,
    overlap_chars: int,
) -> list[_PlannedExtractChunk]:
    overlap_chars = max(0, overlap_chars)
    max_body_chars = max(512, max_body_chars)
    planned: list[_PlannedExtractChunk] = []
    for locator, fragment_text in _extract_source_fragments(source):
        ft = fragment_text.strip()
        if not ft:
            continue
        base_loc = locator if locator.strip() else (source.authoritative_locator or "document:full")
        if len(ft) <= max_body_chars:
            planned.append(_PlannedExtractChunk(locator=base_loc, text=ft, split_strategy="metadata_fragment"))
            continue
        headed = _split_by_article_or_section_headings(ft, base_loc)
        if headed:
            for loc_h, body_h in headed:
                if len(body_h) <= max_body_chars:
                    planned.append(_PlannedExtractChunk(locator=loc_h, text=body_h, split_strategy="article_heading"))
                else:
                    for piece in _split_paragraph_overlap_chunks(body_h, max_body_chars, overlap_chars):
                        planned.append(
                            _PlannedExtractChunk(locator=loc_h, text=piece, split_strategy="paragraph_overlap")
                        )
            continue
        for piece in _split_paragraph_overlap_chunks(ft, max_body_chars, overlap_chars):
            planned.append(_PlannedExtractChunk(locator=base_loc, text=piece, split_strategy="paragraph_overlap"))
    return planned


def plan_frontier_extraction_requests(
    *,
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    model_alias: str,
    max_propositions: int,
    focus_scopes: Sequence[str] | None = None,
    prompt_version: str = EXTRACTION_PROMPT_VERSION_V2,
    schema_version: str = EXTRACTION_SCHEMA_VERSION_V2,
    max_input_tokens: int = 150_000,
    extract_model_context_limit: int = 200_000,
    derived_chunk_cache: DerivedArtifactCache | None = None,
    chunk_cache_pipeline_version: str = "0.1.0",
    chunk_cache_strategy_version: str = "v1",
    include_cached_successes: bool = False,
) -> list[PlannedExtractionRequest]:
    """Plan frontier extraction prompts without invoking the LLM."""
    full_body = source.authoritative_text.strip()
    if not full_body:
        return []
    overlap_chars = min(8192, max(512, int(max_input_tokens) // 40))
    initial_est = _estimate_extract_prompt_tokens(
        source=source,
        topic=topic,
        cluster=cluster,
        extraction_mode="frontier",
        max_propositions=max_propositions,
        focus_scopes=focus_scopes,
        prompt_source_text=full_body,
        fragment_locator_hint=None,
    )
    if initial_est <= max_input_tokens:
        planned_chunks = [
            _PlannedExtractChunk(
                locator=source.authoritative_locator or "document:full",
                text=full_body,
                split_strategy="single",
            )
        ]
    else:
        max_body_chars = _max_body_chars_for_extract_budget(
            source=source,
            topic=topic,
            cluster=cluster,
            extraction_mode="frontier",
            max_propositions=max_propositions,
            focus_scopes=focus_scopes,
            token_budget=max_input_tokens,
        )
        planned_chunks = _plan_llm_text_chunks(
            source,
            max_body_chars=max_body_chars,
            overlap_chars=overlap_chars,
        )
    scopes_tuple = tuple(sorted(str(s).strip() for s in (focus_scopes or ()) if str(s).strip()))
    frag_id = None
    if isinstance(source.metadata, dict) and source.metadata.get("extraction_fragment_id") is not None:
        frag_id = str(source.metadata.get("extraction_fragment_id"))
    source_snapshot_id = str(source.current_snapshot_id).strip() if source.current_snapshot_id else None
    out: list[PlannedExtractionRequest] = []
    for ci, planned in enumerate(planned_chunks, start=1):
        fitted = _fit_body_text_to_extract_budget(
            body=planned.text,
            source=source,
            topic=topic,
            cluster=cluster,
            extraction_mode="frontier",
            max_propositions=max_propositions,
            focus_scopes=focus_scopes,
            fragment_locator_hint=planned.locator,
            max_input_tokens=max_input_tokens,
        )
        if fitted is None:
            continue
        chunk_text, chunk_est = fitted
        prompt = _v2_model_prompt(
            source,
            topic,
            cluster,
            extraction_mode="frontier",
            max_propositions=max_propositions,
            focus_scopes=focus_scopes,
            prompt_source_text=chunk_text,
            fragment_locator_hint=planned.locator,
        )
        chunk_fp = content_hash(chunk_text)[:48]
        cache_key: str | None = None
        if model_alias:
            cache_key = build_proposition_extraction_chunk_cache_key(
                source_snapshot_id=source_snapshot_id,
                source_fragment_id=frag_id,
                    source_fragment_locator=planned.locator,
                chunk_index=ci,
                chunk_body_fingerprint=chunk_fp,
                model_alias=str(model_alias),
                extraction_mode="frontier",
                prompt_version=prompt_version,
                focus_scopes=scopes_tuple,
                max_propositions=max_propositions,
                pipeline_version=chunk_cache_pipeline_version,
                strategy_version=chunk_cache_strategy_version,
            )
        if cache_key and derived_chunk_cache is not None and not include_cached_successes:
            cached_hit = derived_chunk_cache.get(
                stage_name="proposition_extraction_chunk", cache_key=cache_key
            )
            if cached_hit is not None and isinstance(cached_hit.payload, dict):
                if cached_hit.payload.get("chunk_status") == "llm_success":
                    vr = cached_hit.payload.get("validated_rows")
                    if isinstance(vr, list) and vr:
                        continue
        request_id = (
            f"{source.id}:{frag_id or 'full'}:{ci}:{content_hash(prompt)[:16]}"
        )
        out.append(
            PlannedExtractionRequest(
                request_id=request_id,
                source_record_id=source.id,
                source_snapshot_id=source_snapshot_id,
                source_fragment_id=frag_id,
                source_fragment_locator=source.authoritative_locator,
                fragment_locator=planned.locator,
                source_title=source.title,
                model_alias=model_alias,
                prompt_version=prompt_version,
                schema_version=schema_version,
                prompt_text=prompt,
                system_text=_V2_SYSTEM_PROMPT,
                estimated_input_tokens=chunk_est,
                cache_key=cache_key,
                focus_scopes=list(scopes_tuple),
                max_propositions_per_source=max_propositions,
                metadata={
                    "extraction_mode": "frontier",
                    "configured_context_limit": int(extract_model_context_limit),
                    "max_extract_input_tokens": int(max_input_tokens),
                    "chunk_index": ci,
                    "chunk_total": len(planned_chunks),
                    "split_strategy": planned.split_strategy,
                    "chunk_body_fingerprint": chunk_fp,
                },
            )
        )
    return out


def _estimate_extract_prompt_tokens(
    *,
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    extraction_mode: Literal["frontier", "local"],
    max_propositions: int,
    focus_scopes: Sequence[str] | None,
    prompt_source_text: str,
    fragment_locator_hint: str | None,
) -> int:
    prompt = _v2_model_prompt(
        source,
        topic,
        cluster,
        extraction_mode=extraction_mode,
        max_propositions=max_propositions,
        focus_scopes=focus_scopes,
        prompt_source_text=prompt_source_text,
        fragment_locator_hint=fragment_locator_hint,
    )
    return estimate_llm_input_tokens(prompt=prompt, system_prompt=_V2_SYSTEM_PROMPT)


def _max_body_chars_for_extract_budget(
    *,
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    extraction_mode: Literal["frontier", "local"],
    max_propositions: int,
    focus_scopes: Sequence[str] | None,
    token_budget: int,
    safety_margin_tokens: int = 2048,
) -> int:
    """Derive max source-text characters per chunk from conservative token budget."""
    avail = max(256, token_budget - safety_margin_tokens)
    probe_body = "."
    overhead = _estimate_extract_prompt_tokens(
        source=source,
        topic=topic,
        cluster=cluster,
        extraction_mode=extraction_mode,
        max_propositions=max_propositions,
        focus_scopes=focus_scopes,
        prompt_source_text=probe_body,
        fragment_locator_hint=None,
    )
    body_token_budget = max(128, avail - overhead)
    # Prompt tokens scale ~len/3 for body; invert conservatively.
    return max(512, int(body_token_budget * 3))


def _merge_dedupe_validated_v2_rows(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for item in rows:
        ptxt = str(item.get("proposition_text") or "").strip()
        loc_raw = str(item.get("source_locator") or "").strip()
        key = (_normalize_proposition_comparison(ptxt), loc_raw.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
        if len(out) >= limit:
            break
    return out


def _fit_body_text_to_extract_budget(
    *,
    body: str,
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    extraction_mode: Literal["frontier", "local"],
    max_propositions: int,
    focus_scopes: Sequence[str] | None,
    fragment_locator_hint: str | None,
    max_input_tokens: int,
) -> tuple[str, int] | None:
    chunk_text = body.strip()
    if not chunk_text:
        return None
    while True:
        est = _estimate_extract_prompt_tokens(
            source=source,
            topic=topic,
            cluster=cluster,
            extraction_mode=extraction_mode,
            max_propositions=max_propositions,
            focus_scopes=focus_scopes,
            prompt_source_text=chunk_text,
            fragment_locator_hint=fragment_locator_hint,
        )
        if est <= max_input_tokens:
            return chunk_text, est
        if len(chunk_text) <= 400:
            return None
        chunk_text = chunk_text[: max(len(chunk_text) * 2 // 3, 400)]


def _split_fragment_sentences(fragment_text: str) -> list[str]:
    normalized = _normalize_whitespace(fragment_text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[.!?;])\s+(?=(?:\(?[a-z0-9ivx]+\)|[A-Z]))", normalized)
    cleaned = [_normalize_whitespace(part.strip(" ;")) for part in parts if part.strip(" ;")]
    return cleaned or [normalized]


def _build_sentence_candidates(source: SourceRecord) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    for locator, fragment_text in _extract_source_fragments(source):
        for sentence in _split_fragment_sentences(fragment_text):
            candidates.append((locator, sentence))
    return candidates


def _is_operative_locator(locator: str | None) -> bool:
    if not locator:
        return False
    lowered = locator.lower()
    if any(marker in lowered for marker in ("introduction", "preamble", "recital", "note")):
        return False
    return any(
        marker in lowered
        for marker in (
            "article",
            "section",
            "regulation",
            "paragraph",
            "schedule",
            "chapter",
            "part",
        )
    )


def _legacy_try_llm_extraction_pre_v2(
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    llm_client: JuditLLMClient,
) -> list[Proposition]:
    """DEPRECATED pre-v2 extraction prompt (ad-hoc JSON shape, ``local_extract_model`` only).

    Not used by ``extract_propositions_from_source`` / frontier or local quality extraction — those use
    :func:`_v2_model_prompt` and schema validation. Retained for :func:`extract_propositions` tests only.
    """
    prompt = f"""
Extract legal propositions from this source (at most a handful per call).

Return JSON only with this shape:
{{
  "propositions": [
    {{
      "article_reference": "optional string",
      "proposition_text": "string",
      "legal_subject": "string",
      "action": "string",
      "conditions": ["string"],
      "authority": "optional string",
      "required_documents": ["string"],
      "affected_subjects": ["string"],
      "notes": "optional string"
    }}
  ]
}}

Topic: {topic.name}
Cluster: {cluster.name}
Jurisdiction: {source.jurisdiction}
Citation: {source.citation}

Source text:
{source.authoritative_text}
""".strip()

    try:
        raw = llm_client.complete_text(
            prompt=prompt,
            model=llm_client.settings.local_extract_model,
            system_prompt="You extract legal propositions as strict JSON.",
            temperature=0.0,
        )
        parsed = _parse_json(raw)
    except Exception:
        return []

    propositions: list[Proposition] = []
    for index, item in enumerate(parsed.get("propositions", []), start=1):
        propositions.append(
            Proposition(
                id=f"prop-{_proposition_id_stem(source)}-{index:03d}",
                topic_id=topic.id,
                cluster_id=cluster.id,
                source_record_id=source.id,
                source_fragment_id=None,
                fragment_locator=source.authoritative_locator,
                source_snapshot_id=source.current_snapshot_id,
                jurisdiction=source.jurisdiction,
                article_reference=item.get("article_reference"),
                proposition_text=item["proposition_text"],
                legal_subject=item["legal_subject"],
                action=item["action"],
                conditions=item.get("conditions", []),
                authority=item.get("authority"),
                required_documents=item.get("required_documents", []),
                affected_subjects=item.get("affected_subjects", []),
                notes=item.get("notes", ""),
            )
        )
    return propositions


@dataclass(frozen=True)
class _StructuredDraft:
    marker_label: str
    path_key: str
    standalone_text: str
    parent_intro: str
    item_tail: str


def parse_structured_extraction_notes(notes: str | None) -> dict[str, Any] | None:
    """Parse JSON appended after STRUCTURED_NOTE_PREFIX in ``notes``."""
    if not notes or STRUCTURED_NOTE_PREFIX not in notes:
        return None
    idx = notes.find(STRUCTURED_NOTE_PREFIX)
    remainder = notes[idx + len(STRUCTURED_NOTE_PREFIX) :].lstrip("\n:")
    sep = remainder.find("{")
    if sep == -1:
        return None
    remainder = remainder[sep:]
    try:
        data = json.loads(remainder.strip())
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


def _normalize_proposition_comparison(text: str) -> str:
    return " ".join(text.lower().split())


def _primary_letter_bol_ok(line: str) -> tuple[str | None, str | None]:
    """
    Split ``(letter) …`` headings for primary lists; ``(i)`` ambiguous with roman `(i)`
    delegates to roman parser elsewhere (skip `'i'` as primary-letter).
    """
    m = _RE_PRIMARY_LETTER_BOL.match(line)
    if not m:
        return None, None
    inner = (m.group(1) or "").lower()
    remainder = (m.group(2) or "").strip()
    if inner == "i":
        return None, None
    return inner, remainder


def _paragraph_number_above(lines: list[str], first_primary_ix: int) -> str | None:
    """Return ``N`` from the last numbered paragraph opener ``N. …`` above the primary list."""
    para: str | None = None
    for i in range(0, first_primary_ix):
        m_par = _RE_NUMBER_PARA_BOL.match(lines[i])
        if m_par:
            para = str(int(m_par.group(1)))  # normalize "01" -> "1"
    return para


def _compose_standalone_intro_item(*, intro: str, marker: str, body: str) -> str:
    head = intro.strip().rstrip(":")
    body_norm = body.strip().strip(";")
    if head:
        return _normalize_whitespace(f"{head} {marker} {body_norm}")
    return _normalize_whitespace(f"{marker} {body_norm}")


def _draft_structured_subpropositions(fragment_text: str, base_locator: str) -> list[_StructuredDraft]:
    lines = [_normalize_whitespace(ln) for ln in fragment_text.splitlines() if ln.strip()]
    _ = base_locator
    if len(lines) < 3:
        return []

    primary_ix: list[tuple[int, str]] = []
    for i, ln in enumerate(lines):
        inner, _ = _primary_letter_bol_ok(ln)
        if inner is None:
            continue
        primary_ix.append((i, inner))

    if len(primary_ix) < _MIN_PRIMARY_SIBLINGS_FOR_LIST_MODE:
        return []

    first_primary = primary_ix[0][0]
    paragraph_n = _paragraph_number_above(lines, first_primary) or "1"
    parent_intro = _normalize_whitespace(" ".join(lines[0:first_primary]))

    drafts: list[_StructuredDraft] = []

    for k, (start_i, letter) in enumerate(primary_ix):
        end_i = primary_ix[k + 1][0] if k + 1 < len(primary_ix) else len(lines)
        segment_lines = lines[start_i:end_i]
        marker_label = f"({letter})"
        path_letter = f"{paragraph_n}-{letter.lower()}"

        # EUR-Lex / pilot layout: ``(d)`` on its own line, species text on following lines before ``(i)``.
        prelude_before_romans: list[str] = []
        first_roman_idx: int | None = None
        if len(segment_lines) > 1:
            for j in range(1, len(segment_lines)):
                if _RE_ROMAN_BOL.match(segment_lines[j]):
                    first_roman_idx = j
                    break
                prelude_before_romans.append(segment_lines[j])

        roman_rows: list[tuple[str, str]] = []
        if first_roman_idx is not None:
            for ln in segment_lines[first_roman_idx:]:
                mr = _RE_ROMAN_BOL.match(ln)
                if mr:
                    rlab = "(" + mr.group(1).lower() + ")"
                    roman_rows.append((rlab, mr.group(2).strip()))

        if roman_rows:
            first_line_body = ""
            mf = _RE_PRIMARY_LETTER_BOL.match(segment_lines[0])
            if mf:
                first_line_body = (mf.group(2) or "").strip()
            if prelude_before_romans:
                first_line_body = _normalize_whitespace(
                    f"{first_line_body} {' '.join(prelude_before_romans)}".strip()
                ).strip()
            for r_marker, r_body in roman_rows:
                roman_path = slugify(path_letter + "-" + r_marker.strip("()")).replace("__", "_")
                body = first_line_body
                combined_body = body
                if r_body:
                    combined_body = f"{body}: {r_body}" if body else r_body
                standalone = _compose_standalone_intro_item(
                    intro=parent_intro,
                    marker=f"{marker_label} / {r_marker}",
                    body=combined_body,
                )
                drafts.append(
                    _StructuredDraft(
                        marker_label=f"{marker_label}->{r_marker}",
                        path_key=f"{roman_path}",
                        standalone_text=standalone,
                        parent_intro=parent_intro,
                        item_tail=combined_body[:400],
                    )
                )

        if not roman_rows:
            mf_body = _RE_PRIMARY_LETTER_BOL.match(segment_lines[0])
            first_remainder = (mf_body.group(2) or "").strip() if mf_body else ""
            continuation = ""
            if len(segment_lines) > 1:
                continuation = " ".join(segment_lines[1:])
            letter_body_core = _normalize_whitespace(
                f"{first_remainder} {continuation}".strip()
            ).strip()

            standalone_letter = _compose_standalone_intro_item(
                intro=parent_intro,
                marker=marker_label,
                body=letter_body_core or " ".join(segment_lines),
            )
            drafts.append(
                _StructuredDraft(
                    marker_label=marker_label,
                    path_key=slugify(path_letter),
                    standalone_text=_normalize_whitespace(standalone_letter),
                    parent_intro=parent_intro,
                    item_tail=segment_lines[-1][:400],
                )
            )

    uniq: dict[str, _StructuredDraft] = {}
    out: list[_StructuredDraft] = []
    for d in drafts:
        key = _normalize_proposition_comparison(d.standalone_text)
        if key not in uniq:
            uniq[key] = d
            out.append(d)
    return out


def _structured_list_candidates_for_source(source: SourceRecord) -> list[tuple[str, _StructuredDraft]]:
    out: list[tuple[str, _StructuredDraft]] = []
    for locator, fragment in _extract_source_fragments(source):
        for draft in _draft_structured_subpropositions(fragment, locator):
            out.append((locator, draft))
    return out


def _structured_primary_bucket(marker_label: str) -> str:
    """Bucket for round-robin across list items: primary ``(a)`` / head before ``->``."""
    head = marker_label.split("->", 1)[0].strip()
    m_head = re.match(r"^\(([a-z])\)", head, re.IGNORECASE)
    return (m_head.group(1).lower() if m_head else head)[:16]


def _prioritize_structured_drafts(
    drafts: list[tuple[str, _StructuredDraft]],
    quota: int,
) -> list[tuple[str, _StructuredDraft]]:
    """
    When a list explodes into roman sub-items, taking ``drafts[:quota]`` only ever
    returns ``(a)``. Interleave primaries `(a)`, `(b)`, … so later letters (e.g. equine
    `(d)`) survive under small extraction limits.
    """
    if quota <= 0 or not drafts:
        return []
    if len(drafts) <= quota:
        return drafts
    letter_order: list[str] = []
    seen: set[str] = set()
    for _, draft in drafts:
        bkt = _structured_primary_bucket(draft.marker_label)
        if bkt not in seen:
            seen.add(bkt)
            letter_order.append(bkt)
    queues: dict[str, deque[tuple[str, _StructuredDraft]]] = {}
    for item in drafts:
        bkt = _structured_primary_bucket(item[1].marker_label)
        queues.setdefault(bkt, deque()).append(item)
    selected: list[tuple[str, _StructuredDraft]] = []
    while len(selected) < quota:
        progressed = False
        for bkt in letter_order:
            if len(selected) >= quota:
                break
            q = queues.get(bkt)
            if q:
                selected.append(q.popleft())
                progressed = True
        if not progressed:
            break
    return selected


def _structured_list_props(
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    *,
    quota: int,
) -> tuple[list[Proposition], list[str]]:
    if quota <= 0:
        return [], []
    drafts = _structured_list_candidates_for_source(source)
    picked = _prioritize_structured_drafts(drafts, quota)
    props: list[Proposition] = []
    norms: list[str] = []
    base_art = (
        source.authoritative_locator
        if not _is_placeholder_locator(source.authoritative_locator or "")
        else None
    ) or (_extract_article_reference(source.authoritative_text) or "article:unknown")

    for seq_num, (_, draft) in enumerate(picked, start=1):
        standalone = draft.standalone_text
        if not standalone.strip():
            continue

        evidence_loc_suffix = draft.path_key
        composite_loc = f"{base_art}:list:{evidence_loc_suffix}"
        fragment_locator = composite_loc[:512]
        article_reference = (
            _extract_article_reference(standalone)
            or _extract_reference_from_locator(fragment_locator)
            or draft.marker_label.strip("() ")
        )

        slug_base = slugify(source.id.strip())
        key_frag = slugify(evidence_loc_suffix.replace("__", "_"))
        proposition_key = f"{slug_base}:{key_frag}:p{seq_num:03d}"

        meta = {
            "parent_context": draft.parent_intro,
            "list_marker": draft.marker_label,
            "evidence_locator": fragment_locator,
            "structured_list_path": draft.path_key,
            "duplicate_suppression_hint": standalone[:160],
        }
        sentence = standalone
        legal_subject = _guess_subject(sentence)
        notes = STRUCTURED_NOTE_PREFIX + "\n" + json.dumps(meta, sort_keys=True)

        props.append(
            Proposition(
                id=f"prop-{_proposition_id_stem(source)}-{seq_num:03d}",
                proposition_key=proposition_key,
                topic_id=topic.id,
                cluster_id=cluster.id,
                source_record_id=source.id,
                source_fragment_id=None,
                fragment_locator=fragment_locator,
                source_snapshot_id=source.current_snapshot_id,
                jurisdiction=source.jurisdiction,
                article_reference=article_reference,
                proposition_text=sentence,
                legal_subject=legal_subject,
                action=_guess_action(sentence),
                conditions=_guess_conditions(sentence),
                authority=_guess_authority(sentence),
                required_documents=_guess_required_documents(sentence),
                affected_subjects=[legal_subject],
                notes=notes,
            )
        )
        norms.append(_normalize_proposition_comparison(sentence))

    intro_only = drafts[0][1].parent_intro.strip() if drafts else ""
    return props, [*norms, _normalize_proposition_comparison(intro_only)]


def _heuristic_sentence_extraction_filtered(
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    *,
    limit: int,
    exclude_norm_texts: set[str],
    id_seq_start: int,
) -> list[Proposition]:
    excludes = {_normalize_proposition_comparison(x) for x in exclude_norm_texts if x.strip()}
    sentence_candidates = _build_sentence_candidates(source)
    normative_candidates = [
        (locator, sentence)
        for locator, sentence in sentence_candidates
        if _looks_normative(sentence) and _normalize_proposition_comparison(sentence) not in excludes
    ]
    preferred_normative_candidates = [
        (locator, sentence)
        for locator, sentence in normative_candidates
        if _is_operative_locator(locator)
    ]
    fallback_pool = [
        (locator, sentence)
        for locator, sentence in sentence_candidates
        if _normalize_proposition_comparison(sentence) not in excludes
    ]
    selected_candidates = (
        preferred_normative_candidates
        or normative_candidates
        or fallback_pool[:max(1, limit)]
        or [
            (
                source.authoritative_locator or "document:full",
                source.authoritative_text.strip() or "No source text provided.",
            )
        ]
    )

    propositions: list[Proposition] = []
    for offset, (locator, sentence) in enumerate(selected_candidates[:limit]):
        seq = id_seq_start + offset
        if _normalize_proposition_comparison(sentence) in excludes:
            continue
        legal_subject = _guess_subject(sentence)
        action = _guess_action(sentence)
        article_reference = _extract_article_reference(sentence) or _extract_reference_from_locator(
            locator
        )

        propositions.append(
            Proposition(
                id=f"prop-{_proposition_id_stem(source)}-{seq:03d}",
                proposition_key=None,
                topic_id=topic.id,
                cluster_id=cluster.id,
                source_record_id=source.id,
                source_fragment_id=None,
                fragment_locator=str(locator) if locator else None,
                source_snapshot_id=source.current_snapshot_id,
                jurisdiction=source.jurisdiction,
                article_reference=article_reference,
                proposition_text=sentence,
                legal_subject=legal_subject,
                action=action,
                conditions=_guess_conditions(sentence),
                authority=_guess_authority(sentence),
                required_documents=_guess_required_documents(sentence),
                affected_subjects=[legal_subject],
                notes="heuristic extraction",
            )
        )
    return propositions


def _heuristic_extraction(
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    limit: int,
) -> list[Proposition]:
    structured_candidates = _structured_list_candidates_for_source(source)
    structured_props: list[Proposition] = []
    exclusions: list[str] = []
    if structured_candidates:
        structured_props, exclusions = _structured_list_props(
            source,
            topic,
            cluster,
            quota=limit,
        )

    exclude_norms = {_normalize_proposition_comparison(x) for x in exclusions if x}
    remainder = max(0, limit - len(structured_props))
    sentence_props: list[Proposition] = []
    if remainder > 0:
        sentence_props = _heuristic_sentence_extraction_filtered(
            source,
            topic,
            cluster,
            limit=remainder,
            exclude_norm_texts=exclude_norms,
            id_seq_start=len(structured_props) + 1,
        )

    merged = [*structured_props, *sentence_props]
    return merged[:limit]


def parse_judit_extraction_meta(notes: str | None) -> dict[str, Any] | None:
    if not notes:
        return None
    first = notes.split("\n", 1)[0].strip()
    if not first.startswith(JUDIT_EXTRACTION_META_PREFIX):
        return None
    raw = first[len(JUDIT_EXTRACTION_META_PREFIX) :].strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict) or not data:
        return None
    return data


def attach_judit_extraction_meta(base_notes: str, meta: dict[str, Any]) -> str:
    line = f"{JUDIT_EXTRACTION_META_PREFIX}{json.dumps(meta, sort_keys=True)}"
    rest = (base_notes or "").strip()
    if rest:
        return f"{line}\n{rest}"
    return line


def attach_judit_extraction_reuse(base_notes: str, reuse: dict[str, Any]) -> str:
    """Append structured extraction reuse audit (keeps first-line extraction meta intact)."""
    line = f"{JUDIT_EXTRACTION_REUSE_PREFIX}{json.dumps(reuse, sort_keys=True)}"
    rest = (base_notes or "").strip()
    if rest:
        return f"{rest}\n{line}"
    return line


def parse_judit_extraction_reuse(notes: str | None) -> dict[str, Any] | None:
    if not notes:
        return None
    for raw_ln in notes.split("\n"):
        ln = raw_ln.strip()
        if not ln.startswith(JUDIT_EXTRACTION_REUSE_PREFIX):
            continue
        raw_json = ln[len(JUDIT_EXTRACTION_REUSE_PREFIX) :].strip()
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            return None
        if isinstance(data, dict) and data:
            return data
    return None


_UNICODE_QUOTES: dict[int, str] = {
    0x201C: '"',
    0x201D: '"',
    0x2018: "'",
    0x2019: "'",
    0x00AB: '"',
    0x00BB: '"',
    0x2039: "'",
    0x203A: "'",
}


def _fold_unicode_quotes(s: str) -> str:
    return "".join(_UNICODE_QUOTES.get(ord(ch), ch) for ch in s)


def _strip_soft_hyphens(s: str) -> str:
    return s.replace("\u00AD", "").replace("\uFEFF", "")


_LINE_LIST_PREFIX_ITER = re.compile(
    r"^(?:"
    r"\(?:?\d+\)?[.)]\s+"
    r"|"
    r"\(\s*[a-z0-9ivxl]+\s*\)\s+"
    r"|"
    r"[\-\u2013\u2014•\*·]+\s+"
    r")",
    flags=re.IGNORECASE,
)


def _strip_first_list_like_prefix(fragment: str) -> str | None:
    stripped = fragment.lstrip()
    if not stripped:
        return None
    m = _LINE_LIST_PREFIX_ITER.match(stripped)
    if not m:
        return None
    tail = stripped[m.end() :].lstrip()
    if tail.startswith(";"):
        tail = tail[1:].lstrip()
    return tail


def _strip_list_like_line_prefixes(line: str) -> str:
    cur = line.strip()
    if not cur:
        return ""
    for _ in range(8):
        nxt = _strip_first_list_like_prefix(cur)
        if nxt is None:
            break
        cur = nxt.strip()
        if not cur:
            return ""
    return cur


def _normalize_evidence_for_match(s: str, *, strip_line_list_markers: bool) -> str:
    t = _strip_soft_hyphens(s)
    t = _fold_unicode_quotes(t)
    if strip_line_list_markers:
        chunks: list[str] = []
        for raw in t.replace("\r\n", "\n").split("\n"):
            norm_line = _strip_list_like_line_prefixes(raw)
            if norm_line.strip():
                chunks.append(norm_line.strip())
        t = " ".join(chunks)
    else:
        t = " ".join(t.split())
    return t.lower()


def evidence_locates_verbatim_after_normalisation(
    evidence: str, source_text: str
) -> tuple[bool, str, dict[str, Any]]:
    """Return True only if a normalisation of evidence appears verbatim in the same-normalised source (no fuzzy paraphrase)."""
    variants: tuple[tuple[bool, str], ...] = (
        (False, "whitespace_quote_fold_lc_substring_match"),
        (True, "whitespace_quote_fold_lc_list_marker_substring_match"),
    )
    diagnostics: dict[str, Any] = {"attempted": [], "haystack_lens": [], "needle_lens": []}
    cand_raw = evidence.strip()
    if not cand_raw:
        return False, "empty_candidate", diagnostics
    hay_raw = source_text
    for strip_markers, label in variants:
        ev_n = _normalize_evidence_for_match(cand_raw, strip_line_list_markers=strip_markers)
        src_n = _normalize_evidence_for_match(hay_raw, strip_line_list_markers=strip_markers)
        diagnostics["attempted"].append(label)
        diagnostics["needle_lens"].append(len(ev_n))
        diagnostics["haystack_lens"].append(len(src_n))
        if not ev_n:
            diagnostics["failure"] = "evidence_normalized_to_empty"
            return False, label, diagnostics
        if ev_n in src_n:
            diagnostics["success"] = label
            return True, label, diagnostics
        diagnostics.setdefault("needle_tail", ev_n[-36:])
    diagnostics["failure"] = "no_normalized_substring_match_paraphrase_or_unrecoverable_markup"
    return False, variants[-1][1], diagnostics


def _validate_v2_items(
    raw_items: list[dict[str, Any]],
    source_text: str,
    *,
    limit: int,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    errors: list[str] = []
    issue_records: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for idx, item in enumerate(raw_items):
        if not isinstance(item, dict):
            msg = f"row {idx}: expected object"
            errors.append(msg)
            issue_records.append(
                {
                    "kind": "structure",
                    "row_index": idx,
                    "candidate_evidence_text": "",
                    "source_locator_hint": "",
                    "normalization_matching_strategy": "",
                    "reason_code": "invalid_row_type",
                    "failure_reason": msg,
                    "diagnostics": {},
                }
            )
            continue
        ptxt = str(item.get("proposition_text") or "").strip()
        if not ptxt:
            msg = f"row {idx}: proposition_text empty"
            errors.append(msg)
            issue_records.append(
                {
                    "kind": "proposition",
                    "row_index": idx,
                    "candidate_evidence_text": str(item.get("evidence_text") or ""),
                    "source_locator_hint": str(item.get("source_locator") or "").strip(),
                    "normalization_matching_strategy": "",
                    "reason_code": "empty_proposition",
                    "failure_reason": msg,
                    "diagnostics": {},
                }
            )
            continue
        ev = str(item.get("evidence_text") or "").strip()
        reason_raw = str(item.get("reason") or "").strip()
        loc_raw = str(item.get("source_locator") or "").strip()
        if not ev:
            if not reason_raw:
                msg = (
                    f"row {idx}: evidence_text empty but reason does not explain why "
                    "(copy verbatim or leave empty only with justification)"
                )
                errors.append(msg)
                issue_records.append(
                    {
                        "kind": "evidence_traceability",
                        "row_index": idx,
                        "candidate_evidence_text": "",
                        "source_locator_hint": loc_raw,
                        "normalization_matching_strategy": "none_empty_evidence",
                        "reason_code": "empty_evidence_no_reason",
                        "failure_reason": msg,
                        "diagnostics": {},
                    }
                )
                continue
        else:
            ok, strategy, diag = evidence_locates_verbatim_after_normalisation(ev, source_text)
            if not ok:
                msg = f"row {idx}: evidence_text not traceable to source"
                errors.append(msg)
                issue_records.append(
                    {
                        "kind": "evidence_traceability",
                        "row_index": idx,
                        "candidate_evidence_text": ev,
                        "source_locator_hint": loc_raw,
                        "normalization_matching_strategy": strategy,
                        "reason_code": "no_verbatim_normalized_span",
                        "failure_reason": msg,
                        "diagnostics": diag,
                    }
                )
                continue
            item["_validated_evidence_match_strategy"] = strategy
        nk = (_normalize_proposition_comparison(ptxt), loc_raw.lower())
        if nk in seen_keys:
            msg = f"row {idx}: duplicate proposition_text for same locator"
            errors.append(msg)
            issue_records.append(
                {
                    "kind": "dedup",
                    "row_index": idx,
                    "candidate_evidence_text": ev,
                    "source_locator_hint": loc_raw,
                    "normalization_matching_strategy": str(
                        item.get("_validated_evidence_match_strategy") or ""
                    ),
                    "reason_code": "duplicate_proposition_same_locator",
                    "failure_reason": msg,
                    "diagnostics": {},
                }
            )
            continue
        seen_keys.add(nk)
        pt = item.get("provision_type")
        if pt is not None and str(pt) not in _PROVISION_TYPES_V2:
            msg = f"row {idx}: invalid provision_type"
            errors.append(msg)
            issue_records.append(
                {
                    "kind": "schema",
                    "row_index": idx,
                    "candidate_evidence_text": ev,
                    "source_locator_hint": loc_raw,
                    "normalization_matching_strategy": str(
                        item.get("_validated_evidence_match_strategy") or ""
                    ),
                    "reason_code": "invalid_provision_type",
                    "failure_reason": msg,
                    "diagnostics": {},
                }
            )
            continue
        cs = item.get("completeness_status")
        if cs is not None and str(cs) not in _COMPLETENESS_V2:
            msg = f"row {idx}: invalid completeness_status"
            errors.append(msg)
            issue_records.append(
                {
                    "kind": "schema",
                    "row_index": idx,
                    "candidate_evidence_text": ev,
                    "source_locator_hint": loc_raw,
                    "normalization_matching_strategy": str(
                        item.get("_validated_evidence_match_strategy") or ""
                    ),
                    "reason_code": "invalid_completeness",
                    "failure_reason": msg,
                    "diagnostics": {},
                }
            )
            continue
        accepted.append(item)
        if len(accepted) >= limit:
            break
    return accepted, errors, issue_records


def _v2_model_prompt(
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    *,
    extraction_mode: Literal["frontier", "local"],
    max_propositions: int = 4,
    focus_scopes: Sequence[str] | None = None,
    prompt_source_text: str | None = None,
    fragment_locator_hint: str | None = None,
) -> str:
    schema = """
{
  "propositions": [
    {
      "proposition_text": "string",
      "display_label": "string",
      "subject": "string",
      "rule": "string",
      "object": "string",
      "conditions": ["string"],
      "exceptions": ["string"],
      "temporal_condition": "string or empty",
      "provision_type": "core | definition | exception | transitional | cross_reference",
      "source_locator": "string",
      "evidence_text": "string",
      "completeness_status": "complete | context_dependent | fragmentary",
      "confidence": "high | medium | low",
      "reason": "string"
    }
  ]
}
""".strip()
    scopes = tuple(str(s).strip() for s in (focus_scopes or ()) if str(s).strip())
    cap_line = (
        f"Extract at most {max_propositions} legally distinct propositions for this response "
        f"(hard cap — omit lower-salience rows if needed)."
    )
    list_rules = """
Structured lists:
- Where the Source uses numbered paragraphs, lettered subparagraphs ((a), (b), …), or roman sub-items ((i), (ii), …), treat each substantive obligation as its own proposition when it is legally distinct from neighbouring rows.
- Do not merge unrelated list items; do not stop after an early subset when later items in the same list remain legally distinct and within the cap below.
"""
    scoped_prior = ""
    if scopes:
        joined = "; ".join(scopes)
        scoped_prior = f"""
Focus scopes (case-config priority labels — interpret substantively, not keyword matching):
- The case prioritises material relating to: {joined}
- Within the cap below, include propositions whose substance connects to those scopes, including items that appear later in long lists, before dropping them in favour of unrelated earlier rows.
"""
    else:
        scoped_prior = """
No focus scopes:
- Within the cap below, prefer the most legally salient obligations for the Topic and Cluster; stay within the cap rather than enumerating every peripheral line.
"""

    frontier_rules = ""
    if extraction_mode == "frontier":
        frontier_rules = f"""
Frontier extraction rules (critical for audits):
- {cap_line}
{list_rules.strip()}
{scoped_prior.strip()}
- proposition_text MAY be rewritten as a concise, readable legal proposition (normalized wording OK).
- evidence_text MUST be copied verbatim from the Source text chunk — character-for-character as it appears there (subset / exact span selection). It must be the shortest contiguous span from the Source that suffices to justify the proposition. Do NOT paraphrase, summarise, or re-order words inside evidence_text.
- source_locator MUST preserve traceability to the obligation's position (e.g. article/paragraph identifiers, list marks such as '(d)(ii)' or '(b)') consistent with the Source layout.
- If no contiguous verbatim span can be selected (ambiguous layout, garbled OCR, contradictory instructions), set evidence_text to an empty string and use reason to explain briefly why verbatim evidence could not be provided.
"""

    local_rules = ""
    if extraction_mode == "local":
        local_rules = f"""
Local extraction rules:
- {cap_line}
{list_rules.strip()}
{scoped_prior.strip()}
- source_locator SHOULD identify where the obligation sits when identifiable from the Source.
"""

    rules_line = ""
    if frontier_rules.strip():
        rules_line = frontier_rules.strip() + "\n\n"
    elif local_rules.strip():
        rules_line = local_rules.strip() + "\n\n"

    schema_terms = ""
    if extraction_mode == "frontier":
        schema_terms = """
Semantics:
- proposition_text: normalised legal proposition (readable).
- evidence_text: exact quote from Source (verbatim span), or \"\" when impossible with rationale in reason.
"""

    body_text = source.authoritative_text if prompt_source_text is None else prompt_source_text
    locator_note = ""
    if fragment_locator_hint and str(fragment_locator_hint).strip():
        locator_note = f"\nFragment locator (traceability): {str(fragment_locator_hint).strip()}\n"

    return f"""
Extract legal propositions from the source text. Return JSON only with this exact shape:
{schema}

{rules_line}{schema_terms}Topic: {topic.name}
Cluster: {cluster.name}
Jurisdiction: {source.jurisdiction}
Citation: {source.citation}
{locator_note}
Source text:
{body_text}
""".strip()


def _parse_model_propositions_container(parsed: Any) -> list[dict[str, Any]]:
    if isinstance(parsed, dict) and isinstance(parsed.get("propositions"), list):
        return [x for x in parsed["propositions"] if isinstance(x, dict)]
    if isinstance(parsed, list):
        return [x for x in parsed if isinstance(x, dict)]
    return []


_SENSITIVE_OUTPUT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[^\s\"',;]+"),
    re.compile(r"(?i)(x-api-key\s*:\s*)[^\s\"',;]+"),
    re.compile(r"(?i)(api[_-]?key\s*[=:]\s*)[^\s\"',;]+"),
    re.compile(r"(?i)(client[_-]?secret\s*[=:]\s*)[^\s\"',;]+"),
)


def _redact_sensitive_model_output(raw: str) -> str:
    text = raw
    for pattern in _SENSITIVE_OUTPUT_PATTERNS:
        text = pattern.sub(r"\1[REDACTED]", text)
    return text


def _safe_model_output_excerpt(raw: str, *, cap: int = 4000) -> tuple[str, bool]:
    redacted = _redact_sensitive_model_output(raw)
    truncated = len(redacted) > cap
    return redacted[:cap], truncated


def _try_extract_model_v2_json(
    *,
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    llm_client: JuditLLMClient,
    model_alias: str,
    extraction_mode: Literal["frontier", "local"],
    max_propositions: int,
    focus_scopes: Sequence[str] | None = None,
    prompt_source_text: str | None = None,
    fragment_locator_hint: str | None = None,
) -> tuple[list[dict[str, Any]] | None, str | None, dict[str, Any] | None]:
    body = source.authoritative_text if prompt_source_text is None else prompt_source_text
    prompt = _v2_model_prompt(
        source,
        topic,
        cluster,
        extraction_mode=extraction_mode,
        max_propositions=max_propositions,
        focus_scopes=focus_scopes,
        prompt_source_text=body,
        fragment_locator_hint=fragment_locator_hint,
    )
    raw: str | None = None
    try:
        raw = llm_client.complete_text(
            prompt=prompt,
            model=model_alias,
            system_prompt=_V2_SYSTEM_PROMPT,
            temperature=0.0,
            enforce_json_object=True,
        )
        parsed = _parse_json(raw)
    except json.JSONDecodeError as exc:
        excerpt = ""
        truncated = False
        if isinstance(raw, str) and raw:
            excerpt, truncated = _safe_model_output_excerpt(raw, cap=4000)
        return (
            None,
            f"model call or JSON parse failed: {exc}",
            {
                "raw_model_output_excerpt": excerpt,
                "raw_model_output_truncated": truncated,
                "parse_error_message": str(exc),
                "parse_error_line": int(exc.lineno) if isinstance(exc.lineno, int) else None,
                "parse_error_column": int(exc.colno) if isinstance(exc.colno, int) else None,
            },
        )
    except Exception as exc:
        return None, f"model call or JSON parse failed: {exc}", None
    rows = _parse_model_propositions_container(parsed)
    if not rows:
        return None, "model returned no propositions", None
    return rows, None, None


def _stamp_props_meta(
    props: list[Proposition],
    *,
    extraction_mode: str,
    model_alias: str | None,
    fallback_policy: str,
    fallback_used: bool,
    validation_errors: list[str],
    prompt_version: str,
    schema_version: str,
    extra_per_prop: list[dict[str, Any]] | None = None,
    pipeline_signals: dict[str, Any] | None = None,
) -> None:
    pipe = dict(pipeline_signals or {})
    for i, p in enumerate(props):
        ex = dict(extra_per_prop[i]) if extra_per_prop and i < len(extra_per_prop) else {}
        meta = {
            "extraction_mode": extraction_mode,
            "model_alias": model_alias,
            "fallback_policy": fallback_policy,
            "fallback_used": fallback_used,
            "validation_errors": list(validation_errors),
            "prompt_version": prompt_version,
            "schema_version": schema_version,
            **pipe,
            **ex,
        }
        p.notes = attach_judit_extraction_meta(p.notes, meta)


def _build_propositions_from_v2_rows(
    *,
    rows: list[dict[str, Any]],
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    limit: int,
    id_sequence_start: int = 1,
) -> list[Proposition]:
    out: list[Proposition] = []
    for offset, item in enumerate(rows[:limit]):
        index = id_sequence_start + offset
        ptxt = str(item.get("proposition_text") or "").strip()
        subject = str(item.get("subject") or "").strip() or _guess_subject(ptxt)
        rule = str(item.get("rule") or "").strip() or _guess_action(ptxt)
        obj = str(item.get("object") or "").strip()
        affected = [obj] if obj else [subject]
        conditions = [str(x) for x in item.get("conditions") or [] if str(x).strip()]
        for exc in item.get("exceptions") or []:
            if isinstance(exc, str) and exc.strip():
                conditions.append(f"exception: {exc.strip()}")
        tc = item.get("temporal_condition")
        if isinstance(tc, str) and tc.strip():
            conditions.append(f"temporal: {tc.strip()}")
        locator = str(item.get("source_locator") or "").strip()
        frag_loc = locator if locator else (source.authoritative_locator or None)
        art = (
            _extract_reference_from_locator(frag_loc)
            if frag_loc and not _is_placeholder_locator(frag_loc)
            else _extract_article_reference(ptxt)
        )
        label = str(item.get("display_label") or "").strip()
        notes_tail = str(item.get("reason") or "").strip()
        prop = Proposition(
            id=f"prop-{_proposition_id_stem(source)}-{index:03d}",
            topic_id=topic.id,
            cluster_id=cluster.id,
            source_record_id=source.id,
            source_fragment_id=None,
            fragment_locator=frag_loc if frag_loc else source.authoritative_locator,
            source_snapshot_id=source.current_snapshot_id,
            jurisdiction=source.jurisdiction,
            article_reference=art,
            proposition_text=ptxt,
            label=label,
            short_name=label[:200] if label else "",
            legal_subject=subject,
            action=rule,
            conditions=conditions,
            authority=_guess_authority(ptxt),
            required_documents=_guess_required_documents(ptxt),
            affected_subjects=affected,
            notes=notes_tail,
        )
        out.append(prop)
    return out


def extract_propositions_from_source(
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    *,
    llm_client: JuditLLMClient | None,
    limit: int,
    extraction_mode: Literal["heuristic", "local", "frontier"],
    extraction_fallback: Literal["fallback", "fail_closed", "mark_needs_review"],
    prompt_version: str = EXTRACTION_PROMPT_VERSION_V2,
    on_before_llm_call: Callable[[dict[str, Any]], None] | None = None,
    focus_scopes: Sequence[str] | None = None,
    model_error_policy: Literal[
        "continue_with_fallback", "stop_repairable", "continue_repairable"
    ] = "continue_with_fallback",
    derived_chunk_cache: DerivedArtifactCache | None = None,
    retry_failed_llm: bool = False,
    chunk_cache_pipeline_version: str = "0.1.0",
    chunk_cache_strategy_version: str = "v1",
) -> ExtractSourceResult:
    scope_signals: dict[str, Any] = {}
    if focus_scopes:
        cleaned_fs = [str(s).strip() for s in focus_scopes if str(s).strip()]
        if cleaned_fs:
            scope_signals["focus_scopes"] = cleaned_fs

    def _merge_signals(extra: dict[str, Any] | None) -> dict[str, Any] | None:
        if not scope_signals and not extra:
            return None
        merged = dict(scope_signals)
        if extra:
            merged.update(extra)
        return merged

    base_kwargs = dict(
        extraction_mode=extraction_mode,
        fallback_policy=extraction_fallback,
        prompt_version=prompt_version,
    )
    if extraction_mode == "heuristic":
        props = _heuristic_extraction(source=source, topic=topic, cluster=cluster, limit=limit)
        _stamp_props_meta(
            props,
            model_alias=None,
            fallback_used=False,
            validation_errors=[],
            schema_version="none",
            pipeline_signals=_merge_signals(None),
            **base_kwargs,
        )
        return ExtractSourceResult(
            propositions=props,
            model_alias=None,
            fallback_used=False,
            validation_errors=[],
            schema_version="none",
            validation_issue_records=[],
            extraction_llm_call_traces=[],
            **base_kwargs,
        )

    if llm_client is None or not source.authoritative_text.strip():
        err = "LLM extraction requested but client missing or empty source text"
        if extraction_fallback == "fail_closed":
            return ExtractSourceResult(
                propositions=[],
                model_alias=None,
                fallback_used=False,
                validation_errors=[err],
                schema_version=EXTRACTION_SCHEMA_VERSION_V2,
                failed_closed=True,
                failure_reason=err,
                validation_issue_records=[],
                extraction_llm_call_traces=[],
                **base_kwargs,
            )
        if extraction_fallback == "mark_needs_review":
            props = _heuristic_extraction(source=source, topic=topic, cluster=cluster, limit=limit)
            for p in props:
                p.review_status = ReviewStatus.NEEDS_REVIEW
            val_err = [err]
            _stamp_props_meta(
                props,
                model_alias=None,
                fallback_used=True,
                validation_errors=val_err,
                schema_version="none",
                pipeline_signals=_merge_signals(None),
                **base_kwargs,
            )
            return ExtractSourceResult(
                propositions=props,
                model_alias=None,
                fallback_used=True,
                validation_errors=val_err,
                schema_version="none",
                validation_issue_records=[],
                extraction_llm_call_traces=[],
                **base_kwargs,
            )
        props = _heuristic_extraction(source=source, topic=topic, cluster=cluster, limit=limit)
        _stamp_props_meta(
            props,
            model_alias=None,
            fallback_used=True,
            validation_errors=[err],
            schema_version="none",
            pipeline_signals=_merge_signals(None),
            **base_kwargs,
        )
        return ExtractSourceResult(
            propositions=props,
            model_alias=None,
            fallback_used=True,
            validation_errors=[err],
            schema_version="none",
            validation_issue_records=[],
            extraction_llm_call_traces=[],
            **base_kwargs,
        )

    settings = llm_client.settings
    max_input_tokens = int(getattr(settings, "max_extract_input_tokens", 150_000))
    configured_ctx_limit = int(getattr(settings, "extract_model_context_limit", 200_000))
    overlap_chars = min(8192, max(512, max_input_tokens // 40))

    model_alias = (
        settings.frontier_extract_model if extraction_mode == "frontier" else settings.local_extract_model
    )
    llm_prompt_mode: Literal["frontier", "local"] = (
        "frontier" if extraction_mode == "frontier" else "local"
    )

    extraction_llm_call_traces: list[dict[str, Any]] = []

    def _trace_row(**extra: Any) -> dict[str, Any]:
        fid = None
        if isinstance(source.metadata, dict):
            raw_f = source.metadata.get("extraction_fragment_id")
            if raw_f is not None:
                fid = str(raw_f)
        row: dict[str, Any] = {
            "source_record_id": source.id,
            "source_title": source.title,
            "source_fragment_id": fid,
            "fragment_locator": source.authoritative_locator,
            "model_alias": model_alias,
            "configured_context_limit": configured_ctx_limit,
            "extraction_mode": extraction_mode,
        }
        row.update(extra)
        return row

    full_body = source.authoritative_text.strip()
    initial_est = _estimate_extract_prompt_tokens(
        source=source,
        topic=topic,
        cluster=cluster,
        extraction_mode=llm_prompt_mode,
        max_propositions=limit,
        focus_scopes=focus_scopes,
        prompt_source_text=full_body,
        fragment_locator_hint=None,
    )

    planned_chunks: list[_PlannedExtractChunk]
    if initial_est <= max_input_tokens:
        planned_chunks = [
            _PlannedExtractChunk(
                locator=source.authoritative_locator or "document:full",
                text=full_body,
                split_strategy="single",
            )
        ]
    else:
        max_body_chars = _max_body_chars_for_extract_budget(
            source=source,
            topic=topic,
            cluster=cluster,
            extraction_mode=llm_prompt_mode,
            max_propositions=limit,
            focus_scopes=focus_scopes,
            token_budget=max_input_tokens,
        )
        planned_chunks = _plan_llm_text_chunks(
            source,
            max_body_chars=max_body_chars,
            overlap_chars=overlap_chars,
        )

    preflight_blocked: str | None = None
    if initial_est > max_input_tokens and not planned_chunks:
        preflight_blocked = (
            f"context_window_risk: prompt estimate {initial_est} exceeds "
            f"max_extract_input_tokens={max_input_tokens} and no chunks were planned"
        )
        extraction_llm_call_traces.append(
            _trace_row(
                estimated_input_tokens=initial_est,
                skipped_llm=True,
                skip_reason="context_window_risk",
                extraction_llm_chunk_index=0,
                extraction_llm_chunk_total=0,
                extraction_chunk_split_strategy="none",
                llm_invoked=False,
            )
        )

    aggregated_validated: list[dict[str, Any]] = []
    issue_records: list[dict[str, Any]] = []
    valerrs: list[str] = []
    repairable_halt = False
    repairable_halt_reason: str | None = None
    scopes_tuple = tuple(sorted(str(s).strip() for s in (focus_scopes or ()) if str(s).strip()))
    frag_id_cache: str | None = None
    if isinstance(source.metadata, dict) and source.metadata.get("extraction_fragment_id"):
        frag_id_cache = str(source.metadata["extraction_fragment_id"])
    snap_id_cache = str(source.current_snapshot_id).strip() if source.current_snapshot_id else None

    if preflight_blocked and model_error_policy == "stop_repairable":
        return ExtractSourceResult(
            propositions=[],
            model_alias=model_alias,
            fallback_used=False,
            validation_errors=[preflight_blocked],
            schema_version=EXTRACTION_SCHEMA_VERSION_V2,
            validation_issue_records=[],
            extraction_llm_call_traces=extraction_llm_call_traces,
            repairable_extraction_halt=True,
            repairable_extraction_halt_reason=preflight_blocked,
            **base_kwargs,
        )

    if preflight_blocked is None and planned_chunks:
        n_chunks = len(planned_chunks)
        for ci, planned in enumerate(planned_chunks, start=1):
            fitted = _fit_body_text_to_extract_budget(
                body=planned.text,
                source=source,
                topic=topic,
                cluster=cluster,
                extraction_mode=llm_prompt_mode,
                max_propositions=limit,
                focus_scopes=focus_scopes,
                fragment_locator_hint=planned.locator,
                max_input_tokens=max_input_tokens,
            )
            if fitted is None:
                valerrs.append(
                    f"context_window_risk: extraction chunk {ci}/{n_chunks} cannot be shaped to "
                    f"fit max_extract_input_tokens={max_input_tokens}"
                )
                extraction_llm_call_traces.append(
                    _trace_row(
                        estimated_input_tokens=-1,
                        skipped_llm=True,
                        skip_reason="context_window_risk",
                        extraction_llm_chunk_index=ci,
                        extraction_llm_chunk_total=n_chunks,
                        fragment_locator=planned.locator,
                        extraction_chunk_split_strategy=planned.split_strategy,
                        llm_invoked=False,
                    )
                )
                if model_error_policy == "stop_repairable":
                    repairable_halt = True
                    repairable_halt_reason = (
                        f"context_window_risk: extraction chunk {ci}/{n_chunks} cannot be shaped to "
                        f"fit max_extract_input_tokens={max_input_tokens}"
                    )
                    break
                continue

            chunk_text, chunk_est = fitted
            trace_pre = _trace_row(
                estimated_input_tokens=chunk_est,
                skipped_llm=False,
                extraction_llm_chunk_index=ci,
                extraction_llm_chunk_total=n_chunks,
                fragment_locator=planned.locator,
                extraction_chunk_split_strategy=planned.split_strategy,
                llm_invoked=False,
            )
            extraction_llm_call_traces.append(trace_pre)

            chunk_fp = content_hash(chunk_text)[:48]
            cache_key_str: str | None = None
            if derived_chunk_cache is not None and model_alias:
                cache_key_str = build_proposition_extraction_chunk_cache_key(
                    source_snapshot_id=snap_id_cache,
                    source_fragment_id=frag_id_cache,
                    source_fragment_locator=planned.locator,
                    chunk_index=ci,
                    chunk_body_fingerprint=chunk_fp,
                    model_alias=str(model_alias),
                    extraction_mode=str(extraction_mode),
                    prompt_version=prompt_version,
                    focus_scopes=scopes_tuple,
                    max_propositions=limit,
                    pipeline_version=chunk_cache_pipeline_version,
                    strategy_version=chunk_cache_strategy_version,
                )
                cached_hit = derived_chunk_cache.get(
                    stage_name="proposition_extraction_chunk", cache_key=cache_key_str
                )
                if cached_hit is not None and isinstance(cached_hit.payload, dict):
                    cst = cached_hit.payload.get("chunk_status")
                    if cst == "llm_success":
                        vr = cached_hit.payload.get("validated_rows")
                        if isinstance(vr, list) and vr:
                            trace_pre["llm_cache_hit"] = True
                            trace_pre["llm_invoked"] = False
                            aggregated_validated.extend(
                                [dict(r) for r in vr if isinstance(r, dict)]
                            )
                            continue
                    if cst == "failure" and not retry_failed_llm:
                        trace_pre["llm_cache_hit"] = "failed_chunk_cached"
                        trace_pre["llm_invoked"] = False
                        continue

            if on_before_llm_call is not None:
                on_before_llm_call(dict(trace_pre))

            try_result = _try_extract_model_v2_json(
                source=source,
                topic=topic,
                cluster=cluster,
                llm_client=llm_client,
                model_alias=model_alias,
                extraction_mode=llm_prompt_mode,
                max_propositions=limit,
                focus_scopes=focus_scopes,
                prompt_source_text=chunk_text,
                fragment_locator_hint=planned.locator,
            )
            parse_diag: dict[str, Any] | None = None
            if isinstance(try_result, tuple) and len(try_result) == 3:
                raw_rows, model_err, parse_diag = try_result
            else:
                raw_rows, model_err = try_result  # backward-compatible for monkeypatched tests
            trace_pre["llm_invoked"] = True
            if model_err:
                valerrs.append(f"chunk {ci}/{n_chunks}: {model_err}")
                trace_pre["model_error"] = model_err
                if parse_diag:
                    trace_pre.update(parse_diag)
                if cache_key_str and derived_chunk_cache is not None and model_alias:
                    derived_chunk_cache.put(
                        stage_name="proposition_extraction_chunk",
                        cache_key=cache_key_str,
                        payload={
                            "chunk_status": "failure",
                            "error": model_err,
                        },
                    )
                if model_error_policy == "stop_repairable" and _looks_like_infra_llm_failure(model_err):
                    repairable_halt = True
                    repairable_halt_reason = model_err
                    break
                continue
            assert raw_rows is not None
            v_rows, verrs, row_issues = _validate_v2_items(raw_rows, chunk_text, limit=limit)
            valerrs.extend([f"chunk {ci}/{n_chunks}: {x}" for x in verrs])
            issue_records.extend(row_issues)
            aggregated_validated.extend(v_rows)
            if cache_key_str and derived_chunk_cache is not None and model_alias and v_rows:
                derived_chunk_cache.put(
                    stage_name="proposition_extraction_chunk",
                    cache_key=cache_key_str,
                    payload={
                        "chunk_status": "llm_success",
                        "validated_rows": v_rows,
                    },
                )

    merged_validated = _merge_dedupe_validated_v2_rows(aggregated_validated, limit=limit)

    blocked_reason = preflight_blocked
    if blocked_reason:
        valerrs.insert(0, blocked_reason)

    model_failed = bool(blocked_reason) or not merged_validated

    if (
        model_failed
        and repairable_halt
        and model_error_policy == "stop_repairable"
    ):
        merged_err = valerrs or [repairable_halt_reason or "model extraction halted early (repairable)"]
        return ExtractSourceResult(
            propositions=[],
            model_alias=model_alias,
            fallback_used=False,
            validation_errors=merged_err,
            schema_version=EXTRACTION_SCHEMA_VERSION_V2,
            validation_issue_records=issue_records,
            extraction_llm_call_traces=extraction_llm_call_traces,
            repairable_extraction_halt=True,
            repairable_extraction_halt_reason=repairable_halt_reason or merged_err[0],
            **base_kwargs,
        )

    if model_failed:
        definition_fb_props: list[Proposition] = []
        definition_fb_meta: list[dict[str, Any]] = []
        definition_fb_warnings: list[str] = []
        if _is_definition_provision(source.authoritative_text):
            definition_fb_props, definition_fb_meta, definition_fb_warnings = _definition_fallback_extraction(
                source=source,
                topic=topic,
                cluster=cluster,
                limit=limit,
            )
        use_definition_fallback = bool(definition_fb_props)

        if extraction_fallback == "fail_closed":
            merged_err = valerrs or ["model extraction produced no valid propositions"]
            return ExtractSourceResult(
                propositions=[],
                model_alias=model_alias,
                fallback_used=False,
                validation_errors=merged_err,
                schema_version=EXTRACTION_SCHEMA_VERSION_V2,
                failed_closed=True,
                failure_reason=merged_err[0],
                validation_issue_records=issue_records,
                extraction_llm_call_traces=extraction_llm_call_traces,
                fallback_strategy=None,
                **base_kwargs,
            )
        if extraction_fallback == "mark_needs_review":
            if use_definition_fallback:
                props = definition_fb_props
            else:
                props = _heuristic_extraction(source=source, topic=topic, cluster=cluster, limit=limit)
                for p in props:
                    p.review_status = ReviewStatus.NEEDS_REVIEW
            fb_errs = list(valerrs)
            fb_errs.extend(definition_fb_warnings)
            pipe_fb: dict[str, Any] = {}
            if issue_records:
                pipe_fb["pipeline_evidence_issue_records"] = issue_records
            if extraction_llm_call_traces:
                pipe_fb["extraction_llm_call_traces"] = extraction_llm_call_traces
            if blocked_reason and "context_window_risk" in blocked_reason:
                pipe_fb["context_window_risk"] = True
            elif valerrs and any("context_window_risk" in str(x) for x in valerrs):
                pipe_fb["context_window_risk"] = True
            if use_definition_fallback:
                pipe_fb["fallback_strategy"] = "definition_extractor"
            _stamp_props_meta(
                props,
                model_alias=model_alias,
                fallback_used=True,
                validation_errors=fb_errs,
                schema_version="none",
                extra_per_prop=definition_fb_meta if use_definition_fallback else None,
                pipeline_signals=_merge_signals(pipe_fb if pipe_fb else None),
                **base_kwargs,
            )
            return ExtractSourceResult(
                propositions=props,
                model_alias=model_alias,
                fallback_used=True,
                validation_errors=fb_errs,
                schema_version="none",
                validation_issue_records=issue_records,
                extraction_llm_call_traces=extraction_llm_call_traces,
                fallback_strategy="definition_extractor" if use_definition_fallback else "heuristic_fallback",
                **base_kwargs,
            )
        if use_definition_fallback:
            props = definition_fb_props
        else:
            props = _heuristic_extraction(source=source, topic=topic, cluster=cluster, limit=limit)
        fb_errs = list(valerrs)
        fb_errs.extend(definition_fb_warnings)
        pipe_fb2: dict[str, Any] = {}
        if issue_records:
            pipe_fb2["pipeline_evidence_issue_records"] = issue_records
        if extraction_llm_call_traces:
            pipe_fb2["extraction_llm_call_traces"] = extraction_llm_call_traces
        if blocked_reason and "context_window_risk" in blocked_reason:
            pipe_fb2["context_window_risk"] = True
        elif valerrs and any("context_window_risk" in str(x) for x in valerrs):
            pipe_fb2["context_window_risk"] = True
        if use_definition_fallback:
            pipe_fb2["fallback_strategy"] = "definition_extractor"
        _stamp_props_meta(
            props,
            model_alias=model_alias,
            fallback_used=True,
            validation_errors=fb_errs,
            schema_version="none",
            extra_per_prop=definition_fb_meta if use_definition_fallback else None,
            pipeline_signals=_merge_signals(pipe_fb2 if pipe_fb2 else None),
            **base_kwargs,
        )
        return ExtractSourceResult(
            propositions=props,
            model_alias=model_alias,
            fallback_used=True,
            validation_errors=fb_errs,
            schema_version="none",
            validation_issue_records=issue_records,
            extraction_llm_call_traces=extraction_llm_call_traces,
            fallback_strategy="definition_extractor" if use_definition_fallback else "heuristic_fallback",
            **base_kwargs,
        )

    props = _build_propositions_from_v2_rows(
        rows=merged_validated,
        source=source,
        topic=topic,
        cluster=cluster,
        limit=limit,
    )
    row_extras: list[dict[str, Any]] = []
    for r in merged_validated[: len(props)]:
        ev_raw = str(r.get("evidence_text") or "").strip()
        strat = str(r.get("_validated_evidence_match_strategy") or "")
        tw: list[str] = []
        if not ev_raw:
            tw.append("evidence_quote_empty_explained_in_reason")
        ex: dict[str, Any] = {
            "provision_type": r.get("provision_type"),
            "completeness_status": r.get("completeness_status"),
            "model_confidence": r.get("confidence"),
            "evidence_quote": ev_raw,
            "evidence_match_strategy": strat or (
                "empty_with_reason" if not ev_raw else "unknown"
            ),
            "trace_warnings": tw,
        }
        row_extras.append(ex)
    trace_errs = [e for e in valerrs if e]
    pipe_flags: dict[str, Any] = {}
    if issue_records:
        pipe_flags["pipeline_evidence_issue_records"] = issue_records
    if extraction_llm_call_traces:
        pipe_flags["extraction_llm_call_traces"] = extraction_llm_call_traces
        invoked = sum(1 for t in extraction_llm_call_traces if t.get("llm_invoked"))
        if invoked:
            pipe_flags["extraction_chunk_count"] = invoked
        ests = [
            int(t["estimated_input_tokens"])
            for t in extraction_llm_call_traces
            if isinstance(t.get("estimated_input_tokens"), int) and int(t["estimated_input_tokens"]) > 0
        ]
        if ests:
            pipe_flags["estimated_input_tokens_max"] = max(ests)
        if any(str(t.get("skip_reason") or "") == "context_window_risk" for t in extraction_llm_call_traces):
            pipe_flags["context_window_risk"] = True
    _stamp_props_meta(
        props,
        model_alias=model_alias,
        fallback_used=False,
        validation_errors=trace_errs,
        schema_version=EXTRACTION_SCHEMA_VERSION_V2,
        extra_per_prop=row_extras,
        pipeline_signals=_merge_signals(pipe_flags if pipe_flags else None),
        **base_kwargs,
    )
    return ExtractSourceResult(
        propositions=props,
        model_alias=model_alias,
        fallback_used=False,
        validation_errors=trace_errs,
        schema_version=EXTRACTION_SCHEMA_VERSION_V2,
        validation_issue_records=issue_records,
        extraction_llm_call_traces=extraction_llm_call_traces,
        repairable_extraction_halt=repairable_halt,
        repairable_extraction_halt_reason=repairable_halt_reason if repairable_halt else None,
        fallback_strategy=None,
        **base_kwargs,
    )


def _split_sentences(text: str) -> list[str]:
    return _split_fragment_sentences(text)


def _looks_normative(sentence: str) -> bool:
    lowered = sentence.lower()
    return any(word in lowered for word in _TRIGGER_WORDS)


def _guess_subject(sentence: str) -> str:
    lowered = sentence.lower()
    candidates = [
        "operator",
        "keeper",
        "authority",
        "competent authority",
        "importer",
        "exporter",
        "person",
        "member state",
        "appropriate authority",
    ]
    for candidate in candidates:
        if candidate in lowered:
            return candidate
    stripped = re.sub(
        r"^(article|section|regulation|paragraph|schedule)\s+\d+[a-z]?[.:]?\s*",
        "",
        sentence,
        flags=re.IGNORECASE,
    )
    words = stripped.split()
    return words[0].lower() if words else "actor"


def _guess_action(sentence: str) -> str:
    lowered = sentence.lower()
    patterns = [
        "must",
        "shall",
        "may",
        "must not",
        "shall not",
        "is required to",
        "are required to",
        "is prohibited from",
        "are prohibited from",
    ]
    for pattern in patterns:
        if pattern in lowered:
            after = lowered.split(pattern, 1)[1].strip(" .,:;")
            return _normalize_whitespace(after.split(".")[0])[:120]
    return _normalize_whitespace(sentence)[:120]


def _guess_conditions(sentence: str) -> list[str]:
    lowered = sentence.lower()
    markers = ["before ", "after ", "where ", "if ", "unless ", "when "]
    conditions: list[str] = []
    for marker in markers:
        if marker in lowered:
            conditions.append(marker.strip() + " " + lowered.split(marker, 1)[1].strip(" .,:;"))
    return conditions[:3]


def _guess_authority(sentence: str) -> str | None:
    lowered = sentence.lower()
    if "competent authority" in lowered:
        return "competent authority"
    if "appropriate authority" in lowered:
        return "appropriate authority"
    if "commission" in lowered:
        return "commission"
    return None


def _guess_required_documents(sentence: str) -> list[str]:
    lowered = sentence.lower()
    documents = []
    for term in ("certificate", "register", "record", "licence", "authorization", "authorisation"):
        if term in lowered:
            documents.append(term)
    return documents


def _extract_article_reference(sentence: str) -> str | None:
    match = re.search(
        r"\b((?:art(?:icle)?|section|regulation|paragraph|schedule)\.?\s*\d+[a-z]?)\b",
        sentence,
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else None


def _extract_reference_from_locator(locator: str | None) -> str | None:
    if _is_placeholder_locator(locator):
        return None
    value = str(locator)
    match = re.search(
        r"(article|section|regulation|paragraph|schedule)[:/_-]?(\d+[a-z]?)",
        value,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return f"{match.group(1)} {match.group(2)}"


def _parse_json(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    return json.loads(text)
