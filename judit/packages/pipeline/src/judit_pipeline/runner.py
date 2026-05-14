import json
import os
import re
import tempfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Literal

from judit_domain import (
    ComparisonRun,
    DivergenceAssessment,
    DivergenceFinding,
    DivergenceObservation,
    NarrativeExport,
    Proposition,
    PropositionExtractionTrace,
    ReviewDecision,
    ReviewStatus,
    RunArtifact,
    RunStageTrace,
    SourceCategorisationRationale,
    SourceFamilyCandidate,
    SourceFetchAttempt,
    SourceFetchMetadata,
    SourceFragment,
    SourceInventoryArtifact,
    SourceInventoryRow,
    SourceParseTrace,
    SourceRecord,
    SourceSnapshot,
    SourceTargetLink,
)
from judit_llm import JuditLLMClient

from .cli_progress import null_pipeline_progress
from .compare import compare_propositions
from .derived_cache import (
    DerivedArtifactCache,
    build_derived_artifact_cache_hook,
    build_derived_artifact_cache_key,
)
from .export import build_bundle, export_bundle
from .extract import (
    EXTRACTION_PROMPT_VERSION_V2,
    EXTRACTION_SCHEMA_VERSION_V2,
    STRUCTURED_LIST_RULE_ID,
    _is_placeholder_locator,
    attach_judit_extraction_reuse,
    extract_propositions_from_source,
    parse_judit_extraction_meta,
    parse_structured_extraction_notes,
)
from .extraction_repair import classify_repairable_failure_type
from .file_input import load_case_file
from .fragment_types import fragment_type_from_locator
from .intake import content_hash, create_cluster, create_topic, slugify
from .proposition_completeness import build_proposition_completeness_assessments
from .proposition_dataset import (
    build_registry_comparison_config,
    filter_registry_case_sources_by_scope,
    validate_registry_divergence_inputs,
)
from .reviews import apply_review_to_assessment
from .scope_linking import build_scope_artifacts_for_run
from .sources import (
    SOURCE_ROLES,
    SourceIngestionService,
    SourceRegistryService,
    build_source_target_link,
    classify_source_categorisation,
)
from .sources.source_family_discovery import (
    candidates_for_included_ids,
    discover_related_for_registry_entry,
)


def _source_progress_label(source: SourceRecord) -> str:
    title = (source.title or "").strip()
    return title if title else source.id


def _proposition_at_index(propositions: list[Any], index: int) -> Any:
    if not propositions:
        raise ValueError("Cannot select proposition from an empty list.")
    return propositions[min(max(index, 0), len(propositions) - 1)]


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _canonicalize_legacy_review_status(status: ReviewStatus) -> ReviewStatus:
    if status in {ReviewStatus.DRAFT, ReviewStatus.IN_REVIEW}:
        return ReviewStatus.PROPOSED
    return status


def _build_cross_reference_key(proposition: Proposition) -> str:
    return (
        f"{slugify(proposition.jurisdiction)}:"
        f"{slugify(proposition.legal_subject)}:"
        f"{slugify((proposition.action or proposition.proposition_text)[:60])}"
    )


def _proposition_sequence_hint(proposition_id: str) -> str:
    match = re.search(r"(\d+)$", proposition_id)
    return match.group(1) if match else "000"


_LABEL_SEPARATOR = "\u2014"


def _opaque_machine_proposition_id(proposition: Proposition, seq_token: str) -> str:
    anchor = (
        str(proposition.fragment_locator or "").strip()
        or str(proposition.article_reference or "").strip()
        or str(proposition.source_fragment_id or "").strip()
    )
    basis = f"{proposition.source_record_id}\0{anchor}\0{seq_token}"
    return f"prop:{content_hash(basis)[:16]}"


def _fragment_segment_for_proposition_key(proposition: Proposition) -> str:
    raw = (
        proposition.fragment_locator
        or proposition.article_reference
        or proposition.source_fragment_id
        or ""
    )
    raw_str = str(raw).strip()
    if raw_str and not _is_placeholder_locator(raw_str):
        return slugify(raw_str)
    frag_id = str(proposition.source_fragment_id or "").strip()
    if frag_id:
        return slugify(frag_id)
    return slugify(proposition.source_record_id)


def _build_source_derived_proposition_key(proposition: Proposition, seq_token: str) -> str:
    instrument = slugify(proposition.source_record_id.strip())
    frag_part = _fragment_segment_for_proposition_key(proposition)
    try:
        seq_num = max(1, int(seq_token))
    except ValueError:
        seq_num = 1
    return f"{instrument}:{frag_part}:p{seq_num:03d}"


def _format_structured_list_locator_for_label(fragment_locator: str) -> str | None:
    """Readable segment for ``article:109:list:1-d-i``-style locators."""
    if ":list:" not in fragment_locator:
        return None
    parts = fragment_locator.split(":list:", 1)
    if len(parts) != 2:
        return None
    base, path = parts[0].strip(), parts[1].strip().split("/", 1)[0].strip()
    if not path:
        return None
    art_m = re.search(r"(?:article|section)[:/_-]?(\d+[a-z]?)", base, re.IGNORECASE)
    art_label = f"Art {art_m.group(1)}" if art_m else base
    bits = [p for p in path.replace("__", "-").split("-") if p]
    if len(bits) < 2:
        return None
    para, letter, *romans = bits
    seg = f"§{para}({letter})"
    for r in romans:
        seg += f"({r})"
    return f"{art_label} {seg}"


def _locator_display_segment(proposition: Proposition) -> str:
    loc = (proposition.fragment_locator or "").strip()
    if loc and not _is_placeholder_locator(loc):
        listed = _format_structured_list_locator_for_label(loc)
        if listed:
            return listed
        return loc
    article = (proposition.article_reference or "").strip()
    if article:
        return article
    return "Source"


def _derive_short_name(proposition: Proposition) -> str:
    ls = proposition.legal_subject.strip()
    act = proposition.action.strip()
    if ls or act:
        return " ".join(part for part in (ls, act) if part).strip()
    text = proposition.proposition_text.strip()
    return text[:200] if text else "Proposition"


def _derive_label(proposition: Proposition, short_name: str) -> str:
    loc = _locator_display_segment(proposition).strip()
    name = short_name.strip()
    return f"{loc} {_LABEL_SEPARATOR} {name}".strip()


def _derive_slug(label: str, short_name: str) -> str:
    raw = label if label.strip() else short_name
    slug_value = slugify(raw)
    return slug_value[:96] if len(slug_value) > 96 else slug_value


def _opaque_proposition_extraction_trace_id(proposition_id: str) -> str:
    basis = f"proposition_extraction_trace|{proposition_id}"
    return f"extract-trace:{content_hash(basis)[:16]}"


def _build_proposition_version_id(proposition: Proposition, run_id: str) -> str:
    seq_token = _proposition_sequence_hint(proposition.id)
    proposition_key = proposition.proposition_key or _build_source_derived_proposition_key(
        proposition,
        seq_token,
    )
    snapshot_anchor = proposition.source_snapshot_id or "snapshot-unknown"
    return f"pver:{proposition_key}:{slugify(snapshot_anchor)}:{slugify(run_id)}"


def _categorize_proposition(proposition: Proposition) -> tuple[list[str], list[str]]:
    categories: set[str] = {"obligation"}
    tags: set[str] = set()

    text = proposition.proposition_text.lower()
    if proposition.authority:
        categories.add("institutional")
        tags.add(slugify(proposition.authority))
    if proposition.required_documents:
        categories.add("documentary")
    if proposition.conditions:
        categories.add("conditional")
    if "inspect" in text or "inspection" in text:
        categories.add("oversight")
    if "record" in text or "register" in text:
        categories.add("record_keeping")
    if "submit" in text or "return" in text:
        categories.add("reporting")

    tags.update(slugify(item) for item in proposition.affected_subjects if item)
    tags.add(slugify(proposition.legal_subject))
    tags.add(slugify(proposition.jurisdiction))
    return sorted(categories), sorted(tag for tag in tags if tag)


def _build_proposition_records(
    *,
    propositions: list[Proposition],
    run_id: str,
    source_by_id: dict[str, Any],
    source_fragment_by_id: dict[str, Any],
) -> list[Proposition]:
    proposition_records: list[Proposition] = []
    cross_reference_index: dict[str, list[str]] = {}

    for proposition in propositions:
        resolved_source_snapshot_id = proposition.source_snapshot_id
        if not resolved_source_snapshot_id and proposition.source_fragment_id:
            fragment = source_fragment_by_id.get(str(proposition.source_fragment_id))
            if fragment is not None:
                resolved_source_snapshot_id = str(fragment.source_snapshot_id)
        if not resolved_source_snapshot_id:
            source = source_by_id.get(str(proposition.source_record_id))
            if source is not None and source.current_snapshot_id:
                resolved_source_snapshot_id = str(source.current_snapshot_id)

        categories, tags = _categorize_proposition(proposition)
        cross_reference_key = _build_cross_reference_key(proposition)
        seq_token = _proposition_sequence_hint(proposition.id)
        proposition_key = proposition.proposition_key or _build_source_derived_proposition_key(
            proposition,
            seq_token,
        )
        opaque_id = _opaque_machine_proposition_id(proposition, seq_token)
        short_name = (
            proposition.short_name.strip()
            if proposition.short_name.strip()
            else _derive_short_name(proposition)
        )
        label = (
            proposition.label.strip()
            if proposition.label.strip()
            else _derive_label(proposition, short_name)
        )
        slug = (
            proposition.slug.strip()
            if proposition.slug.strip()
            else _derive_slug(label, short_name)
        )
        proposition_payload = {
            **proposition.model_dump(mode="json"),
            "id": opaque_id,
            "source_snapshot_id": resolved_source_snapshot_id,
            "proposition_key": proposition_key,
            "short_name": short_name,
            "label": label,
            "slug": slug,
            "observed_in_run_id": run_id,
        }
        proposition_payload["proposition_version_id"] = _build_proposition_version_id(
            Proposition.model_validate(proposition_payload),
            run_id=run_id,
        )
        proposition_record = Proposition.model_validate(
            {
                **proposition_payload,
                "categories": categories,
                "tags": tags,
                "cross_reference_key": cross_reference_key,
                "review_status": (
                    proposition.review_status
                    if proposition.review_status == ReviewStatus.NEEDS_REVIEW
                    else ReviewStatus.PROPOSED
                ),
            }
        )
        proposition_records.append(proposition_record)
        cross_reference_index.setdefault(cross_reference_key, []).append(proposition_record.id)

    proposition_by_id = {item.id: item for item in proposition_records}
    for key, proposition_ids in cross_reference_index.items():
        if len(proposition_ids) <= 1:
            continue
        for proposition_id in proposition_ids:
            proposition_record = proposition_by_id[proposition_id]
            proposition_record.cross_reference_targets = [
                item for item in proposition_ids if item != proposition_id
            ]
            proposition_record.cross_reference_key = key

    return proposition_records


def _build_proposition_inventory(propositions: list[Proposition]) -> dict[str, Any]:
    by_jurisdiction: dict[str, list[str]] = {}
    by_category: dict[str, list[str]] = {}
    by_tag: dict[str, list[str]] = {}
    cross_reference_index: dict[str, list[str]] = {}
    lineage_index: dict[str, list[str]] = {}

    for proposition in propositions:
        by_jurisdiction.setdefault(proposition.jurisdiction, []).append(proposition.id)
        for category in proposition.categories:
            by_category.setdefault(category, []).append(proposition.id)
        for tag in proposition.tags:
            by_tag.setdefault(tag, []).append(proposition.id)
        if proposition.cross_reference_key:
            cross_reference_index.setdefault(proposition.cross_reference_key, []).append(
                proposition.id
            )
        if proposition.proposition_key:
            lineage_index.setdefault(proposition.proposition_key, []).append(
                proposition.proposition_version_id or proposition.id
            )

    return {
        "proposition_count": len(propositions),
        "proposition_ids": [item.id for item in propositions],
        "by_jurisdiction": by_jurisdiction,
        "categories": by_category,
        "tags": by_tag,
        "cross_reference_index": cross_reference_index,
        "lineage_index": lineage_index,
    }


def _build_proposition_review_decisions(propositions: list[Proposition]) -> list[ReviewDecision]:
    decisions: list[ReviewDecision] = []
    for proposition in propositions:
        decisions.append(
            ReviewDecision(
                id=f"review-proposition-{proposition.id}",
                target_type="proposition",
                target_id=proposition.id,
                previous_status=None,
                new_status=proposition.review_status,
                reviewer="system:extract",
                note="Initial proposition inventory registration.",
                metadata={
                    "source_record_id": proposition.source_record_id,
                    "source_fragment_id": proposition.source_fragment_id,
                    "cross_reference_key": proposition.cross_reference_key,
                },
            )
        )
    return decisions


def _build_stage_trace(
    *,
    stage_name: str,
    run_id: str,
    timestamp: str,
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    strategy_used: str,
    model_alias_used: str | None,
    started_at: float,
    started_at_iso: str | None = None,
    finished_at_iso: str | None = None,
    input_artifact_ids: list[str] | None = None,
    output_artifact_ids: list[str] | None = None,
    metrics: dict[str, Any] | None = None,
    status: str | None = None,
    warnings: list[str] | None = None,
    errors: list[str] | None = None,
) -> dict[str, Any]:
    duration_ms = max(0, int((perf_counter() - started_at) * 1000))
    resolved_started_at = started_at_iso or timestamp
    resolved_finished_at = finished_at_iso or _utc_now_iso()
    resolved_warnings = warnings or []
    resolved_errors = errors or []
    resolved_status = status or ("failed" if resolved_errors else "ok")
    return RunStageTrace(
        stage_name=stage_name,
        run_id=run_id,
        timestamp=timestamp,
        started_at=resolved_started_at,
        finished_at=resolved_finished_at,
        status=resolved_status,
        inputs=inputs,
        outputs=outputs,
        strategy_used=strategy_used,
        model_alias_used=model_alias_used,
        duration_ms=duration_ms,
        input_artifact_ids=input_artifact_ids or [],
        output_artifact_ids=output_artifact_ids or [],
        metrics=metrics or {},
        warnings=resolved_warnings,
        errors=resolved_errors,
    ).model_dump(mode="json")


def _build_source_fetch_metadata(
    *,
    sources: list[Any],
    traces: list[dict[str, Any]],
) -> list[SourceFetchMetadata]:
    metadata_records: list[SourceFetchMetadata] = []
    for source, trace in zip(sources, traces, strict=False):
        response_metadata = trace.get("adapter_trace")
        if not isinstance(response_metadata, dict):
            response_metadata = {}
        fetch_status = str(trace.get("fetch_status") or trace.get("decision") or "unknown")
        metadata_records.append(
            SourceFetchMetadata(
                id=f"fetch-{slugify(str(source.id))}",
                source_record_id=str(source.id),
                authority=str(trace.get("authority", "")),
                authority_source_id=str(trace.get("authority_source_id", "")) or None,
                source_identifier=str(trace.get("authority_source_id", "")) or None,
                citation=str(source.citation) if source.citation else None,
                source_url=str(source.source_url) if source.source_url else None,
                retrieved_at=source.retrieved_at,
                content_hash=str(source.content_hash or trace.get("content_hash", "")),
                fetch_status=fetch_status,
                response_metadata=response_metadata,
                raw_artifact_uri=(
                    str(trace.get("raw_artifact_uri")) if trace.get("raw_artifact_uri") else None
                ),
                parsed_artifact_uri=f"bundle://source_records/{source.id}",
                metadata={
                    "cache_key": str(trace.get("cache_key", "")) or None,
                    "version_id": str(trace.get("version_id", "")) or None,
                },
            )
        )
    return metadata_records


def _classify_proposition_extraction(
    *, notes: str, use_llm: bool
) -> tuple[Literal["heuristic", "llm", "fallback"], str, list[str]]:
    warnings: list[str] = []
    normalized = (notes or "").strip()
    if normalized == "heuristic extraction":
        if use_llm:
            warnings.append(
                "LLM extraction did not return propositions for this source; heuristic selection applied."
            )
            return (
                "fallback",
                "Heuristic normative sentence selection after the LLM extraction path did not produce usable rows.",
                warnings,
            )
        return (
            "heuristic",
            "Normative sentence selection over parsed source fragments.",
            warnings,
        )
    if use_llm:
        return ("llm", "Structured LLM JSON extraction from source text.", warnings)
    return (
        "heuristic",
        "Normative sentence selection over parsed source fragments.",
        warnings,
    )


def _proposition_extraction_confidence(
    method: Literal["heuristic", "llm", "fallback"],
) -> Literal["high", "medium", "low"]:
    if method == "fallback":
        return "low"
    if method == "llm":
        return "medium"
    return "medium"


def _build_proposition_extraction_traces(
    *,
    propositions: list[Proposition],
    use_llm: bool,
    extraction_prompt: dict[str, Any],
    extraction_strategy_version: str,
    extraction_hook: dict[str, Any],
    pipeline_version: str,
) -> list[PropositionExtractionTrace]:
    if not isinstance(extraction_prompt, dict):
        extraction_prompt = {}
    prompt_name = str(extraction_prompt.get("name", "extract.propositions.default"))
    prompt_ver = str(extraction_prompt.get("version", EXTRACTION_PROMPT_VERSION_V2))
    traces: list[PropositionExtractionTrace] = []
    for proposition in propositions:
        structured_meta = parse_structured_extraction_notes(proposition.notes)
        meta = parse_judit_extraction_meta(proposition.notes)
        if meta:
            em = str(meta.get("extraction_mode") or "")
            fb_used = bool(meta.get("fallback_used"))
            val_meta = [str(x) for x in (meta.get("validation_errors") or []) if str(x).strip()]
            if em == "heuristic":
                method: Literal["heuristic", "llm", "fallback"] = "heuristic"
            elif fb_used:
                method = "fallback"
            else:
                method = "llm"
            if method == "llm":
                extractor_name = "judit_pipeline.extract.model_json_v2"
                prompt_id = prompt_name
                prompt_version_val = str(meta.get("prompt_version") or prompt_ver)
                rule_id = None
                rule_version = None
            elif method == "fallback":
                extractor_name = "judit_pipeline.extract.heuristic_fallback"
                prompt_id = prompt_name
                prompt_version_val = str(meta.get("prompt_version") or prompt_ver)
                rule_id = STRUCTURED_LIST_RULE_ID if structured_meta else "extract.heuristic.normative_sentences"
                rule_version = extraction_strategy_version
            else:
                extractor_name = "judit_pipeline.extract.heuristic"
                prompt_id = None
                prompt_version_val = None
                rule_id = STRUCTURED_LIST_RULE_ID if structured_meta else "extract.heuristic.normative_sentences"
                rule_version = extraction_strategy_version

            if fb_used or proposition.review_status == ReviewStatus.NEEDS_REVIEW:
                confidence: Literal["high", "medium", "low"] = "low"
            elif method == "llm":
                mc = str(meta.get("model_confidence") or "").strip().lower()
                if mc == "high":
                    confidence = "high"
                elif mc == "low":
                    confidence = "low"
                else:
                    confidence = "medium"
            else:
                confidence = _proposition_extraction_confidence(method)

            reason = (
                "; ".join(val_meta)
                if val_meta
                else (
                    "Model-assisted extraction (trace metadata)."
                    if method == "llm"
                    else (
                        "Heuristic fallback after model path (trace metadata)."
                        if method == "fallback"
                        else "Normative sentence selection over parsed source fragments."
                    )
                )
            )
            trace_warnings = list(meta.get("trace_warnings") or []) if isinstance(meta.get("trace_warnings"), list) else []
            if method == "llm":
                eq_raw = meta.get("evidence_quote")
                if isinstance(eq_raw, str) and not eq_raw.strip():
                    tw_msg = (
                        "Evidence quote could not be matched exactly to source text (verbatim span unavailable)."
                    )
                    if tw_msg not in trace_warnings:
                        trace_warnings.append(tw_msg)
        else:
            method, reason, trace_warnings = _classify_proposition_extraction(
                notes=proposition.notes,
                use_llm=use_llm,
            )
            confidence = _proposition_extraction_confidence(method)
            if method == "llm":
                extractor_name = "judit_pipeline.extract.llm_json"
                prompt_id = prompt_name
                prompt_version_val = prompt_ver
                rule_id = None
                rule_version = None
            elif method == "fallback":
                extractor_name = "judit_pipeline.extract.heuristic_fallback"
                prompt_id = prompt_name
                prompt_version_val = prompt_ver
                rule_id = STRUCTURED_LIST_RULE_ID if structured_meta else "extract.heuristic.normative_sentences"
                rule_version = extraction_strategy_version
            else:
                extractor_name = "judit_pipeline.extract.heuristic"
                prompt_id = None
                prompt_version_val = None
                rule_id = STRUCTURED_LIST_RULE_ID if structured_meta else "extract.heuristic.normative_sentences"
                rule_version = extraction_strategy_version
            val_meta = []

        if meta is not None and isinstance(meta.get("evidence_quote"), str):
            evidence_text = str(meta.get("evidence_quote") or "")
        else:
            evidence_text = proposition.proposition_text
        evidence_locator = (
            proposition.fragment_locator
            or proposition.article_reference
            or (structured_meta or {}).get("evidence_locator")
            or proposition.article_reference
        )
        trace_status: Literal["success", "partial_success", "failed"] = (
            "success" if proposition.proposition_text.strip() else "failed"
        )
        errors_list: list[str] = []
        if trace_status == "failed":
            errors_list.append("empty proposition_text")
        elif val_meta:
            trace_status = "partial_success"

        signals: dict[str, Any] = {
            "pipeline_version": pipeline_version,
            "strategy_version": extraction_strategy_version,
            "derived_artifact_cache_status": extraction_hook.get("cache_status"),
        }
        if structured_meta:
            for key in (
                "parent_context",
                "list_marker",
                "structured_list_path",
                "duplicate_suppression_hint",
            ):
                if key in structured_meta and structured_meta[key] not in {"", None}:
                    signals[key] = structured_meta[key]
            sig_loc = structured_meta.get("evidence_locator")
            if sig_loc:
                signals["structured_evidence_locator"] = sig_loc
        if meta:
            signals["extraction_mode"] = meta.get("extraction_mode")
            signals["fallback_used"] = meta.get("fallback_used")
            signals["fallback_policy"] = meta.get("fallback_policy")
            peir = meta.get("pipeline_evidence_issue_records")
            if isinstance(peir, list) and peir:
                signals["pipeline_evidence_issue_records"] = peir
            ems = meta.get("evidence_match_strategy")
            if isinstance(ems, str) and ems.strip():
                signals["evidence_match_strategy"] = ems.strip()
            if isinstance(meta.get("evidence_quote"), str):
                signals["verbatim_evidence_text_empty"] = not bool(meta["evidence_quote"].strip())
            eitm = meta.get("estimated_input_tokens_max")
            if isinstance(eitm, int):
                signals["estimated_input_tokens_max"] = eitm
            if meta.get("context_window_risk") is True:
                signals["context_window_risk"] = True

        ext_mode_field = str(meta.get("extraction_mode")) if meta else None
        ma_raw = meta.get("model_alias") if meta else None
        model_alias_field = ma_raw if isinstance(ma_raw, str) else ma_raw
        fb_pol = str(meta.get("fallback_policy")) if meta and meta.get("fallback_policy") else None
        fb_u = bool(meta.get("fallback_used")) if meta else None
        schema_v = str(meta.get("schema_version")) if meta and meta.get("schema_version") else None

        traces.append(
            PropositionExtractionTrace(
                id=_opaque_proposition_extraction_trace_id(proposition.id),
                proposition_id=proposition.id,
                proposition_key=proposition.proposition_key,
                source_record_id=proposition.source_record_id,
                source_snapshot_id=proposition.source_snapshot_id,
                source_fragment_id=proposition.source_fragment_id,
                extraction_method=method,
                extractor_name=extractor_name,
                extractor_version=extraction_strategy_version,
                extraction_mode=ext_mode_field,
                model_alias=model_alias_field if isinstance(model_alias_field, str) else None,
                fallback_policy=fb_pol,
                fallback_used=fb_u,
                validation_errors=val_meta,
                schema_version=schema_v,
                started_at=None,
                finished_at=None,
                status=trace_status,
                prompt_id=prompt_id,
                prompt_version=prompt_version_val,
                rule_id=rule_id,
                rule_version=rule_version,
                evidence_text=evidence_text,
                evidence_locator=evidence_locator,
                confidence=confidence,
                reason=reason,
                warnings=trace_warnings,
                errors=errors_list,
                signals=signals,
            )
        )
    return traces


def _proposition_extraction_trace_metrics(
    traces: list[PropositionExtractionTrace],
) -> dict[str, int]:
    return {
        "proposition_count": len(traces),
        "traced_proposition_count": len(traces),
        "heuristic_extraction_count": sum(1 for item in traces if item.extraction_method == "heuristic"),
        "llm_extraction_count": sum(1 for item in traces if item.extraction_method == "llm"),
        "manual_extraction_count": sum(1 for item in traces if item.extraction_method == "manual"),
        "imported_extraction_count": sum(1 for item in traces if item.extraction_method == "imported"),
        "fallback_extraction_count": sum(1 for item in traces if item.extraction_method == "fallback"),
        "extraction_warning_count": sum(len(item.warnings) for item in traces),
        "extraction_error_count": sum(len(item.errors) for item in traces),
    }


def _build_fetch_attempt_metrics(attempts: list[SourceFetchAttempt]) -> dict[str, int]:
    status_counts = {
        "cache_hit_count": 0,
        "live_fetch_count": 0,
        "success_count": 0,
        "retryable_error_count": 0,
        "fatal_error_count": 0,
        "skipped_count": 0,
    }
    unique_hashes: set[str] = set()
    for attempt in attempts:
        if attempt.status == "cache_hit":
            status_counts["cache_hit_count"] += 1
        if attempt.method == "live_fetch":
            status_counts["live_fetch_count"] += 1
        if attempt.status == "success":
            status_counts["success_count"] += 1
        if attempt.status == "retryable_error":
            status_counts["retryable_error_count"] += 1
        if attempt.status == "fatal_error":
            status_counts["fatal_error_count"] += 1
        if attempt.status == "skipped":
            status_counts["skipped_count"] += 1
        if attempt.content_hash:
            unique_hashes.add(attempt.content_hash)
    return {
        **status_counts,
        "unique_content_hash_count": len(unique_hashes),
    }


def _build_parse_trace_metrics(
    *,
    parse_traces: list[SourceParseTrace],
    source_snapshots: list[Any],
    source_fragments: list[Any],
) -> dict[str, int]:
    parser_success_count = sum(1 for item in parse_traces if item.status == "success")
    parser_partial_success_count = sum(
        1 for item in parse_traces if item.status == "partial_success"
    )
    parser_failed_count = sum(1 for item in parse_traces if item.status == "failed")
    parser_skipped_count = sum(1 for item in parse_traces if item.status == "skipped")
    parsed_snapshot_ids = {
        str(item.source_snapshot_id)
        for item in parse_traces
        if item.status in {"success", "partial_success"}
    }
    fragment_hashes = {
        str(getattr(item, "text_hash", "") or getattr(item, "fragment_hash", ""))
        for item in source_fragments
        if str(getattr(item, "text_hash", "") or getattr(item, "fragment_hash", "")).strip()
    }
    return {
        "snapshot_count": len(source_snapshots),
        "parsed_snapshot_count": len(parsed_snapshot_ids),
        "fragment_count": len(source_fragments),
        "parser_success_count": parser_success_count,
        "parser_partial_success_count": parser_partial_success_count,
        "parser_failed_count": parser_failed_count,
        "parser_skipped_count": parser_skipped_count,
        "unique_fragment_hash_count": len(fragment_hashes),
    }


def _build_source_inventory_artifact(
    *,
    run_id: str,
    sources: list[Any],
    fetch_metadata: list[SourceFetchMetadata],
) -> tuple[
    SourceInventoryArtifact,
    list[SourceCategorisationRationale],
    list[SourceTargetLink],
]:
    fetch_by_source_id = {item.source_record_id: item for item in fetch_metadata}
    primary_target_source = next(
        (
            source
            for source in sources
            if isinstance(source.metadata, dict) and bool(source.metadata.get("is_target_source"))
        ),
        sources[0] if sources else None,
    )
    primary_target_metadata = (
        primary_target_source.metadata
        if primary_target_source is not None and isinstance(primary_target_source.metadata, dict)
        else {}
    )
    primary_target_source_id = (
        str(primary_target_source.id) if primary_target_source is not None else None
    )
    primary_target_citation = (
        str(primary_target_source.citation)
        if primary_target_source is not None and str(primary_target_source.citation).strip()
        else None
    )
    primary_target_instrument_id = (
        str(primary_target_metadata.get("instrument_id") or "")
        or primary_target_citation
        or primary_target_source_id
    )
    primary_target_title = (
        str(primary_target_source.title)
        if primary_target_source is not None and str(primary_target_source.title).strip()
        else None
    )
    rows: list[SourceInventoryRow] = []
    rationales: list[SourceCategorisationRationale] = []
    target_links: list[SourceTargetLink] = []
    for source in sources:
        source_metadata = source.metadata if isinstance(source.metadata, dict) else {}
        fetch_item = fetch_by_source_id.get(str(source.id))
        target_link = build_source_target_link(
            source=source,
            primary_target_source_id=primary_target_source_id,
            primary_target_citation=primary_target_citation,
            primary_target_instrument_id=primary_target_instrument_id,
            primary_target_title=primary_target_title,
        )
        rationale = classify_source_categorisation(
            source=source,
            primary_target_citation=primary_target_citation,
            target_link=target_link,
        )
        relationship = rationale.relationship_to_analysis
        if rationale.source_role == "delegated_act" and relationship == "analysis_target":
            relationship = "implements_target"
        if rationale.source_role == "unknown":
            relationship = "unknown"
        resolved_role = rationale.source_role if rationale.source_role in SOURCE_ROLES else "unknown"
        row_id = f"source-inventory-{slugify(str(source.id))}"
        rows.append(
            SourceInventoryRow(
                id=row_id,
                source_record_id=str(source.id),
                jurisdiction=str(source.jurisdiction),
                instrument_id=str(source_metadata.get("instrument_id") or source.citation or source.id),
                title=str(source.title),
                instrument_type=str(source.kind),
                status=str(source.status),
                version_id=str(source.version_id) if source.version_id else None,
                consolidation_date=source.as_of_date,
                source_url=str(source.source_url) if source.source_url else None,
                citation=str(source.citation) if source.citation else None,
                content_hash=str(source.content_hash or ""),
                source_role=resolved_role,
                relationship_to_analysis=relationship,
                fetch_metadata_id=fetch_item.id if fetch_item else None,
                target_link_id=target_link.id,
                metadata={
                    "provenance": source.provenance,
                    "authoritative_locator": source.authoritative_locator,
                },
            )
        )
        target_links.append(target_link)
        rationales.append(
            rationale.model_copy(
                update={
                    "source_inventory_row_id": row_id,
                    "source_target_link_id": target_link.id,
                    "source_role": resolved_role,
                    "relationship_to_analysis": relationship,
                }
            )
        )
    rows = sorted(rows, key=lambda item: item.source_record_id)
    rationales = sorted(rationales, key=lambda item: item.source_record_id)
    target_links = sorted(target_links, key=lambda item: item.source_record_id)
    serialized_rows = [row.model_dump(mode="json") for row in rows]
    inventory_hash = content_hash(json.dumps(serialized_rows, sort_keys=True))
    return (
        SourceInventoryArtifact(
            id=f"artifact-{run_id}-source-inventory",
            run_id=run_id,
            rows=rows,
            content_hash=inventory_hash,
            metadata={"row_count": len(rows)},
        ),
        rationales,
        target_links,
    )


def _build_comparison_pairs(
    *,
    propositions: list[Any],
    sources: list[Any],
    comparison_cfg: dict[str, Any],
) -> list[tuple[Any, Any]]:
    jurisdiction_a = comparison_cfg.get("jurisdiction_a", sources[0].jurisdiction)
    jurisdiction_b = comparison_cfg.get(
        "jurisdiction_b",
        sources[1].jurisdiction if len(sources) > 1 else sources[0].jurisdiction,
    )

    props_a = [item for item in propositions if item.jurisdiction == jurisdiction_a]
    props_b = [item for item in propositions if item.jurisdiction == jurisdiction_b]

    if not props_a:
        raise ValueError(f"No propositions extracted for jurisdiction_a={jurisdiction_a!r}.")
    if not props_b:
        raise ValueError(f"No propositions extracted for jurisdiction_b={jurisdiction_b!r}.")

    candidates_cfg = comparison_cfg.get("candidates")
    if not isinstance(candidates_cfg, list) or not candidates_cfg:
        proposition_index = int(comparison_cfg.get("proposition_index", 0))
        return [
            (
                _proposition_at_index(props_a, proposition_index),
                _proposition_at_index(props_b, proposition_index),
            )
        ]

    pairs: list[tuple[Any, Any]] = []
    for candidate in candidates_cfg:
        if not isinstance(candidate, dict):
            continue
        candidate_jurisdiction_a = candidate.get("jurisdiction_a", jurisdiction_a)
        candidate_jurisdiction_b = candidate.get("jurisdiction_b", jurisdiction_b)
        candidate_props_a = [
            item for item in propositions if item.jurisdiction == candidate_jurisdiction_a
        ]
        candidate_props_b = [
            item for item in propositions if item.jurisdiction == candidate_jurisdiction_b
        ]
        if not candidate_props_a or not candidate_props_b:
            continue

        shared_index = int(candidate.get("proposition_index", 0))
        proposition_index_a = int(candidate.get("proposition_index_a", shared_index))
        proposition_index_b = int(candidate.get("proposition_index_b", shared_index))
        pairs.append(
            (
                _proposition_at_index(candidate_props_a, proposition_index_a),
                _proposition_at_index(candidate_props_b, proposition_index_b),
            )
        )

    if not pairs:
        raise ValueError("Comparison candidates did not resolve to any valid proposition pairs.")

    return pairs


def _snapshot_content_hash_for_extraction_job(
    *,
    source: SourceRecord,
    fragment: SourceFragment | None,
    source_snapshots: list[SourceSnapshot],
) -> str:
    if fragment is not None:
        frag_hash = str(fragment.text_hash or fragment.fragment_hash or "").strip()
        if frag_hash:
            return frag_hash
    snap_id: str | None = None
    if fragment is not None:
        sid = str(fragment.source_snapshot_id or "").strip()
        snap_id = sid or None
    if not snap_id and source.current_snapshot_id:
        snap_id = str(source.current_snapshot_id).strip()
    if snap_id:
        for sn in source_snapshots:
            if str(sn.id) == snap_id:
                ch = str(sn.content_hash or "").strip()
                if ch:
                    return ch
    return str(source.content_hash or "").strip()


def _clone_propositions_for_shared_content_hash(
    template: list[Proposition],
    *,
    source: SourceRecord,
    fragment: SourceFragment | None,
    shared_content_hash: str,
) -> list[Proposition]:
    cloned: list[Proposition] = []
    for item in template:
        reuse_payload = {
            "source_content_hash": shared_content_hash,
            "original_source_record_id": item.source_record_id,
            "original_source_snapshot_id": str(item.source_snapshot_id or ""),
            "reused_for_source_record_id": source.id,
            "reused_for_jurisdiction": source.jurisdiction,
        }
        notes = attach_judit_extraction_reuse(item.notes or "", reuse_payload)
        p = item.model_copy(
            deep=True,
            update={
                "source_record_id": source.id,
                "jurisdiction": source.jurisdiction,
                "authority": source.title or item.authority,
                "citation": source.citation or item.citation,
                "notes": notes,
            },
        )
        if fragment is not None:
            p.source_fragment_id = fragment.id
            p.fragment_locator = fragment.locator
            p.source_snapshot_id = fragment.source_snapshot_id
        elif source.current_snapshot_id:
            p.source_snapshot_id = source.current_snapshot_id
        cloned.append(p)
    return cloned


def _build_findings_and_observations(
    *,
    assessments: list[DivergenceAssessment],
    sources: list[Any],
    source_fragments: list[Any],
    propositions: list[Proposition],
) -> tuple[list[DivergenceFinding], list[DivergenceObservation], list[DivergenceAssessment]]:
    source_by_id = {str(source.id): source for source in sources}
    fragment_by_record_id = {
        str(fragment.source_record_id): str(fragment.id) for fragment in source_fragments
    }
    proposition_by_id = {str(proposition.id): proposition for proposition in propositions}
    findings_by_id: dict[str, DivergenceFinding] = {}
    observations: list[DivergenceObservation] = []
    compatibility_assessments: list[DivergenceAssessment] = []

    for assessment in assessments:
        source_ids = [str(item) for item in assessment.sources_checked]
        source_snapshot_ids = [
            str(source.current_snapshot_id)
            for source_id in source_ids
            if (source := source_by_id.get(source_id)) and source.current_snapshot_id
        ]
        as_of_candidates = [
            source.as_of_date
            for source_id in source_ids
            if (source := source_by_id.get(source_id)) and source.as_of_date
        ]
        proposition = proposition_by_id.get(str(assessment.proposition_id))
        comparator = proposition_by_id.get(str(assessment.comparator_proposition_id))

        primary_fragment_id = (
            str(proposition.source_fragment_id)
            if proposition and proposition.source_fragment_id
            else fragment_by_record_id.get(str(proposition.source_record_id))
            if proposition
            else None
        )
        comparator_fragment_id = (
            str(comparator.source_fragment_id)
            if comparator and comparator.source_fragment_id
            else fragment_by_record_id.get(str(comparator.source_record_id))
            if comparator
            else None
        )
        supporting_fragment_ids = [
            fragment_by_record_id[source_id]
            for source_id in source_ids
            if source_id in fragment_by_record_id
            and fragment_by_record_id[source_id]
            not in {primary_fragment_id, comparator_fragment_id}
        ]
        evidence_limitations: list[str] = []
        if primary_fragment_id is None:
            evidence_limitations.append(
                "primary fragment inferred from source record fallback or unavailable"
            )
        if comparator_fragment_id is None:
            evidence_limitations.append(
                "comparator fragment inferred from source record fallback or unavailable"
            )
        if not supporting_fragment_ids:
            evidence_limitations.append("no additional supporting fragments inferred")

        context_note = (
            "Deterministic evidence pass from proposition/source links; "
            "not yet semantic quote-span alignment."
        )
        why_these_fragments = (
            "Primary/comparator use proposition-linked source fragments when available, "
            "otherwise first source fragment per source record. Supporting fragments are "
            "remaining checked-source fragments."
        )
        observation = DivergenceObservation.model_validate(
            {
                **assessment.model_dump(mode="json"),
                "source_snapshot_ids": source_snapshot_ids,
                "primary_source_fragment_id": primary_fragment_id,
                "comparator_source_fragment_id": comparator_fragment_id,
                "supporting_source_fragment_ids": supporting_fragment_ids,
                "common_ancestor_fragment_id": None,
                "context_note": context_note,
                "why_these_fragments": why_these_fragments,
                "as_of_date": max(as_of_candidates) if as_of_candidates else None,
                "metadata": {
                    **assessment.metadata,
                    "evidence_selection_limitations": evidence_limitations,
                },
            }
        )
        observations.append(observation)
        compatibility_assessments.append(
            DivergenceAssessment.model_validate(observation.model_dump(mode="json"))
        )

        finding_id = str(
            observation.finding_id
            or f"finding-{observation.proposition_id}-{observation.comparator_proposition_id}"
        )
        if finding_id not in findings_by_id:
            findings_by_id[finding_id] = DivergenceFinding(
                id=finding_id,
                proposition_id=observation.proposition_id,
                comparator_proposition_id=observation.comparator_proposition_id,
                jurisdiction_a=observation.jurisdiction_a,
                jurisdiction_b=observation.jurisdiction_b,
                common_ancestor=observation.common_ancestor,
                metadata={},
            )

    return list(findings_by_id.values()), observations, compatibility_assessments


def apply_assessment_review_decision(
    *,
    bundle: dict[str, Any],
    assessment_id: str,
    new_status: str,
    reviewer: str,
    note: str,
    edited_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    observations_raw = bundle.get(
        "divergence_observations",
        bundle.get("divergence_assessments", []),
    )
    if not isinstance(observations_raw, list):
        raise ValueError("Bundle divergence_observations/divergence_assessments must be a list.")

    status = ReviewStatus(new_status)
    decision_payload: dict[str, Any] | None = None
    updated_assessments: list[dict[str, Any]] = []
    for item in observations_raw:
        observation = DivergenceObservation.model_validate(item)
        if observation.id != assessment_id:
            updated_assessments.append(observation.model_dump(mode="json"))
            continue
        updated, decision = apply_review_to_assessment(
            assessment=DivergenceAssessment.model_validate(observation.model_dump(mode="json")),
            new_status=status,
            reviewer=reviewer,
            note=note,
            edited_fields=edited_fields,
        )
        updated_assessments.append(updated.model_dump(mode="json"))
        decision_payload = decision.model_dump(mode="json")

    if decision_payload is None:
        raise ValueError(f"Assessment {assessment_id!r} was not found in the bundle.")

    existing_decisions = bundle.get("review_decisions", [])
    if not isinstance(existing_decisions, list):
        existing_decisions = []
    existing_decisions.append(decision_payload)

    run = bundle.get("run", {})
    run_review_ids = run.get("review_decision_ids")
    if not isinstance(run_review_ids, list):
        run_review_ids = []
    run_review_ids.append(decision_payload["id"])
    run["review_decision_ids"] = run_review_ids

    stage_traces = bundle.get("stage_traces")
    if not isinstance(stage_traces, list):
        stage_traces = []
        bundle["stage_traces"] = stage_traces
    stage_traces.append(
        _build_stage_trace(
            stage_name="human review transition",
            run_id=str(run.get("id", "run-unknown")),
            timestamp=_utc_now_iso(),
            inputs={
                "target_type": "divergence_assessment",
                "target_id": assessment_id,
                "new_status": status.value,
                "reviewer": reviewer,
                "note": note,
            },
            outputs={
                "review_decision_id": decision_payload["id"],
                "edited_fields": edited_fields or {},
            },
            strategy_used="manual_review_decision_apply",
            model_alias_used=None,
            started_at=perf_counter(),
        )
    )

    bundle["divergence_observations"] = updated_assessments
    bundle["divergence_assessments"] = updated_assessments
    bundle["review_decisions"] = existing_decisions
    bundle["run"] = run
    return bundle


def _resolve_cache_dir(
    *,
    explicit_path: str | None,
    case_data: dict[str, Any],
    case_field: str,
    env_var: str,
    default_path: Path,
) -> tuple[Path, str]:
    if explicit_path:
        return Path(explicit_path), "cli_flag"

    case_value = case_data.get(case_field)
    if isinstance(case_value, str) and case_value.strip():
        return Path(case_value), "case_file"

    env_value = os.getenv(env_var)
    if isinstance(env_value, str) and env_value.strip():
        return Path(env_value), "env_var"

    return default_path, "default_temp"


def _resolve_extraction_and_divergence(
    *,
    use_llm: bool,
    extraction_mode: str | None,
    extraction_execution_mode: str | None,
    extraction_fallback: str | None,
    divergence_reasoning: str | None,
    case_data: dict[str, Any],
) -> tuple[str, str, str, str]:
    case_ex = case_data.get("extraction")
    cx = case_ex if isinstance(case_ex, dict) else {}

    em = extraction_mode
    if em is None and isinstance(cx.get("mode"), str) and cx["mode"].strip():
        em = cx["mode"].strip()
    if em is None:
        em = "local" if use_llm else "heuristic"

    ef = extraction_fallback
    if ef is None:
        if isinstance(cx.get("fallback_policy"), str) and cx["fallback_policy"].strip():
            ef = cx["fallback_policy"].strip()
        else:
            ef = "fallback"
    elif ef == "fallback" and isinstance(cx.get("fallback_policy"), str) and cx["fallback_policy"].strip():
        ef = cx["fallback_policy"].strip()

    dr = divergence_reasoning
    if dr is None and isinstance(cx.get("divergence_reasoning"), str) and cx["divergence_reasoning"].strip():
        dr = cx["divergence_reasoning"].strip()
    if dr is None:
        dr = "frontier" if use_llm else "none"
    eem = extraction_execution_mode
    if eem is None and isinstance(cx.get("execution_mode"), str) and cx["execution_mode"].strip():
        eem = cx["execution_mode"].strip()
    if eem is None:
        eem = "interactive"

    if em not in {"heuristic", "local", "frontier"}:
        raise ValueError(f"invalid extraction_mode: {em!r}")
    if eem not in {"interactive", "batch"}:
        raise ValueError(f"invalid extraction_execution_mode: {eem!r}")
    if ef not in {"fallback", "fail_closed", "mark_needs_review"}:
        raise ValueError(f"invalid extraction_fallback: {ef!r}")
    if dr not in {"none", "frontier"}:
        raise ValueError(f"invalid divergence_reasoning: {dr!r}")
    if eem == "batch" and em != "frontier":
        raise ValueError("extraction_execution_mode='batch' requires extraction_mode='frontier'")
    return em, eem, ef, dr


def _resolve_model_error_policy(
    case_data: dict[str, Any],
) -> Literal["continue_with_fallback", "stop_repairable", "continue_repairable"]:
    cx = case_data.get("extraction")
    if isinstance(cx, dict):
        p = str(cx.get("model_error_policy") or "").strip()
        if p == "stop_repairable":
            return "stop_repairable"
        if p == "continue_repairable":
            return "continue_repairable"
        if p == "continue_with_fallback":
            return "continue_with_fallback"
    return "continue_with_fallback"


def _frontier_aggregate_cache_blocked(
    *,
    extraction_failures: list[dict[str, Any]],
    propositions: list[Proposition],
) -> bool:
    if extraction_failures:
        return True
    for prop in propositions:
        meta = parse_judit_extraction_meta(prop.notes)
        if meta and bool(meta.get("fallback_used")) and str(meta.get("extraction_mode")) == "frontier":
            return True
    return False


def hydrate_intake_from_export_bundle(bundle_dict: dict[str, Any]) -> tuple[
    list[SourceRecord],
    list[SourceSnapshot],
    list[SourceFragment],
    list[SourceParseTrace],
    list[SourceFetchMetadata],
    list[SourceFetchAttempt],
    list[ReviewDecision],
    list[dict[str, Any]],
]:
    sources = [
        SourceRecord.model_validate(x)
        for x in (bundle_dict.get("source_records") or bundle_dict.get("sources") or [])
        if isinstance(x, dict)
    ]
    snapshots = [
        SourceSnapshot.model_validate(x) for x in bundle_dict.get("source_snapshots") or [] if isinstance(x, dict)
    ]
    fragments = [
        SourceFragment.model_validate(x) for x in bundle_dict.get("source_fragments") or [] if isinstance(x, dict)
    ]
    parse_traces = [
        SourceParseTrace.model_validate(x) for x in bundle_dict.get("source_parse_traces") or [] if isinstance(x, dict)
    ]
    fetch_meta = [
        SourceFetchMetadata.model_validate(x) for x in bundle_dict.get("source_fetch_metadata") or [] if isinstance(x, dict)
    ]
    fetch_attempts = [
        SourceFetchAttempt.model_validate(x) for x in bundle_dict.get("source_fetch_attempts") or [] if isinstance(x, dict)
    ]
    review_decisions_h = [
        ReviewDecision.model_validate(x) for x in bundle_dict.get("review_decisions") or [] if isinstance(x, dict)
    ]
    synthetic_traces: list[dict[str, Any]] = []
    for source in sources:
        synthetic_traces.append(
            {
                "authority": "",
                "authority_source_id": "",
                "fetch_status": "export_bundle_replay",
                "cache_key": None,
                "content_hash": str(source.content_hash or ""),
                "raw_artifact_uri": None,
                "adapter_trace": {"replay": True},
                "decision": "replay",
                "version_id": str(source.version_id or ""),
            }
        )
    return (
        sources,
        snapshots,
        fragments,
        parse_traces,
        fetch_meta,
        fetch_attempts,
        review_decisions_h,
        synthetic_traces,
    )


def _extraction_cache_model_alias(extraction_mode: str, llm_client: JuditLLMClient | None) -> str | None:
    if llm_client is None or extraction_mode not in {"local", "frontier"}:
        return None
    if extraction_mode == "frontier":
        return llm_client.settings.frontier_extract_model
    return llm_client.settings.local_extract_model


def _resolve_case_extraction_limits(case_data: dict[str, Any]) -> tuple[list[str], int]:
    """focus_scopes and max_propositions_per_source from case ``extraction`` object."""
    raw = case_data.get("extraction")
    cx = raw if isinstance(raw, dict) else {}
    scopes_raw = cx.get("focus_scopes")
    scopes: list[str] = []
    if isinstance(scopes_raw, list):
        scopes = [str(s).strip() for s in scopes_raw if str(s).strip()]
    max_p = cx.get("max_propositions_per_source")
    limit = 4
    if isinstance(max_p, int) and max_p > 0:
        limit = max_p
    elif isinstance(max_p, str) and max_p.strip().isdigit():
        v = int(max_p.strip())
        if v > 0:
            limit = v
    return scopes, limit


def _normalize_locator_for_match(locator: str | None) -> str:
    raw = str(locator or "").strip().lower()
    if not raw:
        return ""
    base = raw.split("|chunk:", 1)[0].strip()
    m = re.match(r"^(article|annex):(.+)$", base)
    if not m:
        return base
    kind, value = m.group(1), m.group(2).strip()
    if kind == "article" and value.isdigit():
        return f"article:{int(value)}"
    if kind == "annex":
        return f"annex:{value.lower()}"
    return f"{kind}:{value}"


def _resolve_extraction_selection_config(case_data: dict[str, Any]) -> dict[str, Any]:
    raw = case_data.get("extraction")
    cx = raw if isinstance(raw, dict) else {}
    required_raw = cx.get("required_fragment_locators")
    required_locators: set[str] = set()
    if isinstance(required_raw, list):
        for item in required_raw:
            key = _normalize_locator_for_match(str(item))
            if key:
                required_locators.add(key)
    include_annexes = bool(cx.get("include_annexes"))
    focus_terms: list[str] = []
    focus_raw = cx.get("focus_terms")
    if isinstance(focus_raw, list):
        focus_terms = [str(x).strip().lower() for x in focus_raw if str(x).strip()]
    if not focus_terms:
        scopes_raw = cx.get("focus_scopes")
        if isinstance(scopes_raw, list):
            focus_terms = [str(x).strip().lower() for x in scopes_raw if str(x).strip()]
    min_chars = 40
    min_chars_raw = cx.get("min_fragment_chars_for_extraction")
    if isinstance(min_chars_raw, int) and min_chars_raw > 0:
        min_chars = min_chars_raw
    elif isinstance(min_chars_raw, str) and min_chars_raw.strip().isdigit():
        parsed = int(min_chars_raw.strip())
        if parsed > 0:
            min_chars = parsed
    selection_mode_raw = str(cx.get("fragment_selection_mode") or "").strip().lower()
    selection_mode = (
        selection_mode_raw
        if selection_mode_raw in {"required_only", "required_plus_focus", "all_matching"}
        else "all_matching"
    )
    return {
        "required_locators": required_locators,
        "include_annexes": include_annexes,
        "focus_terms": focus_terms,
        "min_fragment_chars": min_chars,
        "fragment_selection_mode": selection_mode,
    }


def _focus_match_strength(fragment_text: str, focus_terms: list[str]) -> tuple[bool, int]:
    if not focus_terms:
        return False, 0
    body = fragment_text.lower()
    matched: set[str] = set()
    multiword_hit = False
    for term in focus_terms:
        if not term:
            continue
        if term in body:
            matched.add(term)
            if " " in term:
                multiword_hit = True
    score = len(matched)
    strong = score >= 2 or multiword_hit
    return strong, score


def _is_non_operative_fragment(fragment_type: str) -> bool:
    return fragment_type in {"recital", "heading", "table", "unknown", "document"}


def _select_fragment_for_extraction(
    *,
    ext_mode: str,
    source: SourceRecord,
    fragment: SourceFragment | None,
    required_locators: set[str],
    include_annexes: bool,
    focus_terms: list[str],
    min_fragment_chars: int,
    fragment_selection_mode: str,
) -> tuple[bool, str, str | None, str, str, int]:
    fragment_locator = str(
        fragment.locator if fragment is not None else source.authoritative_locator or ""
    ).strip()
    fragment_type = str(
        fragment.fragment_type
        if fragment is not None
        else fragment_type_from_locator(fragment_locator or "document:full")
    ).strip() or "unknown"
    fragment_text = str(fragment.fragment_text if fragment is not None else source.authoritative_text or "")
    fragment_text_length = len(fragment_text)
    locator_key = _normalize_locator_for_match(fragment_locator)
    focus_strong, _focus_score = _focus_match_strength(fragment_text, focus_terms)
    selected_for_extraction = False
    selection_reason = "skipped_no_focus_match"
    skip_reason: str | None = "no_focus_match"
    if ext_mode not in {"frontier", "local"}:
        selected_for_extraction = True
        selection_reason = "focus_term_match"
        skip_reason = None
    elif locator_key in required_locators:
        selected_for_extraction = True
        selection_reason = "required_locator"
        skip_reason = None
    elif include_annexes and fragment_type == "annex":
        selected_for_extraction = True
        selection_reason = "annex_included"
        skip_reason = None
    elif fragment_selection_mode == "required_only":
        selection_reason = "skipped_not_required_in_required_only_mode"
        skip_reason = "skipped_not_required_in_required_only_mode"
    elif not required_locators and not include_annexes and not focus_terms:
        selected_for_extraction = True
        selection_reason = "focus_term_match"
        skip_reason = None
    elif focus_strong:
        selected_for_extraction = True
        selection_reason = "focus_term_match"
        skip_reason = None
    elif fragment_selection_mode == "required_plus_focus":
        selection_reason = "skipped_no_focus_match"
        skip_reason = "no_focus_match"
    elif fragment_text_length < min_fragment_chars:
        selection_reason = "skipped_too_short"
        skip_reason = "too_short"
    elif _is_non_operative_fragment(fragment_type):
        selection_reason = "skipped_non_operative"
        skip_reason = "non_operative"
    return (
        selected_for_extraction,
        selection_reason,
        skip_reason,
        fragment_locator,
        fragment_type,
        fragment_text_length,
    )


def build_bundle_from_case(
    case_data: dict[str, Any],
    use_llm: bool = False,
    extraction_mode: str | None = None,
    extraction_execution_mode: str | None = None,
    extraction_fallback: str | None = None,
    divergence_reasoning: str | None = None,
    source_cache_dir: str | None = None,
    derived_cache_dir: str | None = None,
    progress: Any | None = None,
    intake_bundle: dict[str, Any] | None = None,
    extraction_repair_targets: set[tuple[str, str | None]] | None = None,
    extraction_repair_kept_propositions: list[Proposition] | None = None,
    retry_failed_llm: bool = False,
) -> dict[str, Any]:
    pr = progress if progress is not None else null_pipeline_progress()
    topic_cfg = case_data["topic"]
    cluster_cfg = case_data["cluster"]
    comparison_cfg = case_data.get("comparison", {})
    narrative_cfg = case_data.get("narrative", {})
    run_notes = case_data.get("run_notes", "")
    run_id = case_data.get("run_id", "run-001")
    stage_traces: list[dict[str, Any]] = []
    prompt_cfg = case_data.get("prompts", {})
    pipeline_version = str(case_data.get("pipeline_version", "0.1.0"))
    strategy_versions_raw = case_data.get("strategy_versions", {})
    strategy_versions = strategy_versions_raw if isinstance(strategy_versions_raw, dict) else {}

    topic = create_topic(
        name=topic_cfg["name"],
        description=topic_cfg.get("description", ""),
        subject_tags=topic_cfg.get("subject_tags", []),
    )
    cluster = create_cluster(
        topic=topic,
        name=cluster_cfg["name"],
        description=cluster_cfg.get("description", ""),
    )

    intake_started_at = perf_counter()
    intake_timestamp = _utc_now_iso()
    source_cache_dir_path, source_cache_resolution = _resolve_cache_dir(
        explicit_path=source_cache_dir,
        case_data=case_data,
        case_field="source_cache_dir",
        env_var="JUDIT_SOURCE_CACHE_DIR",
        default_path=Path(tempfile.gettempdir()) / "judit" / "source-snapshots",
    )
    derived_cache_dir_path, derived_cache_resolution = _resolve_cache_dir(
        explicit_path=derived_cache_dir,
        case_data=case_data,
        case_field="derived_cache_dir",
        env_var="JUDIT_DERIVED_CACHE_DIR",
        default_path=Path(tempfile.gettempdir()) / "judit" / "derived-artifacts",
    )
    derived_cache = DerivedArtifactCache(cache_dir=derived_cache_dir_path)
    replay_from_export = intake_bundle is not None
    if replay_from_export:
        assert intake_bundle is not None
        (
            sources,
            source_snapshots,
            source_fragments,
            source_parse_traces,
            source_fetch_metadata,
            source_fetch_attempts,
            review_decisions,
            intake_result_traces,
        ) = hydrate_intake_from_export_bundle(intake_bundle)
        raw_source_count = len(sources)
        pr.stage("Source intake", detail=f"{raw_source_count} source(s) (replay export bundle)")
    else:
        source_ingestion = SourceIngestionService(cache_dir=source_cache_dir_path)
        raw_source_count = len(case_data["sources"]) if isinstance(case_data.get("sources"), list) else 0
        pr.stage("Source intake", detail=f"{raw_source_count} raw source(s)")
        intake_result = source_ingestion.ingest_sources(case_data["sources"])
        sources = intake_result.sources
        source_snapshots = intake_result.snapshots
        source_fragments = intake_result.fragments
        source_parse_traces = intake_result.parse_traces
        review_decisions = intake_result.reviews
        source_fetch_metadata = _build_source_fetch_metadata(
            sources=sources,
            traces=intake_result.traces,
        )
        source_fetch_attempts = intake_result.attempts
        intake_result_traces = intake_result.traces
    pr.stage(
        "Source parsing",
        detail=f"{len(source_parse_traces)} parse trace(s), {len(source_snapshots)} snapshot(s)",
    )
    pr.stage(
        "Source fragmentation",
        detail=f"{len(source_fragments)} fragment(s)",
    )
    fetch_attempt_metrics = _build_fetch_attempt_metrics(source_fetch_attempts)
    parse_trace_metrics = _build_parse_trace_metrics(
        parse_traces=source_parse_traces,
        source_snapshots=source_snapshots,
        source_fragments=source_fragments,
    )
    (
        source_inventory_artifact,
        source_categorisation_rationales,
        source_target_links,
    ) = _build_source_inventory_artifact(
        run_id=run_id,
        sources=sources,
        fetch_metadata=source_fetch_metadata,
    )
    cluster.source_record_ids = [source.id for source in sources]
    source_by_id = {source.id: source for source in sources}
    source_fragment_by_id = {fragment.id: fragment for fragment in source_fragments}
    fragments_by_record: defaultdict[str, list[SourceFragment]] = defaultdict(list)
    for fragment in source_fragments:
        fragments_by_record[fragment.source_record_id].append(fragment)
    for sid in fragments_by_record:
        fragments_by_record[sid].sort(key=lambda f: (f.order_index is None, f.order_index or 0))
    extraction_jobs: list[tuple[SourceRecord, SourceFragment | None]] = []
    matched_source_ids: set[str] = set()
    for frag in source_fragments:
        source = source_by_id.get(frag.source_record_id)
        if source is None:
            continue
        extraction_jobs.append((source, frag))
        matched_source_ids.add(source.id)
    for source in sources:
        if source.id not in matched_source_ids:
            extraction_jobs.append((source, None))
    stage_traces.append(
        _build_stage_trace(
            stage_name="source intake",
            run_id=run_id,
            timestamp=intake_timestamp,
            inputs={
                "source_count": len(sources),
                "source_ids": [str(source.id) for source in sources],
                "cache_paths": {
                    "source_cache_dir": str(source_cache_dir_path),
                    "derived_cache_dir": str(derived_cache_dir_path),
                },
                "cache_path_resolution": {
                    "source_cache_dir": source_cache_resolution,
                    "derived_cache_dir": derived_cache_resolution,
                },
            },
            outputs={
                "source_record_ids": [source.id for source in sources],
                "source_snapshot_ids": [snapshot.id for snapshot in source_snapshots],
                "source_fragment_ids": [fragment.id for fragment in source_fragments],
                "source_parse_trace_ids": [trace.id for trace in source_parse_traces],
                "source_fetch_metadata_ids": [item.id for item in source_fetch_metadata],
                "source_fetch_attempt_ids": [item.id for item in source_fetch_attempts],
                "source_inventory_id": source_inventory_artifact.id,
                "source_target_link_ids": [item.id for item in source_target_links],
                "source_categorisation_rationale_count": len(source_categorisation_rationales),
                "review_decision_ids": [review.id for review in review_decisions],
                "review_transitions": [
                    {
                        "target_type": review.target_type,
                        "target_id": review.target_id,
                        "previous_status": review.previous_status.value
                        if review.previous_status
                        else None,
                        "new_status": review.new_status.value,
                        "reviewer": review.reviewer,
                    }
                    for review in review_decisions
                ],
                "fetch_cache_traces": intake_result_traces,
            },
            strategy_used="authority_adapter_fetch_with_snapshot_cache",
            model_alias_used=None,
            started_at=intake_started_at,
            output_artifact_ids=[
                f"artifact-{run_id}-source-records",
                f"artifact-{run_id}-source-snapshots",
                f"artifact-{run_id}-source-fragments",
                f"artifact-{run_id}-source-parse-traces",
                f"artifact-{run_id}-source-fetch-metadata",
                f"artifact-{run_id}-source-fetch-attempts",
                source_inventory_artifact.id,
                f"artifact-{run_id}-source-target-links",
                f"artifact-{run_id}-source-categorisation-rationales",
            ],
            metrics={
                "source_count": len(sources),
                "source_snapshot_count": len(source_snapshots),
                "source_fragment_count": len(source_fragments),
                "source_target_link_count": len(source_target_links),
                "source_categorisation_rationale_count": len(source_categorisation_rationales),
                **fetch_attempt_metrics,
                **parse_trace_metrics,
            },
        )
    )

    ext_mode, ext_exec_mode, ext_fallback, div_reasoning = _resolve_extraction_and_divergence(
        use_llm=use_llm,
        extraction_mode=extraction_mode,
        extraction_execution_mode=extraction_execution_mode,
        extraction_fallback=extraction_fallback,
        divergence_reasoning=divergence_reasoning,
        case_data=case_data,
    )
    model_error_policy = _resolve_model_error_policy(case_data)
    focus_scopes, extraction_prop_limit = _resolve_case_extraction_limits(case_data)
    extraction_selection_cfg = _resolve_extraction_selection_config(case_data)
    fragment_selection_mode = str(extraction_selection_cfg.get("fragment_selection_mode") or "all_matching")
    needs_llm_client = ext_mode in ("local", "frontier") or div_reasoning == "frontier"
    llm_client = JuditLLMClient() if needs_llm_client else None

    extraction_started_at = perf_counter()
    extraction_timestamp = _utc_now_iso()
    extraction_warnings: list[str] = []
    accumulated_raw_props: list[Proposition] = []
    proposition_extraction_failures: list[dict[str, Any]] = []
    extraction_llm_diagnostic_traces: list[dict[str, Any]] = []
    proposition_extraction_jobs: list[dict[str, Any]] = []
    extraction_prompt = prompt_cfg.get("extraction", {})
    prompt_ver_extract = str(extraction_prompt.get("version", EXTRACTION_PROMPT_VERSION_V2))
    extraction_parameters = {
        "limit": extraction_prop_limit,
        "focus_scopes": list(focus_scopes),
        "focus_terms": list(extraction_selection_cfg.get("focus_terms", [])),
        "required_fragment_locators": sorted(extraction_selection_cfg.get("required_locators", set())),
        "include_annexes": bool(extraction_selection_cfg.get("include_annexes")),
        "fragment_selection_mode": fragment_selection_mode,
        "use_llm": use_llm,
        "extraction_mode": ext_mode,
        "extraction_execution_mode": ext_exec_mode,
        "extraction_fallback": ext_fallback,
        "divergence_reasoning": div_reasoning,
        "schema_version": EXTRACTION_SCHEMA_VERSION_V2,
        "max_extract_input_tokens": getattr(llm_client.settings, "max_extract_input_tokens", 150_000)
        if llm_client
        else None,
        "extract_model_context_limit": getattr(llm_client.settings, "extract_model_context_limit", 200_000)
        if llm_client
        else None,
        "model_error_policy": model_error_policy,
        "repair_resume": extraction_repair_targets is not None,
        "retry_failed_llm_chunks": retry_failed_llm,
    }
    required_locators_cfg: set[str] = set(extraction_selection_cfg.get("required_locators", set()))
    available_fragment_locators = {
        _normalize_locator_for_match(fragment.locator) for fragment in source_fragments if fragment.locator
    }
    required_locator_misses: set[str] = {
        locator for locator in required_locators_cfg if locator and locator not in available_fragment_locators
    }
    available_fragment_locators_sample = sorted(x for x in available_fragment_locators if x)[:20]
    if required_locator_misses:
        extraction_warnings.append(
            "Missing required fragment locators in source fragments: "
            + ", ".join(sorted(required_locator_misses))
        )
    extraction_strategy_version = str(strategy_versions.get("proposition_extraction", "v1"))
    extraction_cache_model_alias = _extraction_cache_model_alias(ext_mode, llm_client)
    configured_context_limit = (
        int(getattr(llm_client.settings, "extract_model_context_limit", 200_000))
        if llm_client is not None
        else None
    )
    extraction_cache_key = build_derived_artifact_cache_key(
        stage_name="proposition_extraction",
        source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
        source_fragment_ids=[fragment.id for fragment in source_fragments],
        model_alias=extraction_cache_model_alias,
        prompt_name=str(extraction_prompt.get("name", "extract.propositions.default")),
        prompt_version=prompt_ver_extract,
        pipeline_version=pipeline_version,
        strategy_version=extraction_strategy_version,
        parameters=extraction_parameters,
    )
    skip_extraction = bool(case_data.get("skip_proposition_extraction")) and replay_from_export
    if skip_extraction and extraction_repair_targets is not None:
        raise ValueError(
            "skip_proposition_extraction cannot be combined with extraction repair targets."
        )
    if skip_extraction:
        assert intake_bundle is not None
        raw_props = intake_bundle.get("propositions") or []
        accumulated_raw_props = [
            Proposition.model_validate(x) for x in raw_props if isinstance(x, dict)
        ]
        extraction_hook = {
            "stage_name": "proposition_extraction",
            "cache_status": "skipped_preloaded_propositions",
            "cache_dir": str(derived_cache_dir_path),
            "parameters": extraction_parameters,
        }
        pr.stage("Proposition extraction", detail="skipped — replayed dataset propositions")
    else:
        cached_extraction = derived_cache.get(
            stage_name="proposition_extraction",
            cache_key=extraction_cache_key,
        )
        allow_aggregate_hit = cached_extraction is not None
        if allow_aggregate_hit and extraction_repair_targets is not None:
            allow_aggregate_hit = False
        if allow_aggregate_hit and retry_failed_llm:
            allow_aggregate_hit = False
        if allow_aggregate_hit and ext_mode == "frontier" and cached_extraction is not None:
            agg_flag = cached_extraction.payload.get("frontier_aggregate_eligible")
            if agg_flag is not True:
                allow_aggregate_hit = False
        if allow_aggregate_hit:
            pr.stage("Proposition extraction", detail="cache hit — reusing derived artifacts")
            accumulated_raw_props = [
                Proposition.model_validate(item)
                for item in cached_extraction.payload.get("propositions", [])
            ]
            raw_failures = cached_extraction.payload.get("proposition_extraction_failures")
            if isinstance(raw_failures, list):
                proposition_extraction_failures = [item for item in raw_failures if isinstance(item, dict)]
            extraction_hook = build_derived_artifact_cache_hook(
                stage_name="proposition_extraction",
                source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
                source_fragment_ids=[fragment.id for fragment in source_fragments],
                model_alias=extraction_cache_model_alias,
                prompt_name=str(extraction_prompt.get("name", "extract.propositions.default")),
                prompt_version=prompt_ver_extract,
                pipeline_version=pipeline_version,
                strategy_version=extraction_strategy_version,
                parameters=extraction_parameters,
                cache_status="cache_hit",
                cache_dir=str(derived_cache_dir_path),
                cache_storage_uri=cached_extraction.storage_uri,
                cached_at=cached_extraction.cached_at.isoformat().replace("+00:00", "Z"),
            )
            proposition_counts_by_job: dict[tuple[str, str | None], int] = defaultdict(int)
            for item in accumulated_raw_props:
                proposition_counts_by_job[(str(item.source_record_id), str(item.source_fragment_id or "") or None)] += 1
            for jidx, (source, frag) in enumerate(extraction_jobs, start=1):
                (
                    selected_for_extraction,
                    selection_reason,
                    skip_reason,
                    fragment_locator,
                    fragment_type,
                    fragment_text_length,
                ) = _select_fragment_for_extraction(
                    ext_mode=ext_mode,
                    source=source,
                    fragment=frag,
                    required_locators=set(extraction_selection_cfg["required_locators"]),
                    include_annexes=bool(extraction_selection_cfg["include_annexes"]),
                    focus_terms=list(extraction_selection_cfg["focus_terms"]),
                    min_fragment_chars=int(extraction_selection_cfg["min_fragment_chars"]),
                    fragment_selection_mode=fragment_selection_mode,
                )
                job_key = (str(source.id), str(frag.id) if frag is not None else None)
                proposition_extraction_jobs.append(
                    {
                        "id": f"pexjob-{run_id}-{jidx:04d}",
                        "run_id": run_id,
                        "source_record_id": source.id,
                        "source_title": source.title,
                        "source_fragment_id": frag.id if frag is not None else None,
                        "fragment_locator": fragment_locator or None,
                        "fragment_type": fragment_type,
                        "fragment_text_length": fragment_text_length,
                        "selected_for_extraction": selected_for_extraction,
                        "selection_reason": selection_reason,
                        "skip_reason": skip_reason,
                        "extraction_mode": ext_mode,
                        "model_alias": extraction_cache_model_alias,
                        "estimated_input_tokens": None,
                        "configured_context_limit": configured_context_limit,
                        "llm_invoked": bool(selected_for_extraction and ext_mode in {"frontier", "local"}),
                        "fallback_used": False,
                        "fallback_strategy": None,
                        "context_window_risk": False,
                        "proposition_count": proposition_counts_by_job.get(job_key, 0),
                        "cache_status": "aggregate_cache_hit" if selected_for_extraction else "not_attempted",
                        "started_at": None,
                        "finished_at": None,
                        "duration_ms": 0,
                        "errors": [],
                        "warnings": [],
                        "repairable": False,
                        "repair_reason": None,
                        "estimated_retry_tokens": None,
                        "raw_model_output_excerpt": None,
                        "raw_model_output_truncated": None,
                        "parse_error_message": None,
                        "parse_error_line": None,
                        "parse_error_column": None,
                    }
                )
        else:
            n_jobs = len(extraction_jobs)
            pr.stage("Proposition extraction", detail=f"{n_jobs} extraction job(s)")
            content_hash_first_hits: dict[str, tuple[str, str | None, list[Proposition]]] = {}
            halt_remaining_due_to_policy = False
            halt_policy_reason: str | None = None
            for jidx, (source, frag) in enumerate(extraction_jobs, start=1):
                label = _source_progress_label(source)
                if frag is not None:
                    label = f"{label} · {frag.locator}"
                pr.extraction_source(jidx, n_jobs, ext_mode, label)
                pr.verbose(f"Extract id={source.id} title={source.title!r} mode={ext_mode}")
                job_key = (str(source.id), str(frag.id) if frag is not None else None)
                if extraction_repair_targets is not None and job_key not in extraction_repair_targets:
                    continue
                (
                    selected_for_extraction,
                    selection_reason,
                    skip_reason,
                    fragment_locator,
                    fragment_type,
                    fragment_text_length,
                ) = _select_fragment_for_extraction(
                    ext_mode=ext_mode,
                    source=source,
                    fragment=frag,
                    required_locators=set(extraction_selection_cfg["required_locators"]),
                    include_annexes=bool(extraction_selection_cfg["include_annexes"]),
                    focus_terms=list(extraction_selection_cfg["focus_terms"]),
                    min_fragment_chars=int(extraction_selection_cfg["min_fragment_chars"]),
                    fragment_selection_mode=fragment_selection_mode,
                )

                job_row: dict[str, Any] = {
                    "id": f"pexjob-{run_id}-{jidx:04d}",
                    "run_id": run_id,
                    "source_record_id": source.id,
                    "source_title": source.title,
                    "source_fragment_id": frag.id if frag is not None else None,
                    "fragment_locator": fragment_locator or None,
                    "fragment_type": fragment_type,
                    "fragment_text_length": fragment_text_length,
                    "selected_for_extraction": selected_for_extraction,
                    "selection_reason": selection_reason,
                    "skip_reason": skip_reason,
                    "extraction_mode": ext_mode,
                    "model_alias": extraction_cache_model_alias,
                    "estimated_input_tokens": None,
                    "configured_context_limit": configured_context_limit,
                    "llm_invoked": False,
                    "fallback_used": False,
                    "fallback_strategy": None,
                    "context_window_risk": False,
                    "proposition_count": 0,
                    "cache_status": "not_attempted",
                    "started_at": None,
                    "finished_at": None,
                    "duration_ms": 0,
                    "errors": [],
                    "warnings": [],
                    "repairable": False,
                    "repair_reason": None,
                    "estimated_retry_tokens": None,
                    "raw_model_output_excerpt": None,
                    "raw_model_output_truncated": None,
                    "parse_error_message": None,
                    "parse_error_line": None,
                    "parse_error_column": None,
                }
                if halt_remaining_due_to_policy:
                    job_row["selected_for_extraction"] = False
                    job_row["selection_reason"] = "skipped_model_error_policy_stop_repairable"
                    job_row["skip_reason"] = "model_error_policy_stop_repairable"
                    if halt_policy_reason:
                        job_row["warnings"] = [f"upstream_repairable_halt: {halt_policy_reason}"]
                    proposition_extraction_jobs.append(job_row)
                    continue
                if not selected_for_extraction:
                    proposition_extraction_jobs.append(job_row)
                    continue
                job_started_at = perf_counter()
                job_row["started_at"] = _utc_now_iso()
                work_source = source
                if frag is not None:
                    meta_base = dict(source.metadata) if isinstance(source.metadata, dict) else {}
                    meta_base["extraction_fragment_id"] = frag.id
                    work_source = source.model_copy(
                        deep=True,
                        update={
                            "authoritative_text": frag.fragment_text,
                            "authoritative_locator": frag.locator,
                            "metadata": meta_base,
                        },
                    )
                snap_hash = _snapshot_content_hash_for_extraction_job(
                    source=source, fragment=frag, source_snapshots=source_snapshots
                )
                cached_hit = content_hash_first_hits.get(snap_hash) if snap_hash else None
                if cached_hit is not None and extraction_repair_targets is None:
                    cached_source_id, cached_fragment_id, cached_rows = cached_hit
                    current_fragment_id = str(frag.id) if frag is not None else None
                    same_fragment_identity = (
                        cached_source_id == str(source.id)
                        and cached_fragment_id == current_fragment_id
                    )
                    if same_fragment_identity:
                        cached_hit = None
                if cached_hit is not None and extraction_repair_targets is None:
                    cloned = _clone_propositions_for_shared_content_hash(
                        cached_hit[2],
                        source=source,
                        fragment=frag,
                        shared_content_hash=snap_hash,
                    )
                    accumulated_raw_props.extend(cloned)
                    pr.verbose(
                        f"Reused extraction for identical content_hash={snap_hash[:16]}… "
                        f"source={source.id}"
                    )
                    job_row["proposition_count"] = len(cloned)
                    job_row["cache_status"] = "content_hash_reuse"
                    job_row["warnings"] = ["reused_prior_fragment_content_hash"]
                    job_row["finished_at"] = _utc_now_iso()
                    job_row["duration_ms"] = int(max(0.0, (perf_counter() - job_started_at) * 1000))
                    proposition_extraction_jobs.append(job_row)
                    continue
                if ext_mode in ("frontier", "local"):
                    call_kind: Literal["frontier", "local"] = (
                        "frontier" if ext_mode == "frontier" else "local"
                    )
    
                    def _on_before_llm(
                        trace: dict[str, Any],
                        *,
                        kind: Literal["frontier", "local"] = call_kind,
                        i: int = jidx,
                        total: int = n_jobs,
                        lab: str = label,
                    ) -> None:
                        pr.before_model_extract(
                            kind,
                            i,
                            total,
                            lab,
                            source_record_id=str(trace.get("source_record_id") or ""),
                            estimated_input_tokens=trace.get("estimated_input_tokens")
                            if isinstance(trace.get("estimated_input_tokens"), int)
                            else None,
                            extraction_llm_chunk_index=trace.get("extraction_llm_chunk_index")
                            if isinstance(trace.get("extraction_llm_chunk_index"), int)
                            else None,
                            extraction_llm_chunk_total=trace.get("extraction_llm_chunk_total")
                            if isinstance(trace.get("extraction_llm_chunk_total"), int)
                            else None,
                            trace=trace,
                        )
    
                    on_before = _on_before_llm
                else:
                    on_before = None
                extract_llm = llm_client if ext_mode in ("local", "frontier") else None
                outcome = extract_propositions_from_source(
                    source=work_source,
                    topic=topic,
                    cluster=cluster,
                    llm_client=extract_llm,
                    limit=extraction_prop_limit,
                    extraction_mode=ext_mode,
                    extraction_fallback=ext_fallback,
                    prompt_version=prompt_ver_extract,
                    on_before_llm_call=on_before,
                    focus_scopes=focus_scopes or None,
                    model_error_policy=model_error_policy,
                    derived_chunk_cache=derived_cache if ext_mode in ("frontier", "local") else None,
                    retry_failed_llm=retry_failed_llm,
                    chunk_cache_pipeline_version=pipeline_version,
                    chunk_cache_strategy_version=extraction_strategy_version,
                )
                extraction_llm_diagnostic_traces.extend(outcome.extraction_llm_call_traces)
                pr.extraction_source_complete(outcome)
                extracted = outcome.propositions
                trace_rows = [row for row in outcome.extraction_llm_call_traces if isinstance(row, dict)]
                ests = [
                    int(row["estimated_input_tokens"])
                    for row in trace_rows
                    if isinstance(row.get("estimated_input_tokens"), int)
                ]
                if ests:
                    job_row["estimated_input_tokens"] = max(ests)
                    job_row["estimated_retry_tokens"] = max(ests)
                job_row["llm_invoked"] = any(bool(row.get("llm_invoked")) for row in trace_rows)
                if any(str(row.get("skip_reason") or "") == "context_window_risk" for row in trace_rows):
                    job_row["context_window_risk"] = True
                    if not job_row["llm_invoked"]:
                        job_row["selection_reason"] = "skipped_context_window_risk"
                        job_row["skip_reason"] = "context_window_risk"
                cache_hints = [
                    row.get("llm_cache_hit") for row in trace_rows if row.get("llm_cache_hit") is not None
                ]
                if "failed_chunk_cached" in cache_hints:
                    job_row["cache_status"] = "failed_chunk_cached"
                elif any(h is True for h in cache_hints):
                    job_row["cache_status"] = "chunk_cache_hit"
                else:
                    job_row["cache_status"] = "chunk_cache_miss"
                if outcome.failed_closed:
                    proposition_extraction_failures.append(
                        {
                            "source_record_id": source.id,
                            "source_fragment_id": frag.id if frag else None,
                            "extraction_mode": outcome.extraction_mode,
                            "fallback_policy": outcome.fallback_policy,
                            "validation_errors": outcome.validation_errors,
                            "validation_issue_records": outcome.validation_issue_records,
                            "failure_reason": outcome.failure_reason,
                            "model_alias": outcome.model_alias,
                            "extraction_llm_call_traces": outcome.extraction_llm_call_traces,
                        }
                    )
                    extraction_warnings.append(
                        f"Extraction fail_closed for source {source.id}: {outcome.failure_reason}"
                    )
                    job_row["errors"] = [
                        str(x) for x in (outcome.validation_errors or []) if str(x).strip()
                    ]
                if outcome.fallback_used:
                    fb_reason: str | None = outcome.failure_reason
                    if not fb_reason and outcome.validation_errors:
                        fb_reason = "; ".join(str(x) for x in outcome.validation_errors[:3])
                    if not fb_reason:
                        fb_reason = "model path unavailable or produced no valid rows"
                    pr.fallback_notice(label, fb_reason)
                job_row["fallback_used"] = bool(outcome.fallback_used)
                job_row["fallback_strategy"] = (
                    str(outcome.fallback_strategy) if getattr(outcome, "fallback_strategy", None) else None
                )
                if frag is not None:
                    for item in extracted:
                        item.source_fragment_id = frag.id
                        if not item.fragment_locator or item.fragment_locator == "document:full":
                            item.fragment_locator = frag.locator
                        if not item.source_snapshot_id:
                            item.source_snapshot_id = frag.source_snapshot_id
                elif source.current_snapshot_id:
                    for item in extracted:
                        if not item.source_snapshot_id:
                            item.source_snapshot_id = source.current_snapshot_id
                if not extracted and not outcome.failed_closed:
                    extraction_warnings.append(f"No propositions extracted for source {source.id}.")
                    if not job_row["warnings"]:
                        job_row["warnings"] = ["selected_fragment_returned_no_propositions"]
                accumulated_raw_props.extend(extracted)
                job_row["proposition_count"] = len(extracted)
                if isinstance(outcome.validation_errors, list) and outcome.validation_errors:
                    job_row["warnings"] = [str(x) for x in outcome.validation_errors if str(x).strip()]
                repair_reason: str | None = None
                repair_type: str | None = None
                for message in job_row["errors"] + job_row["warnings"]:
                    repair_type = classify_repairable_failure_type(str(message))
                    if repair_type:
                        repair_reason = repair_type
                        break
                if repair_type:
                    job_row["repairable"] = True
                    job_row["repair_reason"] = repair_reason
                    if repair_reason == "json_parse_or_llm_failure":
                        parse_trace = next(
                            (
                                row
                                for row in trace_rows
                                if isinstance(row, dict)
                                and (
                                    row.get("raw_model_output_excerpt") is not None
                                    or row.get("parse_error_message") is not None
                                )
                            ),
                            None,
                        )
                        if isinstance(parse_trace, dict):
                            raw_excerpt = parse_trace.get("raw_model_output_excerpt")
                            if isinstance(raw_excerpt, str):
                                job_row["raw_model_output_excerpt"] = raw_excerpt[:4000]
                                job_row["raw_model_output_truncated"] = bool(
                                    parse_trace.get("raw_model_output_truncated")
                                ) or (len(raw_excerpt) > 4000)
                            elif parse_trace.get("raw_model_output_truncated") is not None:
                                job_row["raw_model_output_truncated"] = bool(
                                    parse_trace.get("raw_model_output_truncated")
                                )
                            pem = parse_trace.get("parse_error_message")
                            if isinstance(pem, str) and pem.strip():
                                job_row["parse_error_message"] = pem
                            pl = parse_trace.get("parse_error_line")
                            if isinstance(pl, int) and pl > 0:
                                job_row["parse_error_line"] = pl
                            pc = parse_trace.get("parse_error_column")
                            if isinstance(pc, int) and pc > 0:
                                job_row["parse_error_column"] = pc
                    if job_row["estimated_retry_tokens"] is None:
                        job_row["estimated_retry_tokens"] = job_row.get("estimated_input_tokens")
                if (
                    snap_hash
                    and snap_hash not in content_hash_first_hits
                    and extracted
                    and extraction_repair_targets is None
                ):
                    content_hash_first_hits[snap_hash] = (
                        str(source.id),
                        str(frag.id) if frag is not None else None,
                        [x.model_copy(deep=True) for x in extracted],
                    )
                if outcome.repairable_extraction_halt:
                    halt_msg = outcome.repairable_extraction_halt_reason or "model provider halted extraction"
                    extraction_warnings.append(f"Repairable extraction halt ({label}): {halt_msg}")
                    pr.verbose(f"repairable halt: {halt_msg}")
                    job_row["repairable"] = True
                    job_row["repair_reason"] = classify_repairable_failure_type(halt_msg) or halt_msg
                    if not job_row["errors"]:
                        job_row["errors"] = [halt_msg]
                job_row["finished_at"] = _utc_now_iso()
                job_row["duration_ms"] = int(max(0.0, (perf_counter() - job_started_at) * 1000))
                proposition_extraction_jobs.append(job_row)
                if outcome.repairable_extraction_halt:
                    if model_error_policy == "stop_repairable":
                        halt_remaining_due_to_policy = True
                        halt_policy_reason = str(job_row.get("repair_reason") or halt_msg)
                    else:
                        extraction_warnings.append(
                            f"Repairable extraction event ignored by model_error_policy={model_error_policy}"
                        )
            persisted_extraction = derived_cache.put(
                stage_name="proposition_extraction",
                cache_key=extraction_cache_key,
                payload={
                    "propositions": [item.model_dump(mode="json") for item in accumulated_raw_props],
                    "proposition_extraction_failures": proposition_extraction_failures,
                    "frontier_aggregate_eligible": ext_mode != "frontier"
                    or not _frontier_aggregate_cache_blocked(
                        extraction_failures=proposition_extraction_failures,
                        propositions=accumulated_raw_props,
                    ),
                },
            )
            extraction_hook = build_derived_artifact_cache_hook(
                stage_name="proposition_extraction",
                source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
                source_fragment_ids=[fragment.id for fragment in source_fragments],
                model_alias=extraction_cache_model_alias,
                prompt_name=str(extraction_prompt.get("name", "extract.propositions.default")),
                prompt_version=prompt_ver_extract,
                pipeline_version=pipeline_version,
                strategy_version=extraction_strategy_version,
                parameters=extraction_parameters,
                cache_status="cache_miss_persisted",
                cache_dir=str(derived_cache_dir_path),
                cache_storage_uri=persisted_extraction.storage_uri,
                cached_at=persisted_extraction.cached_at.isoformat().replace("+00:00", "Z"),
            )
    if extraction_repair_kept_propositions is None:
        propositions = _build_proposition_records(
            propositions=accumulated_raw_props,
            run_id=run_id,
            source_by_id=source_by_id,
            source_fragment_by_id=source_fragment_by_id,
        )
    else:
        rebuilt = _build_proposition_records(
            propositions=accumulated_raw_props,
            run_id=run_id,
            source_by_id=source_by_id,
            source_fragment_by_id=source_fragment_by_id,
        )
        propositions = list(extraction_repair_kept_propositions) + rebuilt
    proposition_extraction_traces = _build_proposition_extraction_traces(
        propositions=propositions,
        use_llm=use_llm,
        extraction_prompt=extraction_prompt,
        extraction_strategy_version=extraction_strategy_version,
        extraction_hook=extraction_hook,
        pipeline_version=pipeline_version,
    )
    proposition_extraction_metrics = _proposition_extraction_trace_metrics(proposition_extraction_traces)
    selected_by_required_locator = sum(
        1 for row in proposition_extraction_jobs if str(row.get("selection_reason") or "") == "required_locator"
    )
    selected_by_focus_term = sum(
        1 for row in proposition_extraction_jobs if str(row.get("selection_reason") or "") == "focus_term_match"
    )
    selected_by_annex_included = sum(
        1 for row in proposition_extraction_jobs if str(row.get("selection_reason") or "") == "annex_included"
    )
    skipped_not_required_count = sum(
        1
        for row in proposition_extraction_jobs
        if str(row.get("skip_reason") or "") == "skipped_not_required_in_required_only_mode"
    )
    stage_traces.append(
        _build_stage_trace(
            stage_name="proposition extraction",
            run_id=run_id,
            timestamp=extraction_timestamp,
            inputs={
                "source_record_ids": [source.id for source in sources],
                "source_count": len(sources),
                "use_llm": use_llm,
                "extraction_mode": ext_mode,
                "extraction_execution_mode": ext_exec_mode,
                "extraction_fallback": ext_fallback,
                "divergence_reasoning": div_reasoning,
                "focus_scopes": list(focus_scopes),
                "max_propositions_per_source": extraction_prop_limit,
                "effective_required_fragment_locators": sorted(required_locators_cfg),
                "effective_focus_terms": list(extraction_selection_cfg.get("focus_terms", [])),
                "effective_include_annexes": bool(extraction_selection_cfg.get("include_annexes")),
                "effective_fragment_selection_mode": fragment_selection_mode,
                "effective_focus_scopes": list(focus_scopes),
                "effective_model_error_policy": model_error_policy,
                "effective_max_propositions_per_source": extraction_prop_limit,
                "available_fragment_locators_count": len(available_fragment_locators),
                "available_fragment_locators_sample": available_fragment_locators_sample,
                "max_extract_input_tokens": getattr(llm_client.settings, "max_extract_input_tokens", 150_000)
                if llm_client
                else None,
                "extract_model_context_limit": getattr(llm_client.settings, "extract_model_context_limit", 200_000)
                if llm_client
                else None,
                "extraction_llm_call_traces": extraction_llm_diagnostic_traces,
                "derived_artifact_cache": extraction_hook,
            },
            outputs={
                "proposition_ids": [item.id for item in propositions],
                "proposition_count": len(propositions),
                "proposition_extraction_trace_ids": [item.id for item in proposition_extraction_traces],
                "proposition_extraction_failure_count": len(proposition_extraction_failures),
                "source_fragment_links": {
                    item.id: item.source_fragment_id
                    for item in propositions
                    if item.source_fragment_id
                },
                "selected_by_required_locator": selected_by_required_locator,
                "selected_by_focus_term": selected_by_focus_term,
                "selected_by_annex_included": selected_by_annex_included,
                "skipped_not_required_count": skipped_not_required_count,
            },
            strategy_used=(
                "skipped_preloaded_propositions"
                if skip_extraction
                else f"{ext_mode}_extraction|fallback={ext_fallback}"
            ),
            model_alias_used=extraction_cache_model_alias,
            started_at=extraction_started_at,
            warnings=extraction_warnings,
            metrics=proposition_extraction_metrics,
        )
    )
    pr.stage("Proposition inventory", detail=f"{len(propositions)} proposition(s)")
    proposition_inventory = _build_proposition_inventory(propositions)
    proposition_review_decisions = _build_proposition_review_decisions(propositions)
    review_decisions.extend(proposition_review_decisions)
    stage_traces.append(
        _build_stage_trace(
            stage_name="proposition inventory",
            run_id=run_id,
            timestamp=_utc_now_iso(),
            inputs={
                "proposition_ids": [item.id for item in propositions],
            },
            outputs={
                "proposition_ids": [item.id for item in propositions],
                "category_count": len(proposition_inventory["categories"]),
                "tag_count": len(proposition_inventory["tags"]),
                "review_decision_ids": [item.id for item in proposition_review_decisions],
            },
            strategy_used="deterministic_proposition_inventory_enrichment",
            model_alias_used=None,
            started_at=perf_counter(),
            warnings=[
                "Cross references are deterministic keys, not semantic legal citation parsing yet."
            ],
        )
    )

    analysis_mode_raw = str(case_data.get("analysis_mode", "auto")).strip().lower()
    if analysis_mode_raw in {"single_jurisdiction", "single-jurisdiction", "single"}:
        divergence_enabled = False
    elif analysis_mode_raw in {"divergence", "compare"}:
        divergence_enabled = True
    else:
        divergence_enabled = False

    if divergence_enabled:
        pr.stage("Divergence comparison", detail="pairing and classification")
    else:
        pr.stage("Divergence comparison", detail="skipped — single jurisdiction")

    proposition_pairs: list[tuple[Proposition, Proposition]] = []
    divergence_assessments: list[DivergenceAssessment] = []
    divergence_findings: list[DivergenceFinding] = []
    divergence_observations: list[DivergenceObservation] = []
    assessment_review_decisions: list[ReviewDecision] = []

    pairing_started_at = perf_counter()
    pairing_timestamp = _utc_now_iso()
    if divergence_enabled:
        proposition_pairs = _build_comparison_pairs(
            propositions=propositions,
            sources=sources,
            comparison_cfg=comparison_cfg,
        )
        stage_traces.append(
            _build_stage_trace(
                stage_name="proposition pairing",
                run_id=run_id,
                timestamp=pairing_timestamp,
                inputs={
                    "comparison_config": comparison_cfg,
                    "available_proposition_ids": [item.id for item in propositions],
                },
                outputs={
                    "pair_count": len(proposition_pairs),
                    "pairs": [
                        {
                            "proposition_id": proposition_a.id,
                            "comparator_proposition_id": proposition_b.id,
                        }
                        for proposition_a, proposition_b in proposition_pairs
                    ],
                },
                strategy_used="configured_index_pairing",
                model_alias_used=None,
                started_at=pairing_started_at,
            )
        )
    else:
        stage_traces.append(
            _build_stage_trace(
                stage_name="proposition pairing",
                run_id=run_id,
                timestamp=pairing_timestamp,
                inputs={"comparison_config": comparison_cfg},
                outputs={"pair_count": 0, "pairs": []},
                strategy_used="pairing_skipped_single_jurisdiction",
                model_alias_used=None,
                started_at=pairing_started_at,
                warnings=["Divergence pairing skipped: single-jurisdiction analysis mode enabled."],
            )
        )

    classification_started_at = perf_counter()
    classification_timestamp = _utc_now_iso()
    if divergence_enabled:
        divergence_reason_alias = (
            llm_client.settings.frontier_reason_model
            if llm_client and div_reasoning == "frontier"
            else None
        )
        classification_prompt = prompt_cfg.get("classification", {})
        classification_parameters = {
            "pair_ids": [
                f"{proposition_a.id}:{proposition_b.id}"
                for proposition_a, proposition_b in proposition_pairs
            ],
            "pair_count": len(proposition_pairs),
            "use_llm": use_llm,
            "divergence_reasoning": div_reasoning,
        }
        classification_strategy_version = str(
            strategy_versions.get("divergence_classification", "v1")
        )
        classification_cache_key = build_derived_artifact_cache_key(
            stage_name="divergence_classification",
            source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
            source_fragment_ids=[fragment.id for fragment in source_fragments],
            model_alias=divergence_reason_alias,
            prompt_name=str(classification_prompt.get("name", "classify.divergence.default")),
            prompt_version=str(classification_prompt.get("version", "v1")),
            pipeline_version=pipeline_version,
            strategy_version=classification_strategy_version,
            parameters=classification_parameters,
        )
        cached_classification = derived_cache.get(
            stage_name="divergence_classification",
            cache_key=classification_cache_key,
        )
        if cached_classification is not None:
            cached_observations = cached_classification.payload.get("observations", [])
            if not isinstance(cached_observations, list) or not cached_observations:
                cached_observations = cached_classification.payload.get("assessments", [])
            divergence_assessments = [
                DivergenceAssessment.model_validate(item) for item in cached_observations
            ]
            divergence_assessments = [
                assessment.model_copy(
                    update={
                        "review_status": _canonicalize_legacy_review_status(
                            assessment.review_status
                        )
                    }
                )
                for assessment in divergence_assessments
            ]
            classification_hook = build_derived_artifact_cache_hook(
                stage_name="divergence_classification",
                source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
                source_fragment_ids=[fragment.id for fragment in source_fragments],
                model_alias=divergence_reason_alias,
                prompt_name=str(classification_prompt.get("name", "classify.divergence.default")),
                prompt_version=str(classification_prompt.get("version", "v1")),
                pipeline_version=pipeline_version,
                strategy_version=classification_strategy_version,
                parameters=classification_parameters,
                cache_status="cache_hit",
                cache_dir=str(derived_cache_dir_path),
                cache_storage_uri=cached_classification.storage_uri,
                cached_at=cached_classification.cached_at.isoformat().replace("+00:00", "Z"),
            )
        else:
            divergence_assessments = [
                compare_propositions(
                    proposition_a=proposition_a,
                    proposition_b=proposition_b,
                    llm_client=llm_client,
                    divergence_reasoning=div_reasoning,
                )
                for proposition_a, proposition_b in proposition_pairs
            ]
            divergence_assessments = [
                assessment.model_copy(
                    update={
                        "review_status": _canonicalize_legacy_review_status(
                            assessment.review_status
                        )
                    }
                )
                for assessment in divergence_assessments
            ]
            persisted_classification = derived_cache.put(
                stage_name="divergence_classification",
                cache_key=classification_cache_key,
                payload={
                    "observations": [
                        item.model_dump(mode="json") for item in divergence_assessments
                    ],
                    # Legacy cache key for backward compatibility.
                    "assessments": [
                        item.model_dump(mode="json") for item in divergence_assessments
                    ],
                },
            )
            classification_hook = build_derived_artifact_cache_hook(
                stage_name="divergence_classification",
                source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
                source_fragment_ids=[fragment.id for fragment in source_fragments],
                model_alias=divergence_reason_alias,
                prompt_name=str(classification_prompt.get("name", "classify.divergence.default")),
                prompt_version=str(classification_prompt.get("version", "v1")),
                pipeline_version=pipeline_version,
                strategy_version=classification_strategy_version,
                parameters=classification_parameters,
                cache_status="cache_miss_persisted",
                cache_dir=str(derived_cache_dir_path),
                cache_storage_uri=persisted_classification.storage_uri,
                cached_at=persisted_classification.cached_at.isoformat().replace("+00:00", "Z"),
            )
        divergence_findings, divergence_observations, divergence_assessments = (
            _build_findings_and_observations(
                assessments=divergence_assessments,
                sources=sources,
                source_fragments=source_fragments,
                propositions=propositions,
            )
        )
        assessment_review_decisions = [
            apply_review_to_assessment(
                assessment=assessment,
                new_status=assessment.review_status,
                reviewer="system:compare",
                note=assessment.rationale,
                edited_fields=None,
            )[1]
            for assessment in divergence_assessments
        ]
        stage_traces.append(
            _build_stage_trace(
                stage_name="divergence classification",
                run_id=run_id,
                timestamp=classification_timestamp,
                inputs={
                    "pair_count": len(proposition_pairs),
                    "pair_ids": [
                        f"{proposition_a.id}:{proposition_b.id}"
                        for proposition_a, proposition_b in proposition_pairs
                    ],
                    "use_llm": use_llm,
                    "divergence_reasoning": div_reasoning,
                    "derived_artifact_cache": classification_hook,
                },
                outputs={
                    "assessment_ids": [assessment.id for assessment in divergence_assessments],
                    "divergence_types": [
                        assessment.divergence_type.value for assessment in divergence_assessments
                    ],
                    "review_decision_ids": [review.id for review in assessment_review_decisions],
                    "evidence_context": [
                        {
                            "observation_id": observation.id,
                            "primary_source_fragment_id": observation.primary_source_fragment_id,
                            "comparator_source_fragment_id": (
                                observation.comparator_source_fragment_id
                            ),
                            "supporting_source_fragment_ids": (
                                observation.supporting_source_fragment_ids
                            ),
                            "common_ancestor_fragment_id": observation.common_ancestor_fragment_id,
                            "context_note": observation.context_note,
                            "why_these_fragments": observation.why_these_fragments,
                            "limitations": observation.metadata.get(
                                "evidence_selection_limitations", []
                            ),
                        }
                        for observation in divergence_observations
                    ],
                    "review_transitions": [
                        {
                            "target_id": review.target_id,
                            "previous_status": review.previous_status.value
                            if review.previous_status
                            else None,
                            "new_status": review.new_status.value,
                            "reviewer": review.reviewer,
                        }
                        for review in assessment_review_decisions
                    ],
                },
                strategy_used="heuristic_classification_with_optional_llm_rationale",
                model_alias_used=divergence_reason_alias,
                started_at=classification_started_at,
            )
        )
    else:
        stage_traces.append(
            _build_stage_trace(
                stage_name="divergence classification",
                run_id=run_id,
                timestamp=classification_timestamp,
                inputs={
                    "pair_count": 0,
                    "pair_ids": [],
                    "use_llm": use_llm,
                    "derived_artifact_cache": {},
                },
                outputs={
                    "assessment_ids": [],
                    "divergence_types": [],
                    "review_decision_ids": [],
                    "evidence_context": [],
                    "review_transitions": [],
                },
                strategy_used="divergence_skipped_single_jurisdiction",
                model_alias_used=None,
                started_at=classification_started_at,
                warnings=[
                    "Divergence classification skipped: single-jurisdiction analysis mode enabled."
                ],
            )
        )

    run = ComparisonRun(
        id=run_id,
        topic_id=topic.id,
        cluster_id=cluster.id,
        model_profile="lite-llm" if needs_llm_client else "deterministic-file-input",
        workflow_mode="divergence" if divergence_enabled else "single_jurisdiction",
        source_record_ids=[source.id for source in sources],
        source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
        source_fragment_ids=[fragment.id for fragment in source_fragments],
        proposition_ids=[item.id for item in propositions],
        assessment_ids=[assessment.id for assessment in divergence_assessments],
        review_decision_ids=[review.id for review in review_decisions],
        notes=run_notes,
    )
    review_decisions.extend(
        [
            decision.model_copy(
                update={
                    "metadata": {
                        "jurisdiction_a": assessment.jurisdiction_a,
                        "jurisdiction_b": assessment.jurisdiction_b,
                    }
                }
            )
            for assessment, decision in zip(
                divergence_assessments, assessment_review_decisions, strict=False
            )
        ]
    )

    narrative_started_at = perf_counter()
    narrative_timestamp = _utc_now_iso()
    narrative_prompt = prompt_cfg.get("narrative", {})
    narrative_parameters = {
        "workflow_mode": run.workflow_mode,
        "proposition_ids": [item.id for item in propositions],
        "proposition_count": len(propositions),
        "assessment_ids": [assessment.id for assessment in divergence_assessments],
        "assessment_count": len(divergence_assessments),
        "title_override": narrative_cfg.get("title"),
        "summary_override": narrative_cfg.get("summary"),
    }
    narrative_strategy_version = str(strategy_versions.get("narrative_generation", "v1"))
    narrative_cache_key = build_derived_artifact_cache_key(
        stage_name="narrative_generation",
        source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
        source_fragment_ids=[fragment.id for fragment in source_fragments],
        model_alias=None,
        prompt_name=str(narrative_prompt.get("name", "narrative.template.default")),
        prompt_version=str(narrative_prompt.get("version", "v1")),
        pipeline_version=pipeline_version,
        strategy_version=narrative_strategy_version,
        parameters=narrative_parameters,
    )
    cached_narrative = derived_cache.get(
        stage_name="narrative_generation",
        cache_key=narrative_cache_key,
    )
    if cached_narrative is not None:
        narrative = NarrativeExport.model_validate(cached_narrative.payload.get("narrative", {}))
        narrative_hook = build_derived_artifact_cache_hook(
            stage_name="narrative_generation",
            source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
            source_fragment_ids=[fragment.id for fragment in source_fragments],
            model_alias=None,
            prompt_name=str(narrative_prompt.get("name", "narrative.template.default")),
            prompt_version=str(narrative_prompt.get("version", "v1")),
            pipeline_version=pipeline_version,
            strategy_version=narrative_strategy_version,
            parameters=narrative_parameters,
            cache_status="cache_hit",
            cache_dir=str(derived_cache_dir_path),
            cache_storage_uri=cached_narrative.storage_uri,
            cached_at=cached_narrative.cached_at.isoformat().replace("+00:00", "Z"),
        )
    else:
        narrative = NarrativeExport(
            title=narrative_cfg.get("title", f"{topic.name} narrative"),
            summary=narrative_cfg.get(
                "summary",
                (
                    "This case compares propositions across jurisdictions and records divergence."
                    if divergence_enabled
                    else (
                        "This case inventories source-backed legal propositions "
                        "for one jurisdiction."
                    )
                ),
            ),
            sections=[
                f"Topic: {topic.name}",
                f"Cluster: {cluster.name}",
                f"Propositions extracted: {len(propositions)}",
                "Proposition categories: "
                + ", ".join(sorted(proposition_inventory["categories"].keys())[:8]),
                (
                    f"Comparison candidates assessed: {len(divergence_assessments)}"
                    if divergence_enabled
                    else "Divergence analysis skipped in single-jurisdiction mode."
                ),
                (
                    "Divergence types: "
                    + ", ".join(
                        assessment.divergence_type.value for assessment in divergence_assessments
                    )
                    if divergence_assessments
                    else "No divergence observations produced."
                ),
            ],
        )
        persisted_narrative = derived_cache.put(
            stage_name="narrative_generation",
            cache_key=narrative_cache_key,
            payload={"narrative": narrative.model_dump(mode="json")},
        )
        narrative_hook = build_derived_artifact_cache_hook(
            stage_name="narrative_generation",
            source_snapshot_ids=[snapshot.id for snapshot in source_snapshots],
            source_fragment_ids=[fragment.id for fragment in source_fragments],
            model_alias=None,
            prompt_name=str(narrative_prompt.get("name", "narrative.template.default")),
            prompt_version=str(narrative_prompt.get("version", "v1")),
            pipeline_version=pipeline_version,
            strategy_version=narrative_strategy_version,
            parameters=narrative_parameters,
            cache_status="cache_miss_persisted",
            cache_dir=str(derived_cache_dir_path),
            cache_storage_uri=persisted_narrative.storage_uri,
            cached_at=persisted_narrative.cached_at.isoformat().replace("+00:00", "Z"),
        )
    stage_traces.append(
        _build_stage_trace(
            stage_name="narrative generation",
            run_id=run_id,
            timestamp=narrative_timestamp,
            inputs={
                "assessment_ids": [assessment.id for assessment in divergence_assessments],
                "assessment_count": len(divergence_assessments),
                "narrative_overrides": {
                    "title": "title" in narrative_cfg,
                    "summary": "summary" in narrative_cfg,
                },
                "derived_artifact_cache": narrative_hook,
            },
            outputs={
                "narrative_title": narrative.title,
                "section_count": len(narrative.sections),
            },
            strategy_used="template_narrative_synthesis",
            model_alias_used=None,
            started_at=narrative_started_at,
        )
    )

    pr.stage("Scope linking")
    scope_payload = build_scope_artifacts_for_run(
        run_id=run_id,
        propositions=propositions,
        sources=sources,
    )

    pr.stage("Completeness assessment")
    proposition_completeness_assessments = build_proposition_completeness_assessments(
        propositions=propositions,
        proposition_extraction_traces=proposition_extraction_traces,
        source_records=sources,
    )

    sf_raw = case_data.get("source_family_candidates")
    sf_validated_json: list[dict[str, Any]] = []
    if isinstance(sf_raw, list):
        for row in sf_raw:
            if isinstance(row, dict):
                sf_validated_json.append(
                    SourceFamilyCandidate.model_validate(row).model_dump(mode="json")
                )

    run_artifacts = [
        RunArtifact(
            id=f"artifact-{run.id}-source-records",
            run_id=run.id,
            artifact_type="source_records",
            provenance="pipeline.intake",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in sources],
                    sort_keys=True,
                )
            ),
            metadata={"source_record_count": len(sources)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-source-snapshots",
            run_id=run.id,
            artifact_type="source_snapshots",
            provenance="pipeline.intake",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in source_snapshots],
                    sort_keys=True,
                )
            ),
            metadata={"source_snapshot_count": len(source_snapshots)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-source-fragments",
            run_id=run.id,
            artifact_type="source_fragments",
            provenance="pipeline.intake",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in source_fragments],
                    sort_keys=True,
                )
            ),
            metadata={"source_fragment_count": len(source_fragments)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-source-parse-traces",
            run_id=run.id,
            artifact_type="source_parse_traces",
            provenance="pipeline.intake",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in source_parse_traces],
                    sort_keys=True,
                )
            ),
            metadata={"source_parse_trace_count": len(source_parse_traces)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-source-fetch-metadata",
            run_id=run.id,
            artifact_type="source_fetch_metadata",
            provenance="pipeline.intake",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in source_fetch_metadata],
                    sort_keys=True,
                )
            ),
            metadata={"source_fetch_metadata_count": len(source_fetch_metadata)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-source-fetch-attempts",
            run_id=run.id,
            artifact_type="source_fetch_attempts",
            provenance="pipeline.intake",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in source_fetch_attempts],
                    sort_keys=True,
                )
            ),
            metadata={"source_fetch_attempt_count": len(source_fetch_attempts)},
        ),
        RunArtifact(
            id=source_inventory_artifact.id,
            run_id=run.id,
            artifact_type="source_inventory",
            provenance="pipeline.intake",
            content_hash=source_inventory_artifact.content_hash,
            metadata={"source_inventory_row_count": len(source_inventory_artifact.rows)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-source-target-links",
            run_id=run.id,
            artifact_type="source_target_links",
            provenance="pipeline.intake",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in source_target_links],
                    sort_keys=True,
                )
            ),
            metadata={"source_target_link_count": len(source_target_links)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-source-categorisation-rationales",
            run_id=run.id,
            artifact_type="source_categorisation_rationales",
            provenance="pipeline.intake",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in source_categorisation_rationales],
                    sort_keys=True,
                )
            ),
            metadata={"source_categorisation_rationale_count": len(source_categorisation_rationales)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-propositions",
            run_id=run.id,
            artifact_type="propositions",
            provenance="pipeline.extract",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in propositions],
                    sort_keys=True,
                )
            ),
            metadata={"proposition_count": len(propositions)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-proposition-inventory",
            run_id=run.id,
            artifact_type="proposition_inventory",
            provenance="pipeline.extract",
            content_hash=content_hash(json.dumps(proposition_inventory, sort_keys=True)),
            metadata={"category_count": len(proposition_inventory["categories"])},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-proposition-extraction-traces",
            run_id=run.id,
            artifact_type="proposition_extraction_traces",
            provenance="pipeline.extract",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in proposition_extraction_traces],
                    sort_keys=True,
                )
            ),
            metadata={"proposition_extraction_trace_count": len(proposition_extraction_traces)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-proposition-extraction-jobs",
            run_id=run.id,
            artifact_type="proposition_extraction_jobs",
            provenance="pipeline.extract",
            content_hash=content_hash(
                json.dumps(
                    proposition_extraction_jobs,
                    sort_keys=True,
                )
            ),
            metadata={"proposition_extraction_job_count": len(proposition_extraction_jobs)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-proposition-completeness-assessments",
            run_id=run.id,
            artifact_type="proposition_completeness_assessments",
            provenance="pipeline.proposition_completeness",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in proposition_completeness_assessments],
                    sort_keys=True,
                )
            ),
            metadata={
                "proposition_completeness_assessment_count": len(proposition_completeness_assessments),
            },
        ),
        RunArtifact(
            id=f"artifact-{run.id}-legal-scopes",
            run_id=run.id,
            artifact_type="legal_scopes",
            provenance="pipeline.scope_link",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in scope_payload.legal_scopes],
                    sort_keys=True,
                )
            ),
            metadata={"legal_scope_count": len(scope_payload.legal_scopes)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-proposition-scope-links",
            run_id=run.id,
            artifact_type="proposition_scope_links",
            provenance="pipeline.scope_link",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in scope_payload.proposition_scope_links],
                    sort_keys=True,
                )
            ),
            metadata={
                "proposition_scope_link_count": len(scope_payload.proposition_scope_links),
            },
        ),
        RunArtifact(
            id=f"artifact-{run.id}-scope-inventory",
            run_id=run.id,
            artifact_type="scope_inventory",
            provenance="pipeline.scope_link",
            content_hash=content_hash(
                json.dumps(scope_payload.scope_inventory, sort_keys=True)
            ),
            metadata={},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-scope-review-candidates",
            run_id=run.id,
            artifact_type="scope_review_candidates",
            provenance="pipeline.scope_link",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in scope_payload.scope_review_candidates],
                    sort_keys=True,
                )
            ),
            metadata={"scope_review_candidate_count": len(scope_payload.scope_review_candidates)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-assessments",
            run_id=run.id,
            artifact_type="divergence_assessments",
            provenance="pipeline.compare",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in divergence_assessments],
                    sort_keys=True,
                )
            ),
            metadata={"assessment_count": len(divergence_assessments)},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-narrative",
            run_id=run.id,
            artifact_type="narrative_export",
            provenance="pipeline.export",
            content_hash=content_hash(
                json.dumps(narrative.model_dump(mode="json"), sort_keys=True)
            ),
            metadata={"title": narrative.title},
        ),
        RunArtifact(
            id=f"artifact-{run.id}-review-decisions",
            run_id=run.id,
            artifact_type="review_decisions",
            provenance="pipeline.review",
            content_hash=content_hash(
                json.dumps(
                    [item.model_dump(mode="json") for item in review_decisions], sort_keys=True
                )
            ),
            metadata={"review_decision_count": len(review_decisions)},
        ),
    ]
    if sf_validated_json:
        run_artifacts.append(
            RunArtifact(
                id=f"artifact-{run.id}-source-family-candidates",
                run_id=run.id,
                artifact_type="source_family_candidates",
                provenance="registry.source_family",
                content_hash=content_hash(json.dumps(sf_validated_json, sort_keys=True)),
                metadata={"source_family_candidate_count": len(sf_validated_json)},
            )
        )
    run.run_artifact_ids = [artifact.id for artifact in run_artifacts]
    run.review_decision_ids = [review.id for review in review_decisions]

    bundle = build_bundle(
        topic=topic,
        clusters=[cluster],
        run=run,
        sources=sources,
        source_fetch_metadata=source_fetch_metadata,
        source_fetch_attempts=[item.model_dump(mode="json") for item in source_fetch_attempts],
        source_target_links=[item.model_dump(mode="json") for item in source_target_links],
        source_inventory=source_inventory_artifact.model_dump(mode="json"),
        source_categorisation_rationales=[
            item.model_dump(mode="json") for item in source_categorisation_rationales
        ],
        source_snapshots=source_snapshots,
        source_fragments=source_fragments,
        source_parse_traces=source_parse_traces,
        proposition_extraction_traces=proposition_extraction_traces,
        proposition_extraction_jobs=proposition_extraction_jobs,
        proposition_extraction_failures=proposition_extraction_failures,
        proposition_completeness_assessments=proposition_completeness_assessments,
        propositions=propositions,
        proposition_inventory=proposition_inventory,
        divergence_findings=divergence_findings,
        divergence_observations=divergence_observations,
        divergence_assessments=divergence_assessments,
        review_decisions=review_decisions,
        run_artifacts=run_artifacts,
        source_family_candidates=sf_validated_json,
        narrative=narrative,
        legal_scopes=scope_payload.legal_scopes,
        proposition_scope_links=scope_payload.proposition_scope_links,
        scope_inventory=scope_payload.scope_inventory,
        scope_review_candidates=scope_payload.scope_review_candidates,
    )
    bundle["stage_traces"] = stage_traces
    bundle["extraction_llm_call_traces"] = extraction_llm_diagnostic_traces
    bundle["proposition_extraction_jobs"] = proposition_extraction_jobs
    bundle["pipeline_case_inputs"] = {
        "comparison": comparison_cfg,
        "narrative": narrative_cfg,
        "extraction": dict(case_data.get("extraction") or {}),
        "prompts": dict(prompt_cfg),
        "analysis_mode": str(case_data.get("analysis_mode", "auto")),
        "analysis_scope": str(case_data.get("analysis_scope") or "selected_sources"),
        "strategy_versions": dict(strategy_versions),
        "pipeline_version": pipeline_version,
    }
    bundle["missing_required_fragment_locators"] = sorted(required_locator_misses)
    return bundle


def repair_extraction_from_export_dir(
    *,
    base_bundle: dict[str, Any],
    export_dir_abs: str,
    output_export_dir: Path,
    new_run_id: str,
    extraction_mode: str,
    extraction_fallback: str,
    only: Literal["repairable", "all"],
    in_place: bool,
    retry_failed_llm: bool,
    source_cache_dir: str | None,
    derived_cache_dir: str | None,
    use_llm: bool,
    progress: Any | None,
) -> dict[str, Any]:
    pr = progress if progress is not None else null_pipeline_progress()

    fragments_raw = base_bundle.get("source_fragments") or []
    sources_raw = base_bundle.get("source_records") or base_bundle.get("sources") or []
    all_job_keys: set[tuple[str, str | None]] = set()
    if isinstance(fragments_raw, list) and fragments_raw:
        for frow in fragments_raw:
            if not isinstance(frow, dict):
                continue
            rid = str(frow.get("source_record_id") or "").strip()
            if rid:
                all_job_keys.add((rid, str(frow.get("id") or "").strip() or None))
    else:
        for srow in sources_raw:
            if isinstance(srow, dict) and str(srow.get("id") or "").strip():
                all_job_keys.add((str(srow["id"]), None))

    if only == "repairable":
        from .extraction_repair import (
            list_repairable_extraction_chunks,
            repair_job_keys_from_chunks,
        )

        targets = repair_job_keys_from_chunks(list_repairable_extraction_chunks(base_bundle))
        if not targets:
            raise ValueError(
                "No repairable frontier/model extraction failures detected in this export."
            )
        kept_props: list[Proposition] = []
        for row in base_bundle.get("propositions") or []:
            if not isinstance(row, dict):
                continue
            cand = Proposition.model_validate(row)
            pk = (
                str(cand.source_record_id),
                str(cand.source_fragment_id).strip() if cand.source_fragment_id else None,
            )
            if pk in targets:
                continue
            kept_props.append(cand)
        kept_for_repair: list[Proposition] | None = kept_props
    else:
        targets = all_job_keys
        kept_for_repair = None

    from .extraction_repair import build_case_data_for_repair

    run_existing = (
        str(base_bundle.get("run", {}).get("id") or "") if isinstance(base_bundle.get("run"), dict) else ""
    )
    effective_run_id = run_existing if in_place else new_run_id
    case_data = build_case_data_for_repair(base_bundle, new_run_id=effective_run_id)

    case_data.setdefault("extraction", {})
    case_data["extraction"]["mode"] = extraction_mode
    case_data["extraction"]["fallback_policy"] = extraction_fallback

    bundle_out = build_bundle_from_case(
        case_data=case_data,
        use_llm=use_llm,
        extraction_mode=extraction_mode,
        extraction_fallback=extraction_fallback,
        divergence_reasoning="frontier" if use_llm else None,
        source_cache_dir=source_cache_dir,
        derived_cache_dir=derived_cache_dir,
        intake_bundle=base_bundle,
        extraction_repair_targets=targets,
        extraction_repair_kept_propositions=kept_for_repair,
        retry_failed_llm=retry_failed_llm,
        progress=pr,
    )

    repaired_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    base_run = base_bundle.get("run") if isinstance(base_bundle.get("run"), dict) else {}

    bundle_out["extraction_repair_metadata"] = {
        "repaired_from_run_id": base_run.get("id"),
        "repaired_from_export_dir": export_dir_abs,
        "repaired_at": repaired_at,
        "repaired_chunk_jobs": sorted([list(k) for k in targets]),
        "repaired_chunk_count": len(targets),
        "in_place": in_place,
    }

    Path(output_export_dir).mkdir(parents=True, exist_ok=True)
    export_bundle(bundle_out, output_dir=str(Path(output_export_dir).resolve()))
    return bundle_out


def run_case_file(
    case_path: str,
    use_llm: bool = False,
    extraction_mode: str | None = None,
    extraction_execution_mode: str | None = None,
    extraction_fallback: str = "fallback",
    divergence_reasoning: str | None = None,
    source_cache_dir: str | None = None,
    derived_cache_dir: str | None = None,
    progress: Any | None = None,
) -> dict[str, Any]:
    pr = progress if progress is not None else null_pipeline_progress()
    pr.stage("Loading case", detail=Path(case_path).name)
    case_data = load_case_file(case_path)
    return build_bundle_from_case(
        case_data=case_data,
        use_llm=use_llm,
        extraction_mode=extraction_mode,
        extraction_execution_mode=extraction_execution_mode,
        extraction_fallback=extraction_fallback,
        divergence_reasoning=divergence_reasoning,
        source_cache_dir=source_cache_dir,
        derived_cache_dir=derived_cache_dir,
        progress=pr,
    )


def export_case_file(
    case_path: str,
    output_dir: str,
    use_llm: bool = False,
    extraction_mode: str | None = None,
    extraction_execution_mode: str | None = None,
    extraction_fallback: str = "fallback",
    divergence_reasoning: str | None = None,
    source_cache_dir: str | None = None,
    derived_cache_dir: str | None = None,
    progress: Any | None = None,
) -> dict[str, Any]:
    pr = progress if progress is not None else null_pipeline_progress()
    bundle = run_case_file(
        case_path=case_path,
        use_llm=use_llm,
        extraction_mode=extraction_mode,
        extraction_execution_mode=extraction_execution_mode,
        extraction_fallback=extraction_fallback,
        divergence_reasoning=divergence_reasoning,
        source_cache_dir=source_cache_dir,
        derived_cache_dir=derived_cache_dir,
        progress=pr,
    )
    pr.stage("Export bundle", detail=str(output_dir))
    export_bundle(bundle=bundle, output_dir=output_dir)
    rq = bundle.get("run_quality_summary")
    if isinstance(rq, dict):
        pr.stage(
            "Lint / quality summary",
            detail=f"status={rq.get('status')}, warnings={rq.get('warning_count', 0)}",
        )
    return bundle


def run_registry_sources(
    *,
    registry_ids: list[str],
    topic_name: str,
    cluster_name: str | None = None,
    topic_description: str = "",
    subject_tags: list[str] | None = None,
    analysis_mode: str = "auto",
    analysis_scope: str = "selected_sources",
    run_id: str | None = None,
    run_notes: str = "",
    comparison_jurisdiction_a: str | None = None,
    comparison_jurisdiction_b: str | None = None,
    proposition_index: int = 0,
    refresh_sources: bool = False,
    source_registry_path: str | None = None,
    use_llm: bool = False,
    extraction_mode: str | None = None,
    extraction_execution_mode: str | None = None,
    extraction_fallback: str = "fallback",
    divergence_reasoning: str | None = None,
    source_cache_dir: str | None = None,
    derived_cache_dir: str | None = None,
    source_family_selection: dict[str, Any] | None = None,
    focus_scopes: list[str] | None = None,
    max_propositions_per_source: int | None = None,
    model_error_policy: str | None = None,
    progress: Any | None = None,
) -> dict[str, Any]:
    if not registry_ids:
        raise ValueError("registry_ids must contain at least one source.")

    pr = progress if progress is not None else null_pipeline_progress()
    pr.stage("Loading case", detail="registry sources")

    registry = SourceRegistryService(
        registry_path=source_registry_path,
        source_cache_dir=source_cache_dir,
    )
    if refresh_sources:
        for registry_id in registry_ids:
            registry.refresh_reference(registry_id=registry_id)

    case_sources = registry.build_case_sources(registry_ids=registry_ids)
    analysis_scope_eff = (analysis_scope or "selected_sources").strip().lower()
    validate_registry_divergence_inputs(
        case_sources=case_sources,
        analysis_scope=analysis_scope_eff,
        analysis_mode=analysis_mode,
        comparison_jurisdiction_a=comparison_jurisdiction_a,
        comparison_jurisdiction_b=comparison_jurisdiction_b,
    )
    case_sources = filter_registry_case_sources_by_scope(case_sources, analysis_scope_eff)
    if not case_sources:
        raise ValueError(
            f"No sources remain after analysis_scope={analysis_scope_eff!r} filter. "
            "Adjust registry selection or analysis scope."
        )
    comparison_cfg = build_registry_comparison_config(
        case_sources=case_sources,
        proposition_index=proposition_index,
        comparison_jurisdiction_a=comparison_jurisdiction_a,
        comparison_jurisdiction_b=comparison_jurisdiction_b,
        analysis_scope=analysis_scope_eff,
        analysis_mode=analysis_mode,
    )

    resolved_cluster_name = cluster_name or f"{topic_name} cluster"
    resolved_run_id = run_id or f"run-registry-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"

    sf_bundle: list[dict[str, Any]] = []
    if source_family_selection and isinstance(source_family_selection, dict):
        reg_pick = str(source_family_selection.get("registry_id") or "").strip()
        included = source_family_selection.get("included_candidate_ids") or []
        if reg_pick and isinstance(included, list) and included:
            entry_sel = registry.inspect_entry(reg_pick)
            discovered = discover_related_for_registry_entry(entry_sel)
            chosen = candidates_for_included_ids(
                list(discovered.get("candidates") or []),
                [str(item) for item in included],
            )
            sf_bundle = [c.model_dump(mode="json") for c in chosen]

    case_data: dict[str, Any] = {
        "run_id": resolved_run_id,
        "run_notes": run_notes,
        "analysis_mode": analysis_mode,
        "analysis_scope": analysis_scope_eff,
        "topic": {
            "name": topic_name,
            "description": topic_description,
            "subject_tags": subject_tags or [],
        },
        "cluster": {
            "name": resolved_cluster_name,
            "description": "Generated from source registry references.",
        },
        "sources": case_sources,
        "comparison": comparison_cfg,
        "source_family_candidates": sf_bundle,
    }
    extraction_cfg: dict[str, Any] = {}
    if focus_scopes is not None:
        extraction_cfg["focus_scopes"] = list(focus_scopes)
    if max_propositions_per_source is not None:
        extraction_cfg["max_propositions_per_source"] = max_propositions_per_source
    if model_error_policy:
        extraction_cfg["model_error_policy"] = model_error_policy
    if extraction_execution_mode:
        extraction_cfg["execution_mode"] = extraction_execution_mode
    if extraction_cfg:
        case_data["extraction"] = extraction_cfg
    return build_bundle_from_case(
        case_data=case_data,
        use_llm=use_llm,
        extraction_mode=extraction_mode,
        extraction_execution_mode=extraction_execution_mode,
        extraction_fallback=extraction_fallback,
        divergence_reasoning=divergence_reasoning,
        source_cache_dir=source_cache_dir,
        derived_cache_dir=derived_cache_dir,
        progress=pr,
    )
