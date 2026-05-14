"""Pure helpers for CLI completion summary (testable without Rich)."""

from __future__ import annotations

from typing import Any

from .extraction_llm_metrics import (
    compute_extraction_llm_trace_summary_metrics,
    extraction_llm_call_traces_from_bundle,
)


def extraction_mode_from_bundle(bundle: dict[str, Any]) -> str:
    for tr in bundle.get("stage_traces") or []:
        if not isinstance(tr, dict):
            continue
        if str(tr.get("stage_name") or "") != "proposition extraction":
            continue
        inp = tr.get("inputs")
        if isinstance(inp, dict):
            mode = inp.get("extraction_mode")
            if mode is not None and str(mode).strip():
                return str(mode).strip()
    return "unknown"


def count_extraction_fallback_traces(bundle: dict[str, Any]) -> int:
    traces = bundle.get("proposition_extraction_traces")
    if not isinstance(traces, list):
        return 0
    return sum(1 for t in traces if isinstance(t, dict) and t.get("fallback_used"))


def count_low_confidence_extraction_traces(bundle: dict[str, Any]) -> int:
    traces = bundle.get("proposition_extraction_traces")
    if not isinstance(traces, list):
        return 0
    return sum(
        1 for t in traces if isinstance(t, dict) and str(t.get("confidence") or "").lower() == "low"
    )


def build_cli_completion_summary(
    bundle: dict[str, Any],
    *,
    quality_summary: dict[str, Any],
    output_dir: str | None,
) -> dict[str, Any]:
    sources = bundle.get("source_records")
    if not isinstance(sources, list):
        sources = bundle.get("sources")
    n_sources = len(sources) if isinstance(sources, list) else 0
    props = bundle.get("propositions")
    n_props = len(props) if isinstance(props, list) else 0
    ext_mode = extraction_mode_from_bundle(bundle)
    summary: dict[str, Any] = {
        "sources": n_sources,
        "propositions": n_props,
        "extraction_mode": ext_mode,
        "fallback_count": count_extraction_fallback_traces(bundle),
        "low_confidence_count": count_low_confidence_extraction_traces(bundle),
        "validation_warning_count": int(quality_summary.get("warning_count") or 0),
        "output_directory": output_dir or "",
        "run_quality_status": str(quality_summary.get("status") or "unknown"),
    }
    if ext_mode in {"local", "frontier"}:
        llm_metrics = compute_extraction_llm_trace_summary_metrics(
            extraction_llm_call_traces_from_bundle(bundle)
        )
        summary.update(llm_metrics)
    return summary
