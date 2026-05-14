"""Summaries derived from proposition-extraction `extraction_llm_call_traces` (no behaviour changes)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def extraction_llm_call_traces_from_bundle(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    """Rows accumulated during LLM proposition extraction (`inputs.extraction_llm_call_traces`)."""
    top = bundle.get("extraction_llm_call_traces")
    if isinstance(top, list) and top:
        return [t for t in top if isinstance(t, dict)]
    for tr in bundle.get("stage_traces") or []:
        if not isinstance(tr, dict):
            continue
        if str(tr.get("stage_name") or "") != "proposition extraction":
            continue
        inp = tr.get("inputs")
        if not isinstance(inp, dict):
            return []
        raw = inp.get("extraction_llm_call_traces")
        if not isinstance(raw, list):
            return []
        return [t for t in raw if isinstance(t, dict)]
    return []


def compute_extraction_llm_trace_summary_metrics(
    traces: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate diagnostics from `extraction_llm_call_traces` (no I/O)."""
    call_count = sum(1 for t in traces if t.get("llm_invoked") is True)
    skipped_count = sum(1 for t in traces if t.get("skipped_llm") is True)
    ctx_risk_count = sum(1 for t in traces if str(t.get("skip_reason") or "") == "context_window_risk")

    positive_ests: list[tuple[int, dict[str, Any]]] = []
    for t in traces:
        est = t.get("estimated_input_tokens")
        if isinstance(est, int) and est > 0:
            positive_ests.append((est, t))

    max_tok: int | None = max((e for e, _ in positive_ests), default=None)

    locator: str | None = None
    record_id: str | None = None
    if positive_ests:
        _best_est, best_row = max(positive_ests, key=lambda x: x[0])
        frag = best_row.get("fragment_locator")
        if frag is not None and str(frag).strip():
            locator = str(frag).strip()
        sid = best_row.get("source_record_id")
        if sid is not None and str(sid).strip():
            record_id = str(sid).strip()

    return {
        "llm_extraction_call_count": call_count,
        "llm_extraction_skipped_count": skipped_count,
        "max_estimated_input_tokens": max_tok,
        "context_window_risk_count": ctx_risk_count,
        "largest_extraction_fragment_locator": locator,
        "largest_extraction_source_record_id": record_id,
    }
