"""Aggregation of extraction_llm_call_traces into CLI / run-quality summaries."""

from judit_pipeline.cli_run_summary import build_cli_completion_summary
from judit_pipeline.extraction_llm_metrics import (
    compute_extraction_llm_trace_summary_metrics,
    extraction_llm_call_traces_from_bundle,
)
from judit_pipeline.run_quality import build_run_quality_summary


def test_compute_metrics_empty_traces() -> None:
    m = compute_extraction_llm_trace_summary_metrics([])
    assert m == {
        "llm_extraction_call_count": 0,
        "llm_extraction_skipped_count": 0,
        "max_estimated_input_tokens": None,
        "context_window_risk_count": 0,
        "largest_extraction_fragment_locator": None,
        "largest_extraction_source_record_id": None,
    }


def test_compute_metrics_calls_skips_and_context_risk() -> None:
    traces = [
        {
            "estimated_input_tokens": 100,
            "skipped_llm": False,
            "llm_invoked": True,
            "fragment_locator": "doc:a",
            "source_record_id": "src-a",
            "skip_reason": None,
        },
        {
            "estimated_input_tokens": 900,
            "skipped_llm": False,
            "llm_invoked": True,
            "fragment_locator": "doc:b",
            "source_record_id": "src-b",
            "skip_reason": None,
        },
        {
            "estimated_input_tokens": -1,
            "skipped_llm": True,
            "llm_invoked": False,
            "fragment_locator": "doc:c",
            "source_record_id": "src-c",
            "skip_reason": "context_window_risk",
        },
    ]
    m = compute_extraction_llm_trace_summary_metrics(traces)
    assert m["llm_extraction_call_count"] == 2
    assert m["llm_extraction_skipped_count"] == 1
    assert m["context_window_risk_count"] == 1
    assert m["max_estimated_input_tokens"] == 900
    assert m["largest_extraction_fragment_locator"] == "doc:b"
    assert m["largest_extraction_source_record_id"] == "src-b"


def test_compute_metrics_ignores_non_positive_estimates_for_max_and_largest() -> None:
    traces = [
        {
            "estimated_input_tokens": -1,
            "skipped_llm": True,
            "llm_invoked": False,
            "skip_reason": "context_window_risk",
            "fragment_locator": "frag-x",
            "source_record_id": "rec-x",
        },
        {
            "estimated_input_tokens": 0,
            "skipped_llm": True,
            "llm_invoked": False,
            "skip_reason": "",
            "fragment_locator": "frag-y",
            "source_record_id": "rec-y",
        },
    ]
    m = compute_extraction_llm_trace_summary_metrics(traces)
    assert m["max_estimated_input_tokens"] is None
    assert m["largest_extraction_fragment_locator"] is None
    assert m["largest_extraction_source_record_id"] is None
    assert m["context_window_risk_count"] == 1


def test_extraction_llm_call_traces_from_bundle_reads_proposition_stage() -> None:
    bundle: dict = {
        "stage_traces": [
            {"stage_name": "other", "inputs": {"extraction_llm_call_traces": [{"bogus": True}]}},
            {
                "stage_name": "proposition extraction",
                "inputs": {
                    "extraction_mode": "frontier",
                    "extraction_llm_call_traces": [
                        {"llm_invoked": True},
                        "not-a-dict",
                    ],
                },
            },
        ]
    }
    rows = extraction_llm_call_traces_from_bundle(bundle)
    assert rows == [{"llm_invoked": True}]


def test_build_cli_completion_summary_includes_llm_metrics_for_frontier() -> None:
    bundle: dict = {
        "source_records": [{"id": "s1"}],
        "propositions": [],
        "proposition_extraction_traces": [],
        "stage_traces": [
            {
                "stage_name": "proposition extraction",
                "inputs": {
                    "extraction_mode": "frontier",
                    "extraction_llm_call_traces": [
                        {
                            "estimated_input_tokens": 500,
                            "skipped_llm": False,
                            "llm_invoked": True,
                            "fragment_locator": "loc-z",
                            "source_record_id": "rec-z",
                        }
                    ],
                },
            }
        ],
    }
    summary = build_cli_completion_summary(
        bundle, quality_summary={"status": "pass", "warning_count": 0}, output_dir="/out"
    )
    assert summary["extraction_mode"] == "frontier"
    assert summary["llm_extraction_call_count"] == 1
    assert summary["max_estimated_input_tokens"] == 500
    assert summary["largest_extraction_fragment_locator"] == "loc-z"


def test_build_cli_completion_summary_omits_llm_metrics_for_heuristic() -> None:
    bundle: dict = {
        "source_records": [],
        "propositions": [],
        "stage_traces": [
            {
                "stage_name": "proposition extraction",
                "inputs": {
                    "extraction_mode": "heuristic",
                    "extraction_llm_call_traces": [{"llm_invoked": True}],
                },
            }
        ],
    }
    summary = build_cli_completion_summary(
        bundle, quality_summary={"status": "pass", "warning_count": 0}, output_dir=""
    )
    assert "llm_extraction_call_count" not in summary


def test_build_run_quality_summary_merges_llm_metrics_when_frontier() -> None:
    bundle: dict = {
        "run": {"id": "r1"},
        "source_records": [],
        "propositions": [],
        "stage_traces": [
            {
                "stage_name": "proposition extraction",
                "inputs": {
                    "extraction_mode": "local",
                    "extraction_llm_call_traces": [
                        {
                            "estimated_input_tokens": 120,
                            "skipped_llm": False,
                            "llm_invoked": True,
                            "fragment_locator": "p1",
                            "source_record_id": "src1",
                        }
                    ],
                },
            }
        ],
    }
    rq = build_run_quality_summary(bundle, lint_report={"ok": True, "errors": [], "warnings": [], "error_count": 0, "warning_count": 0})
    met = rq["metrics"]
    assert met["llm_extraction_call_count"] == 1
    assert met["max_estimated_input_tokens"] == 120
    assert met["largest_extraction_fragment_locator"] == "p1"


def test_build_run_quality_summary_no_llm_metric_keys_for_unknown_mode() -> None:
    bundle: dict = {
        "run": {"id": "r2"},
        "source_records": [],
        "propositions": [],
    }
    rq = build_run_quality_summary(bundle, lint_report={"ok": True, "errors": [], "warnings": [], "error_count": 0, "warning_count": 0})
    assert "llm_extraction_call_count" not in rq["metrics"]
