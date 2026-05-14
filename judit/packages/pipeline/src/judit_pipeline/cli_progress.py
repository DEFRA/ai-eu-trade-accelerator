"""Rich-based pipeline progress for CLI commands."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator, Literal

from rich.console import Console, RenderableType
from rich.status import Status
from rich.table import Table

# ---- Controller protocol (duck-typed; avoid importing Protocol for older runners) ----


class NullPipelineProgress:
    __slots__ = ()

    def stage(self, title: str, *, detail: str | None = None) -> None:
        return

    def extraction_source(self, index: int, total: int, mode: str, source_label: str) -> None:
        return

    def before_model_extract(
        self,
        kind: Literal["frontier", "local"],
        index: int,
        total: int,
        source_label: str,
        *,
        source_record_id: str | None = None,
        estimated_input_tokens: int | None = None,
        extraction_llm_chunk_index: int | None = None,
        extraction_llm_chunk_total: int | None = None,
        trace: dict[str, Any] | None = None,
    ) -> None:
        return

    def fallback_notice(self, source_label: str, reason: str | None) -> None:
        return

    def verbose(self, message: str) -> None:
        return

    def extraction_source_complete(self, outcome: Any) -> None:
        return


_NULL = NullPipelineProgress()


def null_pipeline_progress() -> NullPipelineProgress:
    return _NULL


class RichPipelineProgress:
    __slots__ = ("_console", "_status", "_verbose")

    def __init__(self, console: Console, status: Status, verbose: bool) -> None:
        self._console = console
        self._status = status
        self._verbose = verbose

    @staticmethod
    def _short(label: str, max_len: int = 72) -> str:
        s = label.strip()
        if len(s) <= max_len:
            return s
        return s[: max_len - 1] + "…"

    def stage(self, title: str, *, detail: str | None = None) -> None:
        if detail:
            renderable: RenderableType = f"[bold]{title}[/bold] — [dim]{detail}[/dim]"
        else:
            renderable = f"[bold]{title}[/bold]"
        self._status.update(renderable)

    def extraction_source(self, index: int, total: int, mode: str, source_label: str) -> None:
        lab = self._short(source_label)
        self._status.update(
            f"[bold]Proposition extraction[/bold] — [cyan]{mode}[/cyan] "
            f"source [white]{index}/{total}[/white] ({lab})"
        )

    def before_model_extract(
        self,
        kind: Literal["frontier", "local"],
        index: int,
        total: int,
        source_label: str,
        *,
        source_record_id: str | None = None,
        estimated_input_tokens: int | None = None,
        extraction_llm_chunk_index: int | None = None,
        extraction_llm_chunk_total: int | None = None,
        trace: dict[str, Any] | None = None,
    ) -> None:
        _ = trace
        call = "frontier_extract" if kind == "frontier" else "local_extract"
        lab = self._short(source_label)
        tid = (source_record_id or "").strip()
        tok = (
            f", estimated tokens: {estimated_input_tokens}"
            if isinstance(estimated_input_tokens, int) and estimated_input_tokens > 0
            else ""
        )
        chunk_hint = ""
        if isinstance(extraction_llm_chunk_index, int) and isinstance(extraction_llm_chunk_total, int):
            chunk_hint = f" — chunk {extraction_llm_chunk_index}/{extraction_llm_chunk_total}"
        sid = f"{tid}{tok}" if tid else ""
        id_frag = f": {sid}" if sid else ""
        self._status.update(
            f"[bold]Proposition extraction[/bold] — Calling [magenta]{call}[/magenta] "
            f"for source [white]{index}/{total}[/white]{id_frag}{chunk_hint} ({lab})…"
        )

    def fallback_notice(self, source_label: str, reason: str | None) -> None:
        r = (reason or "").strip()
        if len(r) > 160:
            r = r[:159] + "…"
        lab = self._short(source_label, 48)
        self._console.print(f"[yellow]↪ Fallback[/yellow] [dim]{lab}[/dim][dim]:[/dim] {r}")

    def verbose(self, message: str) -> None:
        if self._verbose:
            self._console.print(f"[dim]{message}[/dim]")

    def extraction_source_complete(self, outcome: Any) -> None:
        traces = getattr(outcome, "extraction_llm_call_traces", None) or []
        rows = [t for t in traces if isinstance(t, dict)]
        propositions = getattr(outcome, "propositions", None) or []
        prop_count = len(propositions) if isinstance(propositions, list) else 0
        fallback_used = bool(getattr(outcome, "fallback_used", False))
        failed_closed = bool(getattr(outcome, "failed_closed", False))
        ctx_risk = any(str(t.get("skip_reason") or "") == "context_window_risk" for t in rows)
        src_id = ""
        if rows:
            src_id = str(rows[0].get("source_record_id") or "").strip()
        if not src_id:
            src_id = str(getattr(outcome, "source_record_id", "") or "").strip()
        source_hint = f" {src_id}" if src_id else ""
        status_bits = [f"propositions: {prop_count}"]
        if fallback_used:
            status_bits.append("fallback_used: true")
        if ctx_risk:
            status_bits.append("context_window_risk: true")
        if failed_closed:
            status_bits.append("failed_closed: true")
        self._status.update(
            "[bold]Proposition extraction[/bold] — "
            f"completed source{source_hint} ({', '.join(status_bits)})"
        )


@contextmanager
def pipeline_progress(console: Console, *, quiet: bool, verbose: bool) -> Generator[object, None, None]:
    if quiet:
        yield _NULL
        return
    with console.status("[dim]Starting…[/dim]", spinner="dots") as status:
        yield RichPipelineProgress(console, status, verbose)


def print_completion_summary_table(console: Console, summary: dict[str, Any]) -> None:
    table = Table(title="Run summary", show_header=True, header_style="bold")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")
    od = str(summary.get("output_directory") or "").strip()
    table.add_row("Sources", str(summary.get("sources", "")))
    table.add_row("Propositions", str(summary.get("propositions", "")))
    table.add_row("Extraction mode", str(summary.get("extraction_mode", "")))
    table.add_row("Fallback count", str(summary.get("fallback_count", "")))
    table.add_row("Low-confidence (extraction traces)", str(summary.get("low_confidence_count", "")))
    table.add_row("Non-blocking lint warnings", str(summary.get("validation_warning_count", "")))
    table.add_row("Output directory", od or "—")
    table.add_row("Run quality", str(summary.get("run_quality_status", "")))
    mode = str(summary.get("extraction_mode") or "")
    if mode in {"local", "frontier"}:
        dash = "—"

        def _cell(v: object) -> str:
            if v is None:
                return dash
            s = str(v).strip()
            return s if s else dash

        table.add_row("LLM extraction calls", _cell(summary.get("llm_extraction_call_count")))
        table.add_row("LLM extraction skipped", _cell(summary.get("llm_extraction_skipped_count")))
        table.add_row("Max estimated input tokens", _cell(summary.get("max_estimated_input_tokens")))
        table.add_row("Context-window risk traces", _cell(summary.get("context_window_risk_count")))
        table.add_row("Largest extraction fragment locator", _cell(summary.get("largest_extraction_fragment_locator")))
        table.add_row("Largest extraction source id", _cell(summary.get("largest_extraction_source_record_id")))
    console.print(table)
