from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from judit_domain import Cluster, Proposition, SourceRecord, Topic

from .derived_cache import DerivedArtifactCache
from .extract import (
    EXTRACTION_SCHEMA_VERSION_V2,
    _build_propositions_from_v2_rows,
    _merge_dedupe_validated_v2_rows,
    _parse_json,
    _parse_model_propositions_container,
    _stamp_props_meta,
    _validate_v2_items,
    plan_frontier_extraction_requests,
    PlannedExtractionRequest,
)
from .intake import create_cluster, create_topic
from .source_fragmentation import expand_monolithic_source_fragment, max_fragment_body_chars_for_llm_budget
from .sources.service import SourceIngestionService


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class BatchLLMClient(Protocol):
    def submit_batch(self, requests: list[PlannedExtractionRequest]) -> str: ...

    def get_batch_status(self, batch_job_id: str) -> dict[str, Any]: ...

    def fetch_batch_results(self, batch_job_id: str) -> list[dict[str, Any]]: ...


class AnthropicBatchLLMClient:
    """Small adapter around Anthropic Message Batches API."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_base_url: str = "https://api.anthropic.com/v1",
        anthropic_version: str = "2023-06-01",
    ) -> None:
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
        self.api_base_url = api_base_url.rstrip("/")
        self.anthropic_version = anthropic_version

    def _request(self, *, path: str, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY is required for Anthropic batch execution.")
        body = None
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": self.anthropic_version,
            "content-type": "application/json",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.api_base_url}{path}",
            method=method,
            headers=headers,
            data=body,
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            err = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Anthropic batch API error ({exc.code}): {err}") from exc
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError("Unexpected Anthropic response payload.")
        return data

    def submit_batch(self, requests: list[PlannedExtractionRequest]) -> str:
        entries: list[dict[str, Any]] = []
        for req in requests:
            entries.append(
                {
                    "custom_id": req.request_id,
                    "params": {
                        "model": req.model_alias,
                        "max_tokens": 4096,
                        "system": req.system_text or "",
                        "messages": [{"role": "user", "content": req.prompt_text}],
                        "temperature": 0.0,
                    },
                }
            )
        payload = self._request(path="/messages/batches", method="POST", payload={"requests": entries})
        batch_id = str(payload.get("id") or "").strip()
        if not batch_id:
            raise RuntimeError("Anthropic batch submission did not return id.")
        return batch_id

    def get_batch_status(self, batch_job_id: str) -> dict[str, Any]:
        return self._request(path=f"/messages/batches/{batch_job_id}")

    def fetch_batch_results(self, batch_job_id: str) -> list[dict[str, Any]]:
        payload = self._request(path=f"/messages/batches/{batch_job_id}/results")
        rows = payload.get("data")
        if not isinstance(rows, list):
            return []
        return [item for item in rows if isinstance(item, dict)]


@dataclass
class BatchImportResult:
    propositions: list[Proposition]
    validation_errors: list[str]
    validation_issue_records: list[dict[str, Any]]
    extraction_llm_call_traces: list[dict[str, Any]]
    failed_result_count: int


@dataclass
class PlannedBatchCase:
    topic: Topic
    cluster: Cluster
    requests: list[PlannedExtractionRequest]
    source_by_request_id: dict[str, SourceRecord]
    source_count: int
    fragment_count: int


class BatchJobStore:
    def __init__(self, export_dir: str | Path) -> None:
        self.root = Path(export_dir) / "runs" / "_batch_jobs"
        self.root.mkdir(parents=True, exist_ok=True)

    def job_dir(self, batch_job_id: str) -> Path:
        return self.root / str(batch_job_id)

    def write_job(
        self,
        *,
        batch_job_id: str,
        metadata: dict[str, Any],
        requests: list[PlannedExtractionRequest],
    ) -> None:
        jd = self.job_dir(batch_job_id)
        jd.mkdir(parents=True, exist_ok=True)
        (jd / "batch_job.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        (jd / "requests.json").write_text(
            json.dumps([asdict(item) for item in requests], indent=2), encoding="utf-8"
        )

    def write_results(self, *, batch_job_id: str, results: list[dict[str, Any]]) -> None:
        jd = self.job_dir(batch_job_id)
        jd.mkdir(parents=True, exist_ok=True)
        (jd / "results.jsonl").write_text(
            "\n".join(json.dumps(item, sort_keys=True) for item in results),
            encoding="utf-8",
        )

    def write_errors(self, *, batch_job_id: str, errors: list[dict[str, Any]]) -> None:
        jd = self.job_dir(batch_job_id)
        jd.mkdir(parents=True, exist_ok=True)
        (jd / "errors.json").write_text(json.dumps(errors, indent=2), encoding="utf-8")


def plan_frontier_batch_for_source(
    *,
    source: SourceRecord,
    topic: Topic,
    cluster: Cluster,
    model_alias: str,
    max_propositions: int,
    focus_scopes: list[str] | None,
    prompt_version: str,
    max_input_tokens: int,
    extract_model_context_limit: int,
    derived_chunk_cache: DerivedArtifactCache | None,
    chunk_cache_pipeline_version: str,
    chunk_cache_strategy_version: str,
) -> list[PlannedExtractionRequest]:
    return plan_frontier_extraction_requests(
        source=source,
        topic=topic,
        cluster=cluster,
        model_alias=model_alias,
        max_propositions=max_propositions,
        focus_scopes=focus_scopes,
        prompt_version=prompt_version,
        schema_version=EXTRACTION_SCHEMA_VERSION_V2,
        max_input_tokens=max_input_tokens,
        extract_model_context_limit=extract_model_context_limit,
        derived_chunk_cache=derived_chunk_cache,
        chunk_cache_pipeline_version=chunk_cache_pipeline_version,
        chunk_cache_strategy_version=chunk_cache_strategy_version,
        include_cached_successes=False,
    )


def plan_frontier_batch_for_case(
    *,
    case_data: dict[str, Any],
    model_alias: str,
    source_cache_dir: str | None = None,
    derived_cache_dir: str | None = None,
    max_input_tokens: int = 150_000,
    extract_model_context_limit: int = 200_000,
    prompt_version: str = "v2",
    chunk_cache_pipeline_version: str = "0.1.0",
    chunk_cache_strategy_version: str = "v1",
) -> PlannedBatchCase:
    topic_cfg = case_data["topic"]
    cluster_cfg = case_data["cluster"]
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
    extraction_cfg = case_data.get("extraction") if isinstance(case_data.get("extraction"), dict) else {}
    scopes_raw = extraction_cfg.get("focus_scopes") if isinstance(extraction_cfg, dict) else None
    focus_scopes = [str(item).strip() for item in scopes_raw or [] if str(item).strip()]
    max_props = extraction_cfg.get("max_propositions_per_source") if isinstance(extraction_cfg, dict) else None
    max_propositions = int(max_props) if isinstance(max_props, int) and max_props > 0 else 4
    ingest = SourceIngestionService(cache_dir=Path(source_cache_dir) if source_cache_dir else None)
    intake = ingest.ingest_sources(case_data.get("sources") or [])
    max_body = max_fragment_body_chars_for_llm_budget(max_extract_input_tokens=max_input_tokens)
    overlap = min(8192, max(512, max_input_tokens // 40))
    derived_cache = (
        DerivedArtifactCache(cache_dir=Path(derived_cache_dir))
        if derived_cache_dir
        else None
    )
    requests: list[PlannedExtractionRequest] = []
    source_by_request_id: dict[str, SourceRecord] = {}
    expanded_count = 0
    by_record = {s.id: s for s in intake.sources}
    for frag in intake.fragments:
        expanded = expand_monolithic_source_fragment(
            frag,
            max_body_chars=max_body,
            overlap_chars=overlap,
        )
        for piece in expanded:
            expanded_count += 1
            src = by_record[piece.source_record_id]
            metadata = dict(src.metadata) if isinstance(src.metadata, dict) else {}
            metadata["extraction_fragment_id"] = piece.id
            work = src.model_copy(
                deep=True,
                update={
                    "authoritative_text": piece.fragment_text,
                    "authoritative_locator": piece.locator,
                    "metadata": metadata,
                },
            )
            planned = plan_frontier_batch_for_source(
                source=work,
                topic=topic,
                cluster=cluster,
                model_alias=model_alias,
                max_propositions=max_propositions,
                focus_scopes=focus_scopes or None,
                prompt_version=prompt_version,
                max_input_tokens=max_input_tokens,
                extract_model_context_limit=extract_model_context_limit,
                derived_chunk_cache=derived_cache,
                chunk_cache_pipeline_version=chunk_cache_pipeline_version,
                chunk_cache_strategy_version=chunk_cache_strategy_version,
            )
            requests.extend(planned)
            for req in planned:
                source_by_request_id[req.request_id] = work
    return PlannedBatchCase(
        topic=topic,
        cluster=cluster,
        requests=requests,
        source_by_request_id=source_by_request_id,
        source_count=len(intake.sources),
        fragment_count=expanded_count,
    )


def import_frontier_batch_results(
    *,
    requests: list[PlannedExtractionRequest],
    provider_results: dict[str, str],
    source_by_request_id: dict[str, SourceRecord],
    topic: Topic,
    cluster: Cluster,
    extraction_fallback: str,
    derived_chunk_cache: DerivedArtifactCache | None = None,
) -> BatchImportResult:
    all_props: list[Proposition] = []
    valerrs: list[str] = []
    issue_records: list[dict[str, Any]] = []
    llm_traces: list[dict[str, Any]] = []
    failed = 0
    for req in requests:
        source = source_by_request_id[req.request_id]
        raw_text = provider_results.get(req.request_id, "")
        trace: dict[str, Any] = {
            "source_record_id": req.source_record_id,
            "source_title": req.source_title,
            "source_fragment_id": req.source_fragment_id,
            "fragment_locator": req.fragment_locator,
            "model_alias": req.model_alias,
            "estimated_input_tokens": req.estimated_input_tokens,
            "llm_invoked": True,
            "batch_request_id": req.request_id,
            "provider_status": "ok" if raw_text else "missing_result",
        }
        llm_traces.append(trace)
        if not raw_text:
            failed += 1
            valerrs.append(f"batch request {req.request_id}: missing provider result")
            continue
        try:
            parsed = _parse_json(raw_text)
            rows = _parse_model_propositions_container(parsed)
        except Exception as exc:
            failed += 1
            valerrs.append(f"batch request {req.request_id}: invalid JSON payload ({exc})")
            continue
        v_rows, verrs, row_issues = _validate_v2_items(rows, source.authoritative_text, limit=req.max_propositions_per_source)
        if not v_rows:
            failed += 1
        valerrs.extend([f"batch request {req.request_id}: {x}" for x in verrs])
        issue_records.extend(row_issues)
        deduped = _merge_dedupe_validated_v2_rows(v_rows, limit=req.max_propositions_per_source)
        if req.cache_key and derived_chunk_cache is not None and deduped:
            derived_chunk_cache.put(
                stage_name="proposition_extraction_chunk",
                cache_key=req.cache_key,
                payload={"chunk_status": "llm_success", "validated_rows": deduped},
            )
        props = _build_propositions_from_v2_rows(
            rows=deduped,
            source=source,
            topic=topic,
            cluster=cluster,
            limit=req.max_propositions_per_source,
            id_sequence_start=1,
        )
        _stamp_props_meta(
            props,
            extraction_mode="frontier",
            model_alias=req.model_alias,
            fallback_policy=extraction_fallback,
            fallback_used=False,
            validation_errors=verrs,
            prompt_version=req.prompt_version,
            schema_version=req.schema_version,
            pipeline_signals={
                "focus_scopes": req.focus_scopes,
                "batch_request_id": req.request_id,
                "estimated_input_tokens": req.estimated_input_tokens,
                "extraction_llm_call_traces": [trace],
            },
        )
        all_props.extend(props)
    return BatchImportResult(
        propositions=all_props,
        validation_errors=valerrs,
        validation_issue_records=issue_records,
        extraction_llm_call_traces=llm_traces,
        failed_result_count=failed,
    )
