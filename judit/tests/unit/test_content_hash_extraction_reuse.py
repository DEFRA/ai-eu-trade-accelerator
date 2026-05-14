"""Extraction reuse by content_hash must preserve distinct source identity and audit trail."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from judit_pipeline.extract import (
    attach_judit_extraction_meta,
    attach_judit_extraction_reuse,
    parse_judit_extraction_meta,
    parse_judit_extraction_reuse,
)
from judit_pipeline.runner import run_registry_sources
from judit_pipeline.sources import SourceRegistryService


class _FakeJuditLLMClient:
    call_prompts: list[str] = []

    class _Settings:
        frontier_extract_model = "frontier_extract"
        local_extract_model = "local_extract"
        max_extract_input_tokens = 150_000
        extract_model_context_limit = 200_000

    def __init__(self) -> None:
        self.settings = self._Settings()

    def complete_text(  # type: ignore[no-untyped-def]
        self, *, prompt, model, system_prompt=None, temperature=0.0
    ) -> str:
        _ = (model, system_prompt, temperature)
        self.call_prompts.append(str(prompt))
        if "Article 1 text" in prompt:
            proposition_text = "Article 1 proposition"
            locator = "article:1"
            evidence_text = "Article 1 text"
        elif "Article 2 text" in prompt:
            proposition_text = "Article 2 proposition"
            locator = "article:2"
            evidence_text = "Article 2 text"
        elif "Annex I text" in prompt:
            proposition_text = "Annex I proposition"
            locator = "annex:i"
            evidence_text = "Annex I text"
        else:
            raise AssertionError("Unexpected prompt body for fake extraction client.")
        return json.dumps(
            {
                "propositions": [
                    {
                        "proposition_text": proposition_text,
                        "display_label": proposition_text,
                        "subject": "operators",
                        "rule": "must",
                        "object": "",
                        "conditions": [],
                        "exceptions": [],
                        "temporal_condition": "",
                        "provision_type": "core",
                        "source_locator": locator,
                        "evidence_text": evidence_text,
                        "completeness_status": "complete",
                        "confidence": "high",
                        "reason": "fake-client",
                    }
                ]
            }
        )


def _ref(
    *,
    authority_source_id: str,
    source_id: str,
    jurisdiction: str,
    citation: str,
    text: str,
) -> dict[str, object]:
    return {
        "authority": "case_file",
        "authority_source_id": authority_source_id,
        "id": source_id,
        "title": f"{jurisdiction} instrument",
        "jurisdiction": jurisdiction,
        "citation": citation,
        "kind": "regulation",
        "text": text,
        "authoritative_locator": "article:10",
        "version_id": "v1",
    }


def test_parse_judit_extraction_reuse_reads_appended_line_without_breaking_meta() -> None:
    meta = {"model": "frontier", "chunk": 1}
    base = attach_judit_extraction_meta("prior line", meta)
    notes = attach_judit_extraction_reuse(
        base,
        {
            "source_content_hash": "abc",
            "original_source_record_id": "src-a",
            "original_source_snapshot_id": "snap-a",
            "reused_for_source_record_id": "src-b",
            "reused_for_jurisdiction": "UK",
        },
    )
    assert parse_judit_extraction_meta(notes) == meta
    rep = parse_judit_extraction_reuse(notes)
    assert rep is not None
    assert rep["original_source_record_id"] == "src-a"
    assert rep["reused_for_jurisdiction"] == "UK"


def test_identical_eu_uk_text_reuses_extraction_once_and_preserves_snapshots(
    tmp_path: Path,
) -> None:
    reg = str(tmp_path / "reg.json")
    cache = str(tmp_path / "cache")
    derived = str(tmp_path / "derived")
    registry = SourceRegistryService(registry_path=reg, source_cache_dir=cache)

    dup_body = (
        "Article 10. Operators must maintain a movement register before dispatch. "
        "The competent authority may inspect the register on request."
    )
    eu = registry.register_reference(
        reference=_ref(
            authority_source_id="eu-same-body",
            source_id="src-eu-same",
            jurisdiction="EU",
            citation="EU-SAME",
            text=dup_body,
        ),
        refresh=True,
    )
    uk = registry.register_reference(
        reference=_ref(
            authority_source_id="uk-same-body",
            source_id="src-uk-same",
            jurisdiction="UK",
            citation="UK-SAME",
            text=dup_body,
        ),
        refresh=True,
    )

    extract_calls: list[str] = []

    def counting_extract(*, source, **kwargs):  # type: ignore[no-untyped-def]
        from judit_pipeline.extract import extract_propositions_from_source as real_extract

        extract_calls.append(str(source.id))
        return real_extract(source=source, **kwargs)

    with patch("judit_pipeline.runner.extract_propositions_from_source", side_effect=counting_extract):
        bundle = run_registry_sources(
            registry_ids=[eu["registry_id"], uk["registry_id"]],
            topic_name="Same body",
            analysis_mode="divergence",
            analysis_scope="selected_sources",
            comparison_jurisdiction_a="EU",
            comparison_jurisdiction_b="UK",
            source_registry_path=reg,
            source_cache_dir=cache,
            derived_cache_dir=derived,
            use_llm=False,
        )

    assert extract_calls == ["src-eu-same"]

    records = [r for r in bundle["source_records"] if isinstance(r, dict)]
    eu_rec = next(r for r in records if str(r.get("jurisdiction")) == "EU")
    uk_rec = next(r for r in records if str(r.get("jurisdiction")) == "UK")
    assert str(eu_rec.get("content_hash")) == str(uk_rec.get("content_hash"))
    assert str(eu_rec.get("current_snapshot_id")) != str(uk_rec.get("current_snapshot_id"))

    eu_props = [p for p in bundle["propositions"] if p["source_record_id"] == eu_rec["id"]]
    uk_props = [p for p in bundle["propositions"] if p["source_record_id"] == uk_rec["id"]]
    assert eu_props
    assert uk_props
    assert all(parse_judit_extraction_reuse(str(p.get("notes") or "")) is None for p in eu_props)
    for p in uk_props:
        assert p["jurisdiction"] == "UK"
        reuse = parse_judit_extraction_reuse(str(p.get("notes") or ""))
        assert reuse is not None
        assert reuse["original_source_record_id"] == eu_rec["id"]
        assert reuse["reused_for_source_record_id"] == uk_rec["id"]
        assert reuse["reused_for_jurisdiction"] == "UK"
        assert str(p.get("source_snapshot_id")) == str(uk_rec.get("current_snapshot_id"))

    eu_id = str(eu_props[0].get("id"))
    uk_id = str(uk_props[0].get("id"))
    assert eu_id and uk_id and eu_id != uk_id
    eu_pv = str(eu_props[0].get("proposition_version_id") or "")
    uk_pv = str(uk_props[0].get("proposition_version_id") or "")
    assert eu_pv and uk_pv and eu_pv != uk_pv


def test_fragment_level_extraction_does_not_reuse_first_fragment_result(tmp_path: Path) -> None:
    reg = str(tmp_path / "reg-frag.json")
    cache = str(tmp_path / "cache-frag")
    derived = str(tmp_path / "derived-frag")
    registry = SourceRegistryService(registry_path=reg, source_cache_dir=cache)
    ref = _ref(
        authority_source_id="frag-regression",
        source_id="src-frag-regression",
        jurisdiction="EU",
        citation="FRAG-REGRESSION",
        text="Article 1 text\n\nArticle 2 text\n\nAnnex I text",
    )
    ref["authoritative_locator"] = "document:full"
    ref["metadata"] = {
        "structural_fragments": [
            {"locator": "article:1", "text": "Article 1 text", "order_index": 0},
            {"locator": "article:2", "text": "Article 2 text", "order_index": 1},
            {"locator": "annex:i", "text": "Annex I text", "order_index": 2},
        ]
    }
    entry = registry.register_reference(reference=ref, refresh=True)
    _FakeJuditLLMClient.call_prompts = []
    with patch("judit_pipeline.runner.JuditLLMClient", _FakeJuditLLMClient):
        bundle = run_registry_sources(
            registry_ids=[str(entry["registry_id"])],
            topic_name="Fragment extraction",
            analysis_mode="single_source",
            analysis_scope="selected_sources",
            run_id="run-fragment-regression",
            use_llm=True,
            extraction_mode="frontier",
            extraction_fallback="fail_closed",
            source_registry_path=reg,
            source_cache_dir=cache,
            derived_cache_dir=derived,
            refresh_sources=False,
        )

    assert len(_FakeJuditLLMClient.call_prompts) == 3
    props = [p for p in bundle["propositions"] if p["source_record_id"] == "src-frag-regression"]
    assert len(props) == 3
    prop_ids = [str(p["id"]) for p in props]
    assert len(prop_ids) == len(set(prop_ids))
    prop_keys = [str(p.get("proposition_key") or "") for p in props]
    assert len(prop_keys) == len(set(prop_keys))

    traces = [
        t
        for t in bundle["proposition_extraction_traces"]
        if str(t.get("source_record_id")) == "src-frag-regression"
    ]
    assert len(traces) == 3
    trace_ids = [str(t["id"]) for t in traces]
    assert len(trace_ids) == len(set(trace_ids))

    fragments = {
        str(f["id"]): str(f["locator"])
        for f in bundle["source_fragments"]
        if str(f.get("source_record_id")) == "src-frag-regression"
    }
    assert len(fragments) >= 3
    seen_locators = set()
    for trace in traces:
        source_fragment_id = str(trace.get("source_fragment_id") or "")
        assert source_fragment_id in fragments
        expected_locator = fragments[source_fragment_id]
        seen_locators.add(expected_locator)
        assert str(trace.get("evidence_locator") or "").startswith(expected_locator)
    assert {"article:1", "article:2", "annex:i"}.issubset(seen_locators)


def test_fragment_level_extraction_calls_llm_once_per_fragment_with_fake_client(
    tmp_path: Path,
) -> None:
    test_fragment_level_extraction_does_not_reuse_first_fragment_result(tmp_path)


def test_trace_ids_unique_per_fragment(tmp_path: Path) -> None:
    test_fragment_level_extraction_does_not_reuse_first_fragment_result(tmp_path)

