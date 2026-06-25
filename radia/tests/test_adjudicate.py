"""Radia's pass-2 adjudicator: it re-reads pass-1 positives and drops false
positives, kept honest by a stubbed Anthropic batch (no network, no API key).
"""

import json
from importlib.resources import files
from types import SimpleNamespace
from typing import cast

from anthropic.types import Message, TextBlock

from radia.adjudicate import adjudicate, build_prompt, full_spec, parse_verdict

CATEGORIES = [{
    "name": "slurry",
    "description": "Inclusion: animal slurry storage and handling.",
    "exclusion_criteria": "Exclusion Criteria\n\nNot coal slurry or sewage sludge.",
}]
KEEP = '{"about_slurry": true, "exclusion_hit": "none", "verdict_reason": "slurry stores"}'
DROP = '{"about_slurry": false, "exclusion_hit": "homonym", "verdict_reason": "coal slurry"}'


def _reply(text, *, input_tokens=10, output_tokens=5):
    return cast(Message, SimpleNamespace(
        content=[TextBlock(type="text", text=text)],
        usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens),
    ))


def _succeeded(custom_id, text):
    result = SimpleNamespace(type="succeeded", message=_reply(text))
    return SimpleNamespace(custom_id=custom_id, result=result)


def _stub_batch(monkeypatch, rows):
    """Patch the adjudicator's Anthropic client with a finished batch of `rows`."""
    finished = SimpleNamespace(id="batch-test", processing_status="ended")
    batches = SimpleNamespace(
        create=lambda requests: finished,
        retrieve=lambda _id: finished,
        results=lambda _id: iter(rows),
    )
    client = SimpleNamespace(messages=SimpleNamespace(batches=batches))
    monkeypatch.setattr("radia.adjudicate.anthropic.Anthropic", lambda: client)


def _page(url="https://example.gov.uk/p", body="slurry store guidance"):
    return {"url": url, "content_id": "x",
            "meta_data": {"title": "T", "body_text": body}}


def _positive(url="https://example.gov.uk/p", reason="mentions slurry", score=0.3):
    return {"url": url, "content_id": "x", "meta_data": {
        "title": "T", "labels": {"slurry": True},
        "scores": {"slurry": score}, "reasons": {"slurry": reason}}}


def test_full_spec_reattaches_the_exclusion_criteria():
    spec = full_spec(CATEGORIES[0])
    assert "Inclusion" in spec and "Exclusion Criteria" in spec


def test_prompt_carries_the_full_spec_including_exclusions():
    prompt = build_prompt(full_spec(CATEGORIES[0]), "T", "body", "pass-1 note")
    assert "Exclusion Criteria" in prompt and "coal slurry" in prompt


def test_parse_verdict_handles_code_fenced_and_rejects_junk():
    fenced = parse_verdict(f"```json\n{DROP}\n```")
    assert fenced is not None and fenced["about_slurry"] is False
    assert parse_verdict("not json") is None
    assert parse_verdict('{"verdict_reason": "no label"}') is None


def test_drops_a_false_positive(monkeypatch):
    _stub_batch(monkeypatch, [_succeeded("0", DROP)])
    results, stats, audit = adjudicate([_page()], [_positive()], CATEGORIES)
    assert results[0]["meta_data"]["labels"]["slurry"] is False
    assert stats["n_dropped"] == 1 and stats["n_kept"] == 0
    assert audit[0]["pass2_keep"] is False and audit[0]["pass1_score"] == 0.3
    assert results[0]["meta_data"]["reasons"]["slurry"] == "coal slurry"


def test_keeps_a_genuine_positive(monkeypatch):
    _stub_batch(monkeypatch, [_succeeded("0", KEEP)])
    results, stats, _ = adjudicate([_page()], [_positive()], CATEGORIES)
    assert results[0]["meta_data"]["labels"]["slurry"] is True
    assert stats["n_kept"] == 1 and stats["n_dropped"] == 0


def test_keeps_a_page_it_cannot_adjudicate_rather_than_dropping_it(monkeypatch):
    _stub_batch(monkeypatch, [_succeeded("0", "not json at all")])
    results, stats, audit = adjudicate([_page()], [_positive()], CATEGORIES)
    assert results[0]["meta_data"]["labels"]["slurry"] is True
    assert stats["n_unadjudicated"] == 1
    assert results[0]["meta_data"]["reasons"]["slurry"].startswith(
        "[pass-2 adjudication unavailable"
    )
    assert audit[0]["exclusion_hit"] == "unadjudicated"


def test_stamps_truncation_for_an_oversized_body(monkeypatch):
    _stub_batch(monkeypatch, [_succeeded("0", DROP)])
    page = _page(body="slurry " * 100)
    results, _, _ = adjudicate([page], [_positive()], CATEGORIES, max_words=5)
    assert results[0]["meta_data"]["reasons"]["slurry"].startswith(
        "[body truncated to 5 words from 100]"
    )


def test_ignores_pass1_negatives_and_makes_no_call_when_there_are_none():
    negative = {"url": "https://example.gov.uk/n", "content_id": "y",
                "meta_data": {"labels": {"slurry": False}, "reasons": {"slurry": "no"},
                              "scores": {"slurry": 0.0}}}
    # No batch stub: a call would raise, proving the empty-positive path is API-free.
    results, stats, audit = adjudicate([_page()], [negative], CATEGORIES)
    assert stats["n_positives"] == 0 and audit == []
    assert results[0]["meta_data"]["labels"]["slurry"] is False


def test_the_baked_in_category_actually_carries_exclusion_criteria():
    categories = json.loads((files("radia.data") / "categories.json").read_text())
    assert "Exclusion Criteria" in full_spec(categories[0])
