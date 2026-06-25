"""Radia's classifier: word-search routing, batch labelling, and the
no-silent-fallback contract — exercised offline with a stubbed Anthropic client.
"""

import json
from importlib.resources import files
from types import SimpleNamespace
from typing import cast

import anthropic
import pytest
from anthropic.types import Message, TextBlock

from radia.classify import ClassificationIncomplete, build_prompt, classify, parse_response

CATEGORIES = [{"name": "slurry", "description": "slurry handling and storage"}]
LEXICONS = json.loads((files("radia.data") / "lexicon.json").read_text())
SLURRY_REPLY = '{"slurry": true, "slurry_score": 0.9, "slurry_reason": "about slurry"}'


def reply(text, *, input_tokens=10, output_tokens=5):
    """An Anthropic message carrying a single text block and token usage."""
    return cast(Message, SimpleNamespace(
        content=[TextBlock(type="text", text=text)],
        usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens),
    ))


def succeeded(custom_id, text):
    """A succeeded batch-result row for `custom_id`."""
    result = SimpleNamespace(type="succeeded", message=reply(text))
    return SimpleNamespace(custom_id=custom_id, result=result)


def stub_anthropic(monkeypatch, *, batch=(), rescue=None):
    """Patch the Anthropic client with a finished batch and an optional rescue reply.

    `batch` is the succeeded batch results; `rescue` is a zero-arg callable the
    one-off Messages API call delegates to (it raises if a test doesn't set one).
    """
    finished = SimpleNamespace(id="batch-test", processing_status="ended")
    batches = SimpleNamespace(
        create=lambda requests: finished,
        retrieve=lambda _id: finished,
        results=lambda _id: iter(batch),
    )

    def create(**_):
        if rescue is None:
            raise AssertionError("the rescue (Messages API) call was not expected")
        return rescue()

    client = SimpleNamespace(messages=SimpleNamespace(batches=batches, create=create))
    monkeypatch.setattr("radia.classify.anthropic.Anthropic", lambda: client)


@pytest.mark.parametrize("text, label, score", [
    ('{"slurry": true, "slurry_score": 0.8}', True, 0.8),
    ('```json\n{"slurry": false, "slurry_score": 0.0}\n```', False, 0.0),
    ('{"slurry": true, "slurry_score": 0.7, "slurry_reason": "cut off mid sen', True, 0.7),
    ('{"slurry": true}', True, 1.0),
], ids=["clean", "code-fenced", "truncated", "score defaults to label"])
def test_parses_a_model_reply(text, label, score):
    parsed = parse_response(text, CATEGORIES)
    assert parsed is not None
    labels, scores, _ = parsed
    assert labels["slurry"] is label
    assert scores["slurry"] == score


def test_returns_none_for_an_unparseable_reply():
    assert parse_response("not json at all", CATEGORIES) is None


def test_prompt_omits_the_exclusion_criteria_section():
    categories = json.loads((files("radia.data") / "categories.json").read_text())
    prompt = build_prompt("Slurry guidance", "body about slurry", categories)
    assert "Exclusion Criteria" not in prompt


def test_skips_a_page_with_no_lexicon_hit_without_calling_the_llm(build_page):
    page = build_page(body_text="How to renew a passport.")
    results, stats = classify([page], CATEGORIES, LEXICONS)
    assert stats["n_skipped"] == 1
    assert results[0]["meta_data"]["labels"]["slurry"] is False


def test_returns_no_results_for_an_empty_corpus():
    results, _ = classify([], CATEGORIES, LEXICONS)
    assert results == []


def test_labels_a_page_that_hits_the_lexicon(monkeypatch, build_page):
    stub_anthropic(monkeypatch, batch=[succeeded("0", SLURRY_REPLY)])
    results, stats = classify([build_page()], CATEGORIES, LEXICONS)
    assert stats["n_routed"] == 1
    assert results[0]["meta_data"]["labels"]["slurry"] is True
    assert "body_text" not in results[0]["meta_data"]


def test_rescues_a_page_the_batch_dropped(monkeypatch, build_page):
    stub_anthropic(monkeypatch, batch=[], rescue=lambda: reply(SLURRY_REPLY))
    results, stats = classify([build_page()], CATEGORIES, LEXICONS)
    assert stats["n_rescued"] == 1
    assert results[0]["meta_data"]["labels"]["slurry"] is True


def test_notes_the_truncation_when_rescuing_an_oversized_body(monkeypatch, build_page):
    stub_anthropic(monkeypatch, batch=[], rescue=lambda: reply(SLURRY_REPLY))
    page = build_page(body_text="slurry " * 100)
    results, _ = classify([page], CATEGORIES, LEXICONS, max_words=5)
    reason = results[0]["meta_data"]["reasons"]["slurry"]
    assert reason.startswith("[body truncated to 5 words from 100]")


def fails_with_api_error():
    raise anthropic.APIError("down", request=None, body=None)  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("rescue", [
    lambda: reply("not json at all"),
    fails_with_api_error,
], ids=["unparseable reply", "api error"])
def test_raises_rather_than_labelling_a_page_it_cannot_classify(monkeypatch, build_page, rescue):
    stub_anthropic(monkeypatch, batch=[], rescue=rescue)
    with pytest.raises(ClassificationIncomplete):
        classify([build_page()], CATEGORIES, LEXICONS)
