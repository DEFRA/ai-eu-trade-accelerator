from types import SimpleNamespace

from beatrice.matching.prompts import (
    build_group_rerank_prompt,
    build_summarise_prompt,
    law_citation,
    parse_group_rerank_json,
)


def _gp(regulatory_kind="statutory_obligation", topic="slurry_storage"):
    return SimpleNamespace(
        source_url="https://www.gov.uk/x",
        section_locator="section:1",
        proposition_text="A farmer must store slurry safely.",
        regulatory_kind=regulatory_kind,
        topic=topic,
    )


def _lp(law_id="prop:abc", clause_function="floor", topic="slurry_storage",
        article_reference=None, fragment_locator=None, **quality):
    return SimpleNamespace(
        id=law_id,
        jurisdiction="UK",
        proposition_text="Slurry stores must be impermeable.",
        clause_function=clause_function,
        topic=topic,
        article_reference=article_reference,
        fragment_locator=fragment_locator,
        model_confidence=quality.get("model_confidence"),
        completeness_status=quality.get("completeness_status"),
        fallback_policy=quality.get("fallback_policy"),
        fallback_used=quality.get("fallback_used"),
    )


def test_law_citation_falls_back_to_fragment_locator():
    assert law_citation(_lp(article_reference="reg 4")) == "reg 4"
    assert law_citation(_lp(fragment_locator="regulation 1(1)")) == "regulation 1(1)"
    assert law_citation(_lp()) == "—"
    assert law_citation({"fragment_locator": "reg 9"}) == "reg 9"


def test_group_rerank_prompt_renders_tags_and_candidates():
    p = build_group_rerank_prompt(_gp(), [(_lp(fragment_locator="regulation 1(1)"), 0.81)])
    assert "[regulatory_kind: statutory_obligation]" in p
    assert "[topic: slurry_storage]" in p
    assert "[clause_function: floor]" in p
    assert "(regulation 1(1))" in p
    assert "Slurry stores must be impermeable." in p
    assert "[prop:abc]" in p


def test_group_rerank_prompt_degrades_to_unknown_tags():
    p = build_group_rerank_prompt(
        _gp(regulatory_kind="", topic=""), [(_lp(clause_function="", topic=""), 0.7)]
    )
    assert "[regulatory_kind: unknown]" in p
    assert "[clause_function: unknown]" in p


def test_group_rerank_prompt_renders_confidence_sidebar():
    law = _lp(model_confidence="high", completeness_status="context_dependent", fallback_used=True)
    p = build_group_rerank_prompt(_gp(), [(law, 0.8)], render_conf_sidebar=True)
    assert "--- sidebar ---" in p
    assert "model_confidence: high" in p
    assert "completeness_status: context_dependent" in p
    assert "fallback_used: true" in p
    # Without the flag the sidebar is omitted entirely.
    assert "--- sidebar ---" not in build_group_rerank_prompt(_gp(), [(law, 0.8)])


def test_parse_group_rerank_json_normalises_and_handles_prose():
    raw = (
        'Here is the result:\n{"best_anchor_id": "prop:abc", "anchor_rationale": "fits", '
        '"matches": [{"law_id": "prop:abc", "relationship": "GROUNDED", "confidence": "high", '
        '"explanation": "ok", "correctness_score": 0.9}, '
        '{"law_id": "prop:zzz", "relationship": "nonsense", "confidence": "low", '
        '"explanation": "no", "correctness_score": 0}]}'
    )
    out = parse_group_rerank_json(raw)
    assert out["best_anchor_id"] == "prop:abc"
    assert out["matches"][0]["relationship"] == "GROUNDED"
    assert out["matches"][1]["relationship"] == "UNGROUNDED"  # unknown label normalised


def test_summarise_prompt_renders_matches():
    matches = [{"relationship": "GROUNDED", "explanation": "matches",
                "law_proposition": {"proposition_text": "L", "fragment_locator": "reg 4"}}]
    p = build_summarise_prompt(_gp(), matches)
    assert "Relationship: GROUNDED" in p and "Citation: reg 4" in p
    assert "No relevant law matches" in build_summarise_prompt(_gp(), [])
