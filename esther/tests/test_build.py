"""Unit tests for the Esther transform — pure, no IO."""

from esther.build import build

CATEGORY = {"id": "slurry", "title": "Slurry", "description": "Slurry management."}

# One audited page, two guidance propositions, two law propositions.
BEATRICE_CONTENT = [
    {
        "url": "https://www.gov.uk/a",
        "content_id": "cid-a",
        "meta_data": {
            "title": "Page A",
            "description": "About A",
            "document_type": "guidance",
            "updated_at": "2025-01-02T10:00:00Z",
            "view_count": 42,
        },
    }
]

GUIDANCE_INPUT = [
    {"id": "g-aaa-01", "page_url": "https://www.gov.uk/a", "proposition_text": "Do X."},
    {"id": "g-aaa-02", "page_url": "https://www.gov.uk/a", "proposition_text": "Do Y."},
]

# Beatrice output: reordered, and the second prop has no match.
BEATRICE_OUTPUT = [
    {
        "guidance_proposition_text": "Do Y.",
        "guidance_source_url": "https://www.gov.uk/a",
        "matches": [
            {
                "law_id": "prop:law1",
                "relationship": "CONFLICT",  # normalises to CONFLICTS
                "confidence": "high",
                "cosine_score": 0.811234,
                "bert_score_f1": 0.92,
                "explanation": "conflicts with law1",
                "correctness_score": 0.0,
            }
        ],
    },
    {
        "guidance_proposition_text": "Do X.",
        "guidance_source_url": "https://www.gov.uk/a",
        "matches": [],  # zero matches
    },
]

LAW_INPUT = [
    {"id": "prop:law1", "proposition_text": "Law one.", "graph_node": "lex-1"},
    {"id": "prop:law2", "proposition_text": "Law two.", "graph_node": "lex-1"},
]

LEGISLATION_SEED = [
    {"id": 1, "category_id": 1, "name": "Act 1", "url": "https://leg/1", "source_record_id": "lex-1"}
]

LEGACY_LAW_PROPS = [
    {"judit_id": "prop:law1", "short_name": "L1", "label": "Law One", "fragment_locator": "s.1"}
]

RADIA_OUTPUT = [
    {"url": "https://www.gov.uk/a", "content_id": "cid-a", "meta_data": {"scores": {"slurry": 0.73}}},
    {"url": "https://www.gov.uk/other", "content_id": "cid-o", "meta_data": {"scores": {"slurry": 0.1}}},
]

READING_AGE = {"https://www.gov.uk/a": {"word_count": 120, "reading_age": 14}}


def _build():
    return build(
        beatrice_output=BEATRICE_OUTPUT,
        guidance_input=GUIDANCE_INPUT,
        law_input=LAW_INPUT,
        beatrice_content=BEATRICE_CONTENT,
        radia_output=RADIA_OUTPUT,
        legislation_seed=LEGISLATION_SEED,
        legacy_law_props=LEGACY_LAW_PROPS,
        category=CATEGORY,
        reading_age_by_url=READING_AGE,
    )


def test_pages_keyed_by_content_id_with_category_slug():
    files, _ = _build()
    pages = files["pages.json"]
    assert pages == [
        {
            "content_id": "cid-a",
            "category": "slurry",
            "url": "https://www.gov.uk/a",
            "title": "Page A",
            "description": "About A",
            "document_type": "guidance",
        }
    ]


def test_guidance_ids_recovered_from_input_by_text():
    files, _ = _build()
    ids = {g["id"] for g in files["guidance-propositions.json"]}
    # Only "Do Y." produced a match row; both props are emitted as guidance props.
    assert ids == {"g-aaa-01", "g-aaa-02"}
    assert all(g["content_id"] == "cid-a" for g in files["guidance-propositions.json"])


def test_top_match_normalises_conflict_and_links_native_ids():
    files, _ = _build()
    matches = files["proposition-matches.json"]
    top = next(m for m in matches if m["guidance_proposition_id"] is not None)
    assert top["guidance_proposition_id"] == "g-aaa-02"
    assert top["law_proposition_id"] == "prop:law1"
    assert top["relationship"] == "CONFLICTS"
    assert top["cosine_score"] == 0.8112  # rounded to 4dp
    assert top["id"].startswith("m-")


def test_unmatched_law_prop_becomes_guidance_missing():
    files, _ = _build()
    missing = [m for m in files["proposition-matches.json"] if m["relationship"] == "GUIDANCE_MISSING"]
    assert [m["law_proposition_id"] for m in missing] == ["prop:law2"]
    assert missing[0]["guidance_proposition_id"] is None


def test_legislation_rekeyed_by_source_record_id_no_integer_id():
    files, _ = _build()
    assert files["legislation.json"] == [
        {"source_record_id": "lex-1", "category": "slurry", "name": "Act 1", "url": "https://leg/1"}
    ]
    lp = files["legislation-propositions.json"]
    assert {p["id"] for p in lp} == {"prop:law1", "prop:law2"}
    law1 = next(p for p in lp if p["id"] == "prop:law1")
    assert law1["source_record_id"] == "lex-1"
    assert law1["short_name"] == "L1"  # carried from legacy by judit id


def test_relevance_from_radia_scores_by_content_id():
    files, _ = _build()
    assert files["page-relevance.json"] == [
        {"category": "slurry", "content_id": "cid-a", "relevance_score": 0.73}
    ]


def test_subject_summary_total_pages_is_radia_corpus_size():
    files, _ = _build()
    summary = files["subject-summary.json"][0]
    assert summary["total_pages_audited"] == 2  # len(RADIA_OUTPUT)
    assert summary["pages_relevant"] == 1
    assert summary["laws_found"] == 1
    assert "relevance_threshold" not in summary
    assert summary["proposition_status_counts"]["CONFLICTS"] == 1
    assert summary["proposition_status_counts"]["GUIDANCE_MISSING"] == 1


def test_no_correctness_or_aggregation_files_emitted():
    files, _ = _build()
    assert "page-aggregations.json" not in files
    assert "legislation-aggregations.json" not in files
    # correctness must not leak into any emitted record
    for data in files.values():
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            assert "correctness" not in str(row).lower() or "correctness_score" not in row


def test_analytics_and_reading_age_keyed_by_content_id():
    files, _ = _build()
    assert files["page-analytics.json"] == [
        {
            "content_id": "cid-a",
            "last_updated_date": "2025-01-02",
            "view_count_period": 42,
            "period": "last_12_months",
        }
    ]
    assert files["pages-reading-age.json"] == [
        {"content_id": "cid-a", "url": "https://www.gov.uk/a", "word_count": 120, "reading_age": 14}
    ]
