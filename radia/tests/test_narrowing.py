"""Word-search routing: a page reaches the LLM only when its body mentions a
lexicon term (in any inflection); otherwise the category is excluded.
"""

import pytest

from radia.narrowing import select_categories

CATEGORIES = [{"name": "slurry", "description": "slurry handling and storage"}]
LEXICON = {"slurry": ["Slurry", "Cattle manure"]}


@pytest.mark.parametrize("body, term", [
    ("Guidance on slurry storage and lagoon freeboard.", "Slurry"),
    ("The stored slurries must be covered.", "Slurry"),
    ("Rules covering cattle manures spread on farmland.", "Cattle manure"),
], ids=["unigram", "inflected unigram", "inflected bigram"])
def test_selects_a_category_when_its_lexicon_term_appears(body, term):
    selected, meta = select_categories(body, CATEGORIES, LEXICON)
    assert [c["name"] for c in selected] == ["slurry"]
    assert term in meta["hits_per_cat"]["slurry"]


def test_selects_nothing_when_no_lexicon_term_appears():
    body = "Guidance on passport applications and overseas travel insurance."
    selected, _ = select_categories(body, CATEGORIES, LEXICON)
    assert selected == []
