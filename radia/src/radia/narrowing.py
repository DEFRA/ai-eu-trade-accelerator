"""Word-search routing: decide which pages reach the LLM.

Radia routes a page to the classifier **only** if its body hits the curated
inclusion lexicon, Porter-stemmed. Pages with no hit are excluded without an LLM
call — the absence of an inclusion hit *is* the exclusion mechanism.

Trimmed to the one strategy this package bakes in; the tf-idf and ``all``
strategies are out of scope here. There are NO silent fallbacks: the caller is
responsible for recording an explicit "excluded by word-search" verdict on the
pages this module routes away.

Matching is **unigram and bigram only**: a lexicon term is checked as a single
stem or as a two-stem bigram against the body. A lexicon term that stems to three
or more tokens (e.g. ``"Other inorganics (from animal processing)"``) cannot
match and is effectively inert — keep curated lexicon terms to one or two
significant words.
"""
from __future__ import annotations

import re

import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

# Self-bootstrap the one NLTK corpus we use (the tokenizer is a plain regex, so
# punkt is not needed). First run downloads it; subsequent runs find it cached.
try:
    nltk.data.find("corpora/stopwords")
except LookupError:
    nltk.download("stopwords", quiet=True)

_STEMMER = PorterStemmer()
_STOPWORDS = set(stopwords.words("english"))
_TOKEN_RE = re.compile(r"[a-z][a-z\-]+")


def tokenize_and_stem(text: str) -> list[str]:
    """Lowercase, regex-tokenise, drop English stopwords, Porter-stem."""
    raw = _TOKEN_RE.findall(text.lower())
    return [_STEMMER.stem(t) for t in raw if t not in _STOPWORDS]


def stem_term(term: str) -> str | tuple[str, ...] | None:
    """Return a stemmed unigram, a stemmed n-gram tuple, or None if empty."""
    parts = _TOKEN_RE.findall(term.lower())
    parts = [_STEMMER.stem(p) for p in parts if p not in _STOPWORDS]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return tuple(parts)


def select_categories(
    body_text: str,
    categories: list[dict],
    lexicons: dict[str, list[str]],
) -> tuple[list[dict], dict]:
    """Return ``(selected_categories, selection_meta)`` for word-search routing.

    A category is selected iff any of its stemmed lexicon terms (unigram or
    bigram) appears in the stemmed body. ``selection_meta`` records, per
    selected category, which original lexicon terms hit — so a run can always be
    explained. Only presence matters, so the body is reduced to a set of its
    unigrams and bigrams.
    """
    body_stems = tokenize_and_stem(body_text)
    unigrams = set(body_stems)
    bigrams = set(zip(body_stems, body_stems[1:], strict=False))

    hits_per_cat: dict[str, list[str]] = {}
    for cat in categories:
        name = cat["name"]
        hits: list[str] = []
        for term in lexicons.get(name, []):
            st = stem_term(term)
            if isinstance(st, str):
                if st in unigrams:
                    hits.append(term)
            elif st is not None and len(st) == 2 and st in bigrams:
                # Only bigrams route; 3+ token terms can never match (see module docstring).
                hits.append(term)
        if hits:
            hits_per_cat[name] = hits

    selected = [c for c in categories if c["name"] in hits_per_cat]
    return selected, {"strategy": "wordsearch_binary", "hits_per_cat": hits_per_cat}
