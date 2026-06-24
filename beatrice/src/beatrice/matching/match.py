from typing import TYPE_CHECKING

from beatrice.domain import Proposition
from beatrice.guidance import GuidanceProposition

from .embeddings import embed_texts, find_candidates
from .group_rerank import group_rerank_match
from .models import MatchSet
from .prompts import LABEL_PRIORITY

if TYPE_CHECKING:
    from beatrice.llm import BeatriceLLMClient


def match_propositions(
    guidance_propositions: list[GuidanceProposition],
    law_propositions: list[Proposition],
    llm_client: "BeatriceLLMClient",
    top_k: int = 15,
    similarity_threshold: float = 0.65,
    score_gap: float | None = 0.04,
    render_conf_sidebar: bool = True,
) -> list[MatchSet]:
    """
    Match each guidance proposition against the law proposition corpus using the
    group-rerank strategy.

    Three-stage process:
    1. Embed all texts in two batched calls (guidance corpus + law corpus).
    2. For each guidance proposition, retrieve a survivor set of law candidates
       (adaptive threshold + score_gap, capped at top_k).
    3. One LLM call per guidance proposition labels every survivor at once. A
       proposition with no survivor above the threshold costs no LLM call.

    Args:
        guidance_propositions: Propositions extracted from GOV.UK guidance.
        law_propositions: Propositions extracted from legislation.
        llm_client: BeatriceLLMClient used for both embeddings and classification.
        top_k: Hard cap on survivors per guidance proposition.
        similarity_threshold: Minimum cosine similarity to be a candidate.
        score_gap: Adaptive-size window below the top score (None to disable).
        render_conf_sidebar: Include Judit's per-candidate quality sidebar when
            the law records carry the extraction-quality fields.

    Returns:
        One MatchSet per guidance proposition. ``matches`` is empty when no law
        proposition clears the threshold.
    """
    if not guidance_propositions or not law_propositions:
        return [
            MatchSet(guidance_proposition_id=gp.id, matches=[], best_match=None)
            for gp in guidance_propositions
        ]

    # Stage 1: embed both corpora in two batched calls
    guidance_texts = [gp.proposition_text for gp in guidance_propositions]
    law_texts = [lp.proposition_text for lp in law_propositions]

    guidance_embeddings = embed_texts(guidance_texts, llm_client)
    law_embeddings = embed_texts(law_texts, llm_client)

    law_index: list[tuple[str, list[float]]] = [
        (lp.id, emb) for lp, emb in zip(law_propositions, law_embeddings, strict=False)
    ]
    law_by_id = {lp.id: lp for lp in law_propositions}

    # Stage 2 + 3: retrieve survivors and group-rerank each guidance proposition
    results: list[MatchSet] = []
    for guidance_prop, g_emb in zip(
        guidance_propositions, guidance_embeddings, strict=False
    ):
        candidates = find_candidates(
            guidance_embedding=g_emb,
            law_embeddings=law_index,
            top_k=top_k,
            threshold=similarity_threshold,
            score_gap=score_gap,
        )
        survivors = [(law_by_id[law_id], score) for law_id, score in candidates]
        matches = group_rerank_match(
            guidance_prop,
            survivors,
            llm_client,
            render_conf_sidebar=render_conf_sidebar,
        )
        results.append(
            MatchSet(
                guidance_proposition_id=guidance_prop.id,
                matches=matches,
                best_match=_best_match(matches),
            )
        )

    return results


_CONFIDENCE_WEIGHT = {"high": 0, "medium": 1, "low": 2}


def _best_match(matches: list) -> object | None:
    """The most substantively useful match: best label, then confidence, then
    similarity. Group-rerank already sorts matches by label priority, so this is
    the head of the list, but we make the tie-break explicit."""
    if not matches:
        return None
    return min(
        matches,
        key=lambda m: (
            LABEL_PRIORITY.get(m.relationship, 99),
            _CONFIDENCE_WEIGHT.get(m.confidence, 99),
            -m.similarity_score,
        ),
    )
