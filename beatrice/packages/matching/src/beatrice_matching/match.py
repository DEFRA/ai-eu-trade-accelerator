from typing import TYPE_CHECKING

from beatrice_domain import Proposition
from beatrice_guidance import GuidanceProposition

from .classify import classify_match
from .embeddings import embed_texts, find_candidates
from .models import MatchSet

if TYPE_CHECKING:
    from beatrice_llm import BeatriceLLMClient


def match_propositions(
    guidance_propositions: list[GuidanceProposition],
    law_propositions: list[Proposition],
    llm_client: "BeatriceLLMClient",
    top_k: int = 5,
    similarity_threshold: float = 0.4,
    compute_bert_score: bool = False,
) -> list[MatchSet]:
    """
    Match each guidance proposition against the law proposition corpus.

    Three-stage process:
    1. Embed all texts in two batched calls (guidance corpus + law corpus).
    2. For each guidance proposition, find top_k law candidates above the
       similarity threshold.
    3. Optionally batch-compute BERTScore F1 for all candidate pairs, then
       LLM-classify each pair.

    Args:
        guidance_propositions: Propositions extracted from GOV.UK guidance.
        law_propositions: Propositions extracted from legislation.
        llm_client: BeatriceLLMClient used for both embeddings and classification.
        top_k: Maximum law candidates to classify per guidance proposition.
        similarity_threshold: Minimum cosine similarity to be a candidate.
        compute_bert_score: If True, compute BERTScore F1 for each candidate
            pair using the model set by MODEL_BERT_SCORE.

    Returns:
        One MatchSet per guidance proposition. ``matches`` is empty when no
        law proposition clears the threshold.
    """
    if not guidance_propositions or not law_propositions:
        return [
            MatchSet(
                guidance_proposition_id=gp.id,
                matches=[],
                best_match=None,
            )
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

    # Stage 2: find candidates for all guidance propositions
    all_candidates: list[tuple[GuidanceProposition, list[tuple[str, float]]]] = []
    for guidance_prop, g_emb in zip(guidance_propositions, guidance_embeddings, strict=False):
        candidates = find_candidates(
            guidance_embedding=g_emb,
            law_embeddings=law_index,
            top_k=top_k,
            threshold=similarity_threshold,
        )
        all_candidates.append((guidance_prop, candidates))

    # Stage 3a: optionally batch-compute BERTScore for all candidate pairs
    bert_score_map: dict[tuple[str, str], float] = {}
    if compute_bert_score:
        from .bert_score import compute_bert_scores

        pair_keys = [
            (gp.id, law_id)
            for gp, candidates in all_candidates
            for law_id, _ in candidates
        ]
        pairs = [
            (gp.proposition_text, law_by_id[law_id].proposition_text)
            for gp, candidates in all_candidates
            for law_id, _ in candidates
        ]
        bert_scores = compute_bert_scores(pairs)
        bert_score_map = dict(zip(pair_keys, bert_scores, strict=False))

    # Stage 3b: classify each (guidance, candidate) pair
    results: list[MatchSet] = []
    for guidance_prop, candidates in all_candidates:
        matches = [
            classify_match(
                guidance_prop=guidance_prop,
                law_prop=law_by_id[law_id],
                similarity_score=score,
                llm_client=llm_client,
                bert_score_f1=bert_score_map.get((guidance_prop.id, law_id), 0.0),
            )
            for law_id, score in candidates
        ]

        best = _best_match(matches)
        results.append(
            MatchSet(
                guidance_proposition_id=guidance_prop.id,
                matches=matches,
                best_match=best,
            )
        )

    return results


_RELATIONSHIP_PRIORITY = {
    "confirmed": 0,
    "guidance omits detail": 1,
    "guidance contains additional detail": 2,
    "outdated": 3,
    "contradicts": 4,
    "does not match": 5,
}

_CONFIDENCE_WEIGHT = {"high": 0, "medium": 1, "low": 2}


def _best_match(matches: list) -> object | None:
    if not matches:
        return None
    return min(
        matches,
        key=lambda m: (
            _RELATIONSHIP_PRIORITY.get(m.relationship, 99),
            _CONFIDENCE_WEIGHT.get(m.confidence, 99),
            -m.similarity_score,
        ),
    )
