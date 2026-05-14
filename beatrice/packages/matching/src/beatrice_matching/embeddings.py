import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from beatrice_llm import BeatriceLLMClient


def embed_texts(texts: list[str], llm_client: "BeatriceLLMClient") -> list[list[float]]:
    """Embed a list of texts in a single batched call to Ollama."""
    if not texts:
        return []
    return llm_client.embed_texts(texts)


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0.0 or mag_b == 0.0:
        return 0.0
    return dot / (mag_a * mag_b)


def find_candidates(
    guidance_embedding: list[float],
    law_embeddings: list[tuple[str, list[float]]],
    top_k: int = 5,
    threshold: float = 0.4,
) -> list[tuple[str, float]]:
    """
    Return the top_k law proposition ids whose embedding exceeds the threshold,
    sorted by descending similarity score.

    Args:
        guidance_embedding: Embedding of the guidance proposition text.
        law_embeddings: List of (law_proposition_id, embedding) pairs.
        top_k: Maximum number of candidates to return.
        threshold: Minimum cosine similarity to be considered a candidate.

    Returns:
        List of (law_proposition_id, score) sorted by descending score.
    """
    scored = [
        (law_id, cosine_similarity(guidance_embedding, law_emb))
        for law_id, law_emb in law_embeddings
    ]
    filtered = [(law_id, score) for law_id, score in scored if score >= threshold]
    filtered.sort(key=lambda x: x[1], reverse=True)
    return filtered[:top_k]
