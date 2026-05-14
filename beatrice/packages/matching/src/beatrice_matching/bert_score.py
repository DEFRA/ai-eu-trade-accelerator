import os

BERT_SCORE_MODEL = os.getenv("MODEL_BERT_SCORE", "nlpaueb/legal-bert-base-uncased")
# Number of transformer layers to use for scoring. bert-base models use 9,
# bert-large models use 18. Required for custom HuggingFace models not in
# bert-score's built-in list.
BERT_SCORE_NUM_LAYERS = int(os.getenv("BERT_SCORE_NUM_LAYERS", "9"))


def compute_bert_scores(pairs: list[tuple[str, str]]) -> list[float]:
    """
    Compute BERTScore F1 for a list of (hypothesis, reference) text pairs.

    Uses the model set by MODEL_BERT_SCORE (default: nlpaueb/legal-bert-base-uncased).
    The model is downloaded and cached by HuggingFace on first use.

    Args:
        pairs: List of (guidance_text, law_text) string pairs.

    Returns:
        List of F1 scores (0.0–1.0), one per pair.
    """
    if not pairs:
        return []

    try:
        from bert_score import score as _score
    except ImportError as exc:
        raise ImportError(
            "bert-score is not installed. Install it with: "
            "uv sync --extra bert (from packages/matching)"
        ) from exc

    cands = [p[0] for p in pairs]
    refs = [p[1] for p in pairs]

    _, _, f1 = _score(
        cands, refs,
        model_type=BERT_SCORE_MODEL,
        num_layers=BERT_SCORE_NUM_LAYERS,
        verbose=False,
        use_fast_tokenizer=False,
    )
    return f1.tolist()
