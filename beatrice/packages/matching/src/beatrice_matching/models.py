from pydantic import BaseModel

RELATIONSHIP_TYPES = (
    "confirmed",   # guidance accurately reflects the law
    "outdated",    # same intent but law has since changed
    "guidance omits detail",              # guidance only covers part of what the law requires
    "guidance contains additional detail", # guidance adds requirements beyond the law
    "contradicts", # guidance says the opposite of the law
    "does not match",  # no corresponding law proposition found
)


class GuidanceMatch(BaseModel):
    guidance_proposition_id: str
    law_proposition_id: str
    relationship: str       # one of RELATIONSHIP_TYPES
    confidence: str         # "high" | "medium" | "low"
    explanation: str
    similarity_score: float
    correctness_score: float = 0.0  # 0–1: 0=does not match/contradicts, 1=confirmed
    bert_score_f1: float = 0.0      # BERTScore F1 (0–1) using MODEL_BERT_SCORE
    classify_cached: bool = False   # True if result was served from the classify cache


class MatchSet(BaseModel):
    guidance_proposition_id: str
    matches: list[GuidanceMatch]
    best_match: GuidanceMatch | None
