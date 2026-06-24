from pydantic import BaseModel


class GuidanceProposition(BaseModel):
    id: str
    section_locator: str
    proposition_text: str
    legal_subject: str
    action: str = ""
    conditions: list[str] = []
    required_documents: list[str] = []
    source_url: str
    extraction_method: str  # "heuristic" | "llm" | "susan"
    source_paragraphs: list[str] = []  # verbatim page paragraphs (one per <p>/<li>) this proposition is drawn from
    # ── group-rerank matching tags (from Susan; empty when untyped) ──
    regulatory_kind: str = ""  # statutory_obligation, permit_requirement, grant_condition,
                               # code_of_practice, procedural_step, factual_statement, definition
    topic: str = ""            # closed-vocab topic id shared with the law side
