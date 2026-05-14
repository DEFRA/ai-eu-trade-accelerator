from pydantic import BaseModel


class GuidanceProposition(BaseModel):
    id: str
    section_locator: str
    proposition_text: str
    legal_subject: str
    action: str
    conditions: list[str]
    required_documents: list[str]
    source_url: str
    extraction_method: str  # "heuristic" | "llm"
    source_paragraphs: list[str] = []  # verbatim page paragraphs (one per <p>/<li>) this proposition is drawn from
