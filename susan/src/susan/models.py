"""Proposition output schema."""

from pydantic import BaseModel, Field


class GuidanceProposition(BaseModel):
    """One atomic proposition lifted from a GOV.UK guidance page.

    Metadata fields (subject_area, instrument, actor) carry the contextual
    grounding a downstream consumer needs to read the proposition in
    isolation. Empty values are legitimate — the prompt forbids guessing
    when the page doesn't supply the fact.
    """

    proposition_text: str
    subject_area: str = ""
    instrument: str = ""
    actor: str = ""
    source_paragraphs: list[str] = Field(default_factory=list)
    regulatory_kind: str = ""
    derives_from: list[str] = Field(default_factory=list)
