"""
Mary — the page contract.

Both stages pass around the same dict shape, and stage 2's output *is* Radia's
input. These TypedDicts document that contract in one place so the keys aren't
re-spelled by hand in every script. (Defined in Mary only — Radia is untouched
and still just reads plain JSON.)
"""

from typing import NotRequired, TypedDict


class MetaData(TypedDict):
    title: str
    description: str
    updated_at: str
    document_type: str
    view_count: int
    # Added by stage 2 (fetch_body). None when the fetch failed; "" when the
    # page genuinely has no body. Absent on stage 1's discovery-only output.
    body_text: NotRequired[str | None]


class PageItem(TypedDict):
    url: str
    content_id: str
    meta_data: MetaData
