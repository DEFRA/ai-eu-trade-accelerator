"""
Susan's extraction entry-point.

One page in, a list of GuidanceProposition out. No fallback path. Any
API error, schema mismatch, or truncated response raises — Susan does not
silently substitute a degraded result.

Uses Anthropic's structured tool-use API with a forced tool choice. The
model returns structured input rather than serialised JSON, so malformed-
JSON failures are not possible. Truncation at the token budget surfaces
cleanly as `stop_reason == "max_tokens"` rather than as a parse error.
"""

from __future__ import annotations

from .adapters import render_structured_body
from .fetch import FetchedPage
from .models import GuidanceProposition
from .prompt import render


DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_MAX_TOKENS = 8192
DEFAULT_TIMEOUT_SECONDS = 120


_PROPOSITION_SCHEMA = {
    "type": "object",
    "properties": {
        "proposition_text": {"type": "string"},
        "subject_area": {"type": "string"},
        "instrument": {"type": "string"},
        "actor": {"type": "string"},
        "source_paragraphs": {
            "type": "array",
            "items": {"type": "string"},
        },
        "regulatory_kind": {"type": "string"},
        "derives_from": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["proposition_text"],
}

_EMIT_TOOL = {
    "name": "emit_propositions",
    "description": "Emit every atomic proposition extracted from the page.",
    "input_schema": {
        "type": "object",
        "properties": {
            "propositions": {
                "type": "array",
                "items": _PROPOSITION_SCHEMA,
            },
        },
        "required": ["propositions"],
    },
}


class ExtractionError(Exception):
    """Anything that went wrong during extraction. Loud, not silent."""


def extract_propositions(
    page: FetchedPage,
    *,
    topic: str = "guidance",
    model: str = DEFAULT_MODEL,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> list[GuidanceProposition]:
    """Extract atomic propositions from one fetched gov.uk page.

    Raises ExtractionError on truncation, missing tool call, or schema
    mismatch. Raises anthropic exceptions on API failure.
    """
    import anthropic  # imported lazily so importing susan is cheap

    body = render_structured_body(page)
    prompt = render(body=body, source_url=page.url, topic=topic)

    client = anthropic.Anthropic(timeout=timeout)
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        tools=[_EMIT_TOOL],
        tool_choice={"type": "tool", "name": "emit_propositions"},
        messages=[{"role": "user", "content": prompt}],
    )

    if response.stop_reason == "max_tokens":
        raise ExtractionError(
            f"response truncated at {max_tokens} tokens — raise --max-tokens or split the page"
        )

    tool_block = next(
        (b for b in response.content if getattr(b, "type", None) == "tool_use"),
        None,
    )
    if tool_block is None:
        raise ExtractionError(
            f"model did not emit the tool call. stop_reason={response.stop_reason!r}"
        )

    raw_dicts = tool_block.input.get("propositions")  # type: ignore[union-attr]
    if not isinstance(raw_dicts, list):
        raise ExtractionError(
            f"tool call missing 'propositions' array. Got keys: {list(tool_block.input)!r}"  # type: ignore[union-attr]
        )

    return [GuidanceProposition(**d) for d in raw_dicts]
