"""
Parse GOV.UK Content API JSON responses into text fragments.

Handles two page formats:
- Guidance (``details.body``): single HTML body segmented by H2 anchor id.
- Guide (``details.parts``): multi-part pages, each part filtered by slug.

Usage::

    import json
    with open("content_api_response.json") as f:
        data = json.load(f)

    fragments = parse_content_api_response(data)
    # → [("section:testing-and-keeping-horses-...", "You must ensure...\\n- item 1\\n..."), ...]
"""

import re
from html.parser import HTMLParser


_SKIP_TAGS = {"nav", "aside", "footer", "header", "script", "style"}


class _GovUkBodyParser(HTMLParser):
    """
    Walks ``details.body`` HTML and groups text by H2 section.

    Produces ``sections``: list of ``(section_id, lines)`` where ``section_id``
    is the ``id`` attribute of the nearest enclosing H2, and ``lines`` is a list
    of text strings (one per paragraph or list item).
    """

    def __init__(self) -> None:
        super().__init__()
        self.sections: list[tuple[str, list[str]]] = []
        self._current_id: str = ""
        self._current_lines: list[str] = []
        self._skip_depth: int = 0        # depth counter while inside a skip tag
        self._tag_stack: list[str] = []
        self._in_li: bool = False
        self._li_buffer: list[str] = []
        self._in_p: bool = False
        self._p_buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._tag_stack.append(tag)
        if tag in _SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return

        attr_dict = dict(attrs)

        if tag == "h2":
            # Save previous section before starting a new one
            self._flush_section()
            self._current_id = attr_dict.get("id") or ""
            self._current_lines = []
        elif tag == "li":
            self._in_li = True
            self._li_buffer = []
        elif tag == "p":
            self._in_p = True
            self._p_buffer = []

    def handle_endtag(self, tag: str) -> None:
        if self._tag_stack and self._tag_stack[-1] == tag:
            self._tag_stack.pop()

        if tag in _SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._skip_depth:
            return

        if tag == "li" and self._in_li:
            self._in_li = False
            text = _normalise(" ".join(self._li_buffer))
            if text:
                self._current_lines.append(text)
            self._li_buffer = []
        elif tag == "p" and self._in_p:
            self._in_p = False
            text = _normalise(" ".join(self._p_buffer))
            if text:
                self._current_lines.append(text)
            self._p_buffer = []

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        text = data.strip()
        if not text:
            return
        if self._in_li:
            self._li_buffer.append(text)
        elif self._in_p:
            self._p_buffer.append(text)

    def _flush_section(self) -> None:
        if self._current_id and self._current_lines:
            self.sections.append((self._current_id, list(self._current_lines)))

    def close(self) -> None:
        super().close()
        self._flush_section()


def _normalise(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _parse_body_html(
    body_html: str,
    section_filter: str | None,
    locator_prefix: str = "section",
) -> list[tuple[str, str]]:
    """Parse a single HTML body string into (locator, text) pairs."""
    parser = _GovUkBodyParser()
    parser.feed(body_html)
    parser.close()

    result: list[tuple[str, str]] = []
    for section_id, lines in parser.sections:
        if section_filter and section_id != section_filter:
            continue
        locator = f"{locator_prefix}:{section_id}"
        text = "\n".join(lines)
        result.append((locator, text))
    return result


def parse_content_api_response(
    content_api_json: dict,
    section_filter: str | None = None,
) -> list[tuple[str, str]]:
    """
    Parse a GOV.UK Content API JSON response into ``(locator, text)`` pairs.

    Handles two Content API formats:

    - **Guidance** (``details.body``): single HTML body segmented by H2.
      ``locator`` is ``section:<h2-id>``.
    - **Guide** (``details.parts``): multi-part pages where each part has a
      ``slug`` and ``body``. ``section_filter`` matches against the part slug.
      ``locator`` is ``part:<slug>:section:<h2-id>`` when H2 sections exist,
      or ``part:<slug>`` when the part body has no H2 headings.

    Args:
        content_api_json: The parsed JSON from the GOV.UK Content API.
        section_filter: For guidance pages, filters by H2 id. For guide pages,
            filters by part slug. All sections/parts returned when ``None``.

    Returns:
        List of ``(locator, text)`` tuples, or ``[]`` if no content found.
    """
    details = content_api_json.get("details", {})

    # ── Single guidance page ──────────────────────────────────────────────────
    if "body" in details:
        return _parse_body_html(details["body"], section_filter)

    # ── Multi-part guide ──────────────────────────────────────────────────────
    if "parts" in details:
        parts: list[dict] = details["parts"]
        if section_filter:
            parts = [p for p in parts if p.get("slug") == section_filter]

        result: list[tuple[str, str]] = []
        for part in parts:
            slug = part.get("slug", "")
            body_html = part.get("body", "")
            if not body_html:
                continue
            # Try to extract H2 sub-sections within the part
            sub_sections = _parse_body_html(body_html, section_filter=None)
            if sub_sections:
                for locator, text in sub_sections:
                    result.append((f"part:{slug}:{locator}", text))
            else:
                # No H2 headings — treat the whole part as one fragment
                parser = _GovUkBodyParser()
                parser.feed(body_html)
                parser.close()
                # Collect all lines regardless of section
                all_lines = [line for _, lines in parser.sections for line in lines]
                if all_lines:
                    result.append((f"part:{slug}", "\n".join(all_lines)))
        return result

    return []
