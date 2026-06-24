"""Stage 2 body fetch: extracting body text from a GOV.UK Content API response."""

import pytest

from fetch_body import _extract_body_text


@pytest.mark.parametrize("content, expected", [
    ({"details": {"body": "<p>Hello world</p>"}}, "Hello world"),
    ({"details": {"parts": [{"body": "<p>One</p>"}, {"body": "<p>Two</p>"}]}}, "One\n\nTwo"),
    ({"details": {}}, ""),
], ids=["single body", "multipart guide", "no body"])
def test_extracts_body_text_from_a_page(content, expected):
    assert _extract_body_text(content) == expected
