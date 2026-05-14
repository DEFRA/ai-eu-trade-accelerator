#!/usr/bin/env python3
"""
Extract guidance propositions from a plain-text document (e.g. a .txt export of a Word doc).

Splits on section headings, extracts propositions per section using the guidance
extractor, and prints results (optionally saving to JSON).

Usage:
    uv run scripts/extract_guidance_from_text.py <path-to-txt> [options]

Examples:
    uv run scripts/extract_guidance_from_text.py "SPS agreement (core content).docx.txt" \\
        --source-url "https://www.gov.uk/guidance/sps-agreement" \\
        --topic "SPS agreement" \\
        --output sps_guidance_propositions.json

    # Without LLM (heuristic only):
    uv run scripts/extract_guidance_from_text.py "SPS agreement (core content).docx.txt" \\
        --source-url "internal://sps-agreement" --topic "SPS" --no-llm
"""

import argparse
import json
import re
import sys
from pathlib import Path

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "packages" / "guidance" / "src"))
sys.path.insert(0, str(_root / "packages" / "llm" / "src"))

from beatrice_guidance import extract_propositions

# Matches lines that look like section headings:
# - Short (under 80 chars), no trailing period, not a bullet
_HEADING_RE = re.compile(r"^([A-Z][^\n]{2,79})$", re.MULTILINE)


def split_by_headings(text: str) -> list[tuple[str, str]]:
    """Split text into (heading, body) chunks on capitalised short lines."""
    lines = text.splitlines()
    chunks: list[tuple[str, str]] = []
    current_heading = "Introduction"
    current_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        # Heading: short, capitalised, no trailing period, not a bullet point
        if (
            stripped
            and len(stripped) < 80
            and stripped[0].isupper()
            and not stripped.endswith(".")
            and not stripped.startswith("*")
            and not stripped.startswith("-")
            and not stripped.startswith("(")
            and len(stripped.split()) <= 10
        ):
            if current_lines:
                body = "\n".join(current_lines).strip()
                if body:
                    chunks.append((current_heading, body))
            current_heading = stripped
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines:
        body = "\n".join(current_lines).strip()
        if body:
            chunks.append((current_heading, body))

    return chunks if chunks else [("document:full", text.strip())]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract guidance propositions from a plain-text document."
    )
    parser.add_argument("filename", help="Path to the .txt document")
    parser.add_argument(
        "--source-url",
        default=None,
        help="URL to use as the source (e.g. the future GOV.UK URL)",
    )
    parser.add_argument(
        "--topic",
        default="GOV.UK guidance",
        help="Topic label used in extraction (default: 'GOV.UK guidance')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum total propositions to extract (default: 200)",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Use heuristic extraction only (no LLM calls)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Save propositions as JSON to this file",
    )
    args = parser.parse_args()

    path = Path(args.filename)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    source_url = args.source_url or f"file://{path.resolve()}"

    llm_client = None
    if not args.no_llm:
        try:
            from beatrice_llm import BeatriceLLMClient
            llm_client = BeatriceLLMClient()
            print(f"LLM enabled: {llm_client.settings.base_url} model={llm_client.settings.guidance_extract_model}")
        except Exception as e:
            print(f"Warning: could not initialise LLM client: {e}", file=sys.stderr)
            print("Falling back to heuristic extraction.", file=sys.stderr)

    text = path.read_text(encoding="utf-8", errors="replace")
    sections = split_by_headings(text)
    print(f"Found {len(sections)} section(s) in {path.name}\n")

    # Build fragments list: (section_locator, section_text)
    fragments = [(heading, body) for heading, body in sections]

    propositions = extract_propositions(
        fragments=fragments,
        topic=args.topic,
        source_url=source_url,
        limit=args.limit,
        llm_client=llm_client,
    )

    if not propositions:
        print("No propositions extracted.")
        return

    print(f"Extracted {len(propositions)} proposition(s):\n")
    for i, prop in enumerate(propositions, start=1):
        print(f"[{i}] {prop.section_locator}")
        print(f"     Text:      {prop.proposition_text}")
        print(f"     Subject:   {prop.legal_subject}")
        print(f"     Action:    {prop.action}")
        if prop.conditions:
            print(f"     Conditions: {', '.join(prop.conditions)}")
        print()

    if args.output:
        Path(args.output).write_text(
            json.dumps([p.model_dump() for p in propositions], indent=2)
        )
        print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
