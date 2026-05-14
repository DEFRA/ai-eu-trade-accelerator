#!/usr/bin/env python3
"""
Extract law propositions from a plain-text document (e.g. a .txt export of a Word doc).

Splits on Article headings, extracts propositions per article using the pipeline
LLM extractor, and outputs JSON compatible with the /embed-law endpoint.

Usage:
    uv run scripts/extract_law_propositions_from_text.py <path-to-txt> [options]

Examples:
    uv run scripts/extract_law_propositions_from_text.py "SPS agreement (core content).docx.txt" \\
        --citation "SPS Agreement" \\
        --topic "Sanitary and Phytosanitary Measures" \\
        --output sps_propositions.json

    # Without LLM (heuristic only):
    uv run scripts/extract_law_propositions_from_text.py "SPS agreement (core content).docx.txt" \\
        --citation "SPS Agreement" --topic "SPS" --no-llm --output sps_propositions.json
"""

import argparse
import json
import re
import sys
import uuid
from pathlib import Path

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "packages" / "pipeline" / "src"))
sys.path.insert(0, str(_root / "packages" / "domain" / "src"))
sys.path.insert(0, str(_root / "packages" / "llm" / "src"))

from beatrice_domain import Cluster, SourceRecord, Topic
from beatrice_pipeline.extract import extract_propositions

_ARTICLE_RE = re.compile(
    r"^(Article\s+\d+[\w\-\.]*|ARTICLE\s+\d+[\w\-\.]*)",
    re.MULTILINE | re.IGNORECASE,
)


def split_by_article(text: str) -> list[tuple[str, str]]:
    """Return list of (article_heading, article_text) tuples."""
    matches = list(_ARTICLE_RE.finditer(text))
    if not matches:
        return [("document:full", text.strip())]

    chunks = []
    for i, match in enumerate(matches):
        heading = match.group(0).strip()
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        chunks.append((heading, body))
    return chunks


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract law propositions from a plain-text document."
    )
    parser.add_argument("filename", help="Path to the .txt document")
    parser.add_argument(
        "--citation",
        default="Unknown",
        help="Legal citation for the document (e.g. 'SPS Agreement')",
    )
    parser.add_argument(
        "--jurisdiction",
        default="UK",
        help="Jurisdiction label (default: UK)",
    )
    parser.add_argument(
        "--topic",
        default="Law",
        help="Topic label used in extraction (default: 'Law')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum propositions to extract per article (default: 10)",
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

    llm_client = None
    if not args.no_llm:
        try:
            from beatrice_llm import BeatriceLLMClient
            llm_client = BeatriceLLMClient()
            print(f"LLM enabled: {llm_client.settings.base_url} model={llm_client.settings.local_extract_model}")
        except Exception as e:
            print(f"Warning: could not initialise LLM client: {e}", file=sys.stderr)
            print("Falling back to heuristic extraction.", file=sys.stderr)

    text = path.read_text(encoding="utf-8", errors="replace")
    articles = split_by_article(text)
    print(f"Found {len(articles)} article(s) in {path.name}\n")

    topic_id = f"topic-{uuid.uuid4().hex[:8]}"
    cluster_id = f"cluster-{uuid.uuid4().hex[:8]}"

    topic = Topic(id=topic_id, name=args.topic, description=args.topic)
    cluster = Cluster(id=cluster_id, topic_id=topic_id, name=args.citation)

    all_propositions = []

    for heading, body in articles:
        source_id = f"src-{uuid.uuid4().hex[:12]}"
        source = SourceRecord(
            id=source_id,
            title=f"{args.citation} — {heading}",
            jurisdiction=args.jurisdiction,
            citation=args.citation,
            kind="legislation",
            authoritative_text=body,
            authoritative_locator=heading,
        )

        props = extract_propositions(
            source=source,
            topic=topic,
            cluster=cluster,
            llm_client=llm_client,
            limit=args.limit,
        )

        print(f"  {heading}: {len(props)} proposition(s)")
        for p in props:
            print(f"    - {p.proposition_text[:80]}...")
        all_propositions.extend(props)

    print(f"\nTotal: {len(all_propositions)} proposition(s)")

    result = [p.model_dump() for p in all_propositions]

    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2))
        print(f"Saved to {args.output}")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
