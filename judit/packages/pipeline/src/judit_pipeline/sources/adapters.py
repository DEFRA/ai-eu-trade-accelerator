import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.request import Request, urlopen
from xml.etree import ElementTree


@dataclass(frozen=True)
class SourceFetchRequest:
    authority: str
    authority_source_id: str
    version_id: str
    expected_content_hash: str | None
    raw_source: dict[str, Any]


@dataclass(frozen=True)
class SourcePayload:
    title: str
    jurisdiction: str
    citation: str
    kind: str
    authoritative_text: str
    authoritative_locator: str
    provenance: str
    as_of_date: Any | None
    retrieved_at: Any | None
    source_url: str | None
    review_status: Any
    metadata: dict[str, Any]


@dataclass(frozen=True)
class AdapterFetchResult:
    payload: SourcePayload
    trace_metadata: dict[str, Any]


class AuthorityAdapter(Protocol):
    authority_name: str

    def fetch(self, request: SourceFetchRequest) -> AdapterFetchResult: ...


class CaseFileAuthorityAdapter:
    """
    Adapter for inline case-file sources.

    This is intentionally simple so that a future legislation.gov.uk adapter can
    implement the same `fetch` contract and be swapped in through the registry.
    """

    authority_name = "case_file"

    def fetch(self, request: SourceFetchRequest) -> AdapterFetchResult:
        raw = request.raw_source
        locator = raw.get("authoritative_locator") or raw.get("fragment_locator") or "document:full"
        metadata = raw.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        return AdapterFetchResult(
            payload=SourcePayload(
                title=raw["title"],
                jurisdiction=raw["jurisdiction"],
                citation=raw["citation"],
                kind=raw["kind"],
                authoritative_text=str(raw.get("text", "")),
                authoritative_locator=locator,
                provenance=str(raw.get("provenance", "demo.case_file")),
                as_of_date=raw.get("as_of_date"),
                retrieved_at=raw.get("retrieved_at"),
                source_url=raw.get("source_url"),
                review_status=raw.get("review_status", "proposed"),
                metadata=metadata,
            ),
            trace_metadata={"source_kind": "inline_case_file"},
        )


def _local_name(tag_name: str) -> str:
    if "}" in tag_name:
        return tag_name.split("}", maxsplit=1)[1]
    return tag_name


def _first_text(root: ElementTree.Element, candidates: set[str]) -> str | None:
    for node in root.iter():
        if _local_name(node.tag) in candidates:
            text = "".join(node.itertext()).strip()
            if text:
                return " ".join(text.split())
    return None


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.split()).strip()


def _looks_like_noise_text(value: str) -> bool:
    lowered = value.lower()
    if len(lowered) < 4:
        return True
    noise_markers = (
        "changes to legislation",
        "no changes have been applied",
        "introductory text",
        "opening options",
        "more resources",
        "correction slips",
        "impact assessments",
        "explanatory memorandum",
        "explanatory notes",
        "policy notes",
        "show timeline",
        "geographical extent",
        "commencement information",
        "extent information",
        "annotations",
        "table of contents",
        "print options",
    )
    if any(marker in lowered for marker in noise_markers):
        return True
    if lowered.startswith("status:"):
        return True
    return False


def _extract_locator_from_node(node: ElementTree.Element) -> str | None:
    for key in (
        "id",
        "eId",
        "eid",
        "EId",
        "URI",
        "uri",
        "DocumentURI",
        "documentURI",
        "href",
        "Ref",
    ):
        value = node.attrib.get(key)
        if value:
            return f"xml:{value.strip()}"
    return None


def _infer_locator_from_text(value: str) -> str | None:
    patterns = (
        (r"\b(article)\s+(\d+[a-z]?)\b", "article"),
        (r"\b(section)\s+(\d+[a-z]?)\b", "section"),
        (r"\b(regulation)\s+(\d+[a-z]?)\b", "regulation"),
        (r"\b(paragraph)\s+(\d+[a-z]?)\b", "paragraph"),
        (r"\b(schedule)\s+(\d+[a-z]?)\b", "schedule"),
    )
    for pattern, locator_name in patterns:
        match = re.search(pattern, value, flags=re.IGNORECASE)
        if match:
            return f"{locator_name}:{match.group(2).lower()}"
    return None


def _is_operative_locator(locator: str | None) -> bool:
    if not locator:
        return False
    lowered = locator.lower()
    if any(marker in lowered for marker in ("introduction", "preamble", "recital", "note")):
        return False
    return any(
        marker in lowered
        for marker in (
            "article",
            "section",
            "regulation",
            "paragraph",
            "schedule",
            "chapter",
            "part",
        )
    )


def _looks_operative_legal_text(value: str, locator: str | None = None) -> bool:
    if _is_operative_locator(locator):
        return True
    lowered = value.lower()
    if lowered.startswith("whereas "):
        return False
    if any(trigger in lowered for trigger in ("must", "shall", "may", "must not", "shall not")):
        return True
    heading_markers = ("article ", "section ", "regulation ", "paragraph ", "schedule ")
    if any(lowered.startswith(marker) for marker in heading_markers):
        return True
    return False


def _extract_text_chunks(root: ElementTree.Element) -> list[tuple[str, str]]:
    text_nodes = {
        "P",
        "P1",
        "P2",
        "P3",
        "P4",
        "P5",
        "P6",
        "Text",
    }
    non_operative_ancestors = {
        "ExplanatoryNotes",
        "Commentaries",
        "Footnotes",
        "Annotations",
        "Contents",
        "TableOfContents",
        "Metadata",
    }

    chunks: list[tuple[str, str]] = []

    def walk(
        node: ElementTree.Element, ancestor_names: tuple[str, ...], inherited_locator: str | None
    ) -> None:
        node_name = _local_name(node.tag)
        current_locator = _extract_locator_from_node(node) or inherited_locator
        next_ancestors = (*ancestor_names, node_name)
        has_non_operative_ancestor = any(
            ancestor in non_operative_ancestors for ancestor in next_ancestors
        )

        if node_name in text_nodes and not has_non_operative_ancestor:
            value = _normalize_whitespace("".join(node.itertext()))
            if value and not _looks_like_noise_text(value):
                locator = current_locator or _infer_locator_from_text(value) or "document:full"
                chunks.append((locator, value))

        for child in list(node):
            walk(child, next_ancestors, current_locator)

    walk(root, tuple(), None)
    return chunks


_RE_ROOT_PROVISION_ID = re.compile(
    r"^(regulation|article|rule|schedule|annex)-([a-z0-9]+)$",
    re.IGNORECASE,
)
_RE_SCHEDULE_PARAGRAPH_ID = re.compile(
    r"^schedule-([a-z0-9]+)-paragraph-([a-z0-9]+)$",
    re.IGNORECASE,
)
_RE_SCHEDULE_NUMERIC_PARAGRAPH_ID = re.compile(
    r"^schedule-([a-z0-9]+)-([a-z0-9]+)$",
    re.IGNORECASE,
)
_RE_TARGET_CITATION = re.compile(
    r"\b(Regulation|Article|Rule|Schedule|Annex)\s+([0-9]+[A-Za-z]?)\b"
)


def _classify_fragment_kind(locator: str, text: str) -> str:
    lowered_locator = locator.lower()
    lowered_text = text.lower()
    if re.match(r"^schedule:[^:]+$", lowered_locator):
        return "schedule"
    amendment_markers = (
        "amend",
        "amended",
        "substitute",
        "insert",
        "omit",
        "revoke",
        "replace",
        "for ",
    )
    if any(marker in lowered_text for marker in amendment_markers):
        return "amendment_provision"
    operative_markers = ("must", "shall", "may", "must not", "shall not")
    if any(marker in lowered_text for marker in operative_markers):
        return "operative_provision"
    return "unknown"


def _possible_target_citation(text: str) -> str | None:
    match = _RE_TARGET_CITATION.search(text)
    if not match:
        return None
    return f"{match.group(1).lower()}:{match.group(2).lower()}"


def _build_legislation_structural_fragments(
    *,
    root: ElementTree.Element,
    source_url: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    document_uri = root.attrib.get("DocumentURI", "").strip().rstrip("/")
    if source_url.endswith("/data.xml"):
        source_base_url = source_url[: -len("/data.xml")]
    else:
        source_base_url = source_url.rstrip("/")

    order_index = 0
    structural_rows: list[dict[str, Any]] = []
    known_locators: set[str] = set()
    provision_links: list[str] = []

    for node in root.iter():
        node_id = (
            node.attrib.get("id")
            or node.attrib.get("eId")
            or node.attrib.get("eid")
            or ""
        ).strip()
        if not node_id:
            continue

        node_text = _normalize_whitespace("".join(node.itertext()))
        if not node_text or _looks_like_noise_text(node_text):
            continue

        locator = ""
        parent_locator: str | None = None
        source_path = ""

        match_root = _RE_ROOT_PROVISION_ID.match(node_id)
        if match_root:
            unit_kind = match_root.group(1).lower()
            unit_number = match_root.group(2).lower()
            locator = f"{unit_kind}:{unit_number}"
            source_path = f"{unit_kind}/{unit_number}"
        else:
            match_para = _RE_SCHEDULE_PARAGRAPH_ID.match(node_id)
            if match_para:
                schedule_number = match_para.group(1).lower()
                para_number = match_para.group(2).lower()
                locator = f"schedule:{schedule_number}:paragraph:{para_number}"
                parent_locator = f"schedule:{schedule_number}"
                source_path = f"schedule/{schedule_number}/paragraph/{para_number}"
            else:
                match_numeric_para = _RE_SCHEDULE_NUMERIC_PARAGRAPH_ID.match(node_id)
                if match_numeric_para:
                    schedule_number = match_numeric_para.group(1).lower()
                    para_number = match_numeric_para.group(2).lower()
                    locator = f"schedule:{schedule_number}:paragraph:{para_number}"
                    parent_locator = f"schedule:{schedule_number}"
                    source_path = f"schedule/{schedule_number}/paragraph/{para_number}"
        if not locator:
            continue
        if locator in known_locators:
            continue

        known_locators.add(locator)
        legislation_uri = f"{document_uri}/{source_path}" if document_uri else None
        legislation_url = f"{source_base_url}/{source_path}"
        fragment_kind = _classify_fragment_kind(locator, node_text)
        target_citation = _possible_target_citation(node_text)
        structural_rows.append(
            {
                "locator": locator,
                "text": node_text,
                "parent_locator": parent_locator,
                "order_index": order_index,
                "metadata": {
                    "source_path": source_path,
                    "legislation_uri": legislation_uri,
                    "fragment_kind": fragment_kind,
                    "possible_target_citation": target_citation,
                },
            }
        )
        provision_links.append(f"{legislation_url}/data.xml")
        order_index += 1

    return structural_rows, provision_links


def _jurisdiction_from_series(series: str) -> str:
    series_lower = series.lower()
    if series_lower in {"asp", "ssi"}:
        return "Scotland"
    if series_lower in {"anaw", "wsi", "asc"}:
        return "Wales"
    if series_lower in {"nia", "nisr"}:
        return "Northern Ireland"
    return "UK"


class LegislationGovUkAuthorityAdapter:
    authority_name = "legislation_gov_uk"

    def __init__(
        self,
        *,
        fetch_xml: Callable[[str], tuple[str, dict[str, Any]]] | None = None,
    ) -> None:
        self.fetch_xml = fetch_xml or self._fetch_xml_from_network

    def fetch(self, request: SourceFetchRequest) -> AdapterFetchResult:
        raw = request.raw_source
        source_url = str(
            raw.get("source_url")
            or f"https://www.legislation.gov.uk/{request.authority_source_id.strip('/')}/data.xml"
        )
        xml_text, http_meta = self.fetch_xml(source_url)
        root = ElementTree.fromstring(xml_text)
        parts = [part for part in request.authority_source_id.split("/") if part]
        if len(parts) < 3:
            raise ValueError(
                "legislation_gov_uk authority_source_id must look like '<series>/<year>/<number>'."
            )

        series = parts[0]
        citation_fallback = f"{series.upper()} {parts[1]}/{parts[2]}"
        title = _first_text(root, {"title", "Title", "DocumentTitle"}) or citation_fallback
        long_title = _first_text(root, {"LongTitle"})
        structural_fragments, provision_links = _build_legislation_structural_fragments(
            root=root,
            source_url=source_url,
        )
        selected_chunks = (
            [(row["locator"], row["text"]) for row in structural_fragments]
            if structural_fragments
            else []
        )
        extracted_chunks = _extract_text_chunks(root)
        locator_scoped_chunks = [
            (locator, value)
            for locator, value in extracted_chunks
            if _is_operative_locator(locator)
        ]
        operative_chunks = [
            (locator, value)
            for locator, value in extracted_chunks
            if _looks_operative_legal_text(value, locator)
        ]
        if not selected_chunks:
            selected_chunks = locator_scoped_chunks or operative_chunks or extracted_chunks
        authoritative_text = "\n".join(value for _, value in selected_chunks).strip()
        fragment_locators = [locator for locator, _ in selected_chunks]
        if not authoritative_text:
            fallback = _normalize_whitespace("".join(root.itertext()))
            if not fallback:
                raise ValueError(
                    "No authoritative text extracted from legislation.gov.uk XML payload."
                )
            authoritative_text = fallback
            fragment_locators = ["document:full"]
        resolved_locator = (
            str(raw.get("authoritative_locator") or raw.get("fragment_locator")).strip()
            if raw.get("authoritative_locator") or raw.get("fragment_locator")
            else ""
        )
        locator = resolved_locator or fragment_locators[0] or "document:full"
        metadata = raw.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        metadata = {
            **metadata,
            "authority_source_id": request.authority_source_id,
            "source_url": source_url,
            "series": series,
            "http_meta": http_meta,
            "fragment_locators": fragment_locators,
            "structural_fragments": structural_fragments,
            "provision_data_xml_links": provision_links,
            "extracted_chunk_count": len(selected_chunks),
            "structural_fragment_count": len(structural_fragments),
            "locator_scoped_chunk_count": len(locator_scoped_chunks),
            "operative_chunk_count": len(operative_chunks),
        }
        if long_title:
            metadata["long_title"] = long_title

        payload = SourcePayload(
            title=str(raw.get("title") or title),
            jurisdiction=str(raw.get("jurisdiction") or _jurisdiction_from_series(series)),
            citation=str(raw.get("citation") or citation_fallback),
            kind=str(raw.get("kind") or series.lower()),
            authoritative_text=authoritative_text,
            authoritative_locator=locator,
            provenance=str(raw.get("provenance", "authority.legislation_gov_uk")),
            as_of_date=raw.get("as_of_date"),
            retrieved_at=raw.get("retrieved_at") or datetime.now(UTC),
            source_url=source_url,
            review_status=raw.get("review_status", "proposed"),
            metadata=metadata,
        )
        return AdapterFetchResult(
            payload=payload,
            trace_metadata={
                "source_kind": "authority_fetch",
                "source_url": source_url,
                "http_status": http_meta.get("status"),
                "response_bytes": http_meta.get("response_bytes"),
                "parser": "legislation_gov_uk_data_xml_v1",
            },
        )

    def _fetch_xml_from_network(self, source_url: str) -> tuple[str, dict[str, Any]]:
        request = Request(source_url, headers={"Accept": "application/xml, text/xml;q=0.9"})
        with urlopen(request, timeout=20) as response:
            body = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            return body.decode(charset, errors="replace"), {
                "status": response.status,
                "content_type": response.headers.get("Content-Type", ""),
                "response_bytes": len(body),
            }
