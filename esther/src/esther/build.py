"""Shape upstream pipeline outputs into the content-audit frontend data files.

Pure transform: every function here takes already-loaded JSON and returns plain
data — no file or network IO (the CLI owns that). This keeps the join logic
unit-testable and the data lineage explicit.

Identity model (pipeline-native keys, one file per entity):
  - pages            keyed by ``content_id`` (gov.uk content id)
  - legislation      keyed by ``source_record_id`` (``lex-…``)
  - law propositions keyed by ``id`` (Judit ``prop:…``)
  - guidance props   keyed by ``id`` (``g-…``, recovered from Beatrice's input)
  - matches          keyed by a derived stable ``id`` (``m-…``) for feedback

No silent fallbacks: anything that can't be derived is recorded in ``warnings``
and either left null or dropped with an explicit message.
"""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict

# Beatrice's six-way taxonomy, normalised onto the frontend's status values.
# CONFLICT is the only rename (the prompt sometimes emits the singular).
RELATIONSHIP_NORMALISE = {
    "GROUNDED": "GROUNDED",
    "GUIDANCE_INCOMPLETE": "GUIDANCE_INCOMPLETE",
    "GUIDANCE_MISSING": "GUIDANCE_MISSING",
    "GUIDANCE_BROADER": "GUIDANCE_BROADER",
    "UNGROUNDED": "UNGROUNDED",
    "CONFLICT": "CONFLICTS",
    "CONFLICTS": "CONFLICTS",
}

STATUS_ORDER = [
    "UNGROUNDED",
    "GUIDANCE_BROADER",
    "GUIDANCE_INCOMPLETE",
    "GROUNDED",
    "CONFLICTS",
    "GUIDANCE_MISSING",
]


def _sha8(*parts: str) -> str:
    return hashlib.sha1("\0".join(parts).encode()).hexdigest()[:8]


def _radia_score(meta: dict, category: str) -> float | None:
    """Radia's per-category relevance score. New runs use ``scores``; older
    runs used ``cosine_score`` — accept either, prefer the new key."""
    for key in ("scores", "cosine_score"):
        block = meta.get(key)
        if isinstance(block, dict) and category in block:
            return block[category]
    return None


def build_pages(beatrice_content: list[dict], category: str, warnings: list[str]) -> list[dict]:
    """Audited pages, sorted by URL. Keyed by content_id."""
    pages = []
    for item in sorted(beatrice_content, key=lambda p: p["url"]):
        content_id = item.get("content_id")
        if not content_id:
            warnings.append(f"page has no content_id, skipped: {item.get('url')}")
            continue
        md = item.get("meta_data", {}) or {}
        pages.append({
            "content_id": content_id,
            "category": category,
            "url": item["url"],
            "title": md.get("title", ""),
            "description": md.get("description", ""),
            "document_type": md.get("document_type"),
        })
    return pages


def build_guidance_propositions(
    beatrice_output: list[dict],
    guidance_input: list[dict],
    url_to_content_id: dict[str, str],
    warnings: list[str],
) -> tuple[list[dict], list[str | None]]:
    """Flatten Beatrice's guidance propositions, recovering their ``g-…`` ids.

    Beatrice's output is a reordered subset of its input and carries no id, so
    we recover the id by ``(page_url, proposition_text)`` against the input
    bundle. Returns the rows plus a per-output-row id list (aligned to
    ``beatrice_output``) for the matches builder to reuse.
    """
    id_by_key = {
        (g["page_url"], g["proposition_text"]): g["id"]
        for g in guidance_input
        if g.get("id")
    }
    rows: list[dict] = []
    row_ids: list[str | None] = []
    seen_ids: set[str] = set()
    minted = 0
    for entry in beatrice_output:
        url = entry["guidance_source_url"]
        text = entry["guidance_proposition_text"]
        content_id = url_to_content_id.get(url)
        if content_id is None:
            warnings.append(f"guidance proposition source URL not in audited pages: {url}")
            row_ids.append(None)
            continue
        gp_id = id_by_key.get((url, text))
        if gp_id is None:
            gp_id = f"g-{_sha8(url, text)}"
            minted += 1
        row_ids.append(gp_id)
        if gp_id not in seen_ids:
            seen_ids.add(gp_id)
            rows.append({
                "id": gp_id,
                "content_id": content_id,
                "proposition_text": text,
            })
    if minted:
        warnings.append(
            f"{minted} guidance propositions had no id in the Beatrice input bundle; "
            "minted a deterministic g-<hash> id from (url, text)."
        )
    return rows, row_ids


def build_legislation(legislation_seed: list[dict], category: str) -> list[dict]:
    """Re-key the legislation seed by source_record_id (drops the integer id)."""
    return [
        {
            "source_record_id": law["source_record_id"],
            "category": category,
            "name": law["name"],
            "url": law.get("url"),
        }
        for law in legislation_seed
        if law.get("source_record_id")
    ]


def build_legislation_propositions(
    law_input: list[dict],
    legacy_law_props: list[dict],
    known_source_record_ids: set[str],
    warnings: list[str],
) -> list[dict]:
    """Law propositions keyed by Judit ``prop:`` id, linked by source_record_id.

    ``graph_node`` in the Beatrice law input is a rename of Judit's
    ``source_record_id``. short_name/label/fragment_locator are not carried in
    that input bundle, so they are recovered from the legacy frontend file by
    judit id; new propositions get null + a backfill warning.
    """
    legacy_by_id = {lp["judit_id"]: lp for lp in legacy_law_props if lp.get("judit_id")}
    rows: list[dict] = []
    missing_legislation: set[str] = set()
    missing_legacy = 0
    for lp in law_input:
        source_record_id = lp.get("graph_node") or lp.get("source_record_id")
        if source_record_id not in known_source_record_ids:
            missing_legislation.add(str(source_record_id))
            continue
        legacy = legacy_by_id.get(lp["id"])
        if legacy is None:
            missing_legacy += 1
        rows.append({
            "id": lp["id"],
            "source_record_id": source_record_id,
            "proposition_text": lp["proposition_text"],
            "short_name": (legacy or {}).get("short_name"),
            "label": (legacy or {}).get("label"),
            "fragment_locator": (legacy or {}).get("fragment_locator"),
        })
    if missing_legislation:
        for g in sorted(missing_legislation):
            warnings.append(
                f"law proposition source_record_id {g} has no row in legislation.json "
                "— add it to the seed or drop the proposition"
            )
    if missing_legacy:
        warnings.append(
            f"{missing_legacy} law propositions are new (no legacy judit id match); "
            "short_name/label/fragment_locator are null — backfill from Judit."
        )
    return rows


def build_proposition_matches(
    beatrice_output: list[dict],
    guidance_row_ids: list[str | None],
    law_prop_ids: set[str],
    warnings: list[str],
) -> list[dict]:
    """Top match per guidance proposition + synthesised GUIDANCE_MISSING rows.

    Beatrice sorts ``matches`` best-first, so ``matches[0]`` is the top match.
    Every law proposition with no guidance match gets a GUIDANCE_MISSING row
    (``guidance_proposition_id: null``) so the frontend can surface coverage gaps.
    """
    matches: list[dict] = []
    matched_law_ids: set[str] = set()
    unknown_law_ids: set[str] = set()
    unmapped: set[str] = set()
    zero_match = 0

    for entry, gp_id in zip(beatrice_output, guidance_row_ids, strict=True):
        if gp_id is None:
            continue
        top = next(iter(entry.get("matches", [])), None)
        if top is None:
            zero_match += 1
            continue
        law_id = top["law_id"]
        if law_id not in law_prop_ids:
            unknown_law_ids.add(law_id)
            continue
        relationship = RELATIONSHIP_NORMALISE.get(top.get("relationship"))
        if relationship is None:
            unmapped.add(str(top.get("relationship")))
            continue
        matched_law_ids.add(law_id)
        matches.append({
            "id": f"m-{_sha8(gp_id, law_id)}",
            "guidance_proposition_id": gp_id,
            "law_proposition_id": law_id,
            "relationship": relationship,
            "confidence": top.get("confidence"),
            "cosine_score": round(top["cosine_score"], 4) if "cosine_score" in top else None,
            "bert_score_f1": round(top["bert_score_f1"], 4) if "bert_score_f1" in top else None,
            "explanation": top.get("explanation"),
        })

    if zero_match:
        warnings.append(f"{zero_match} guidance propositions had zero matches")
    if unknown_law_ids:
        for lid in sorted(unknown_law_ids):
            warnings.append(f"top match references unknown law id (not in law input): {lid}")
    if unmapped:
        for r in sorted(unmapped):
            warnings.append(f"unknown relationship value in Beatrice output: {r!r}")

    for law_id in sorted(law_prop_ids - matched_law_ids):
        matches.append({
            "id": f"m-{_sha8('MISSING', law_id)}",
            "guidance_proposition_id": None,
            "law_proposition_id": law_id,
            "relationship": "GUIDANCE_MISSING",
            "confidence": None,
            "cosine_score": None,
            "bert_score_f1": None,
            "explanation": None,
        })
    return matches


def build_page_relevance(
    pages: list[dict], radia_output: list[dict], category: str, warnings: list[str]
) -> list[dict]:
    """Per-page relevance from Radia, joined by content_id (falls back to url)."""
    score_by_content_id: dict[str, float | None] = {}
    score_by_url: dict[str, float | None] = {}
    for row in radia_output:
        score = _radia_score(row.get("meta_data", {}) or {}, category)
        if row.get("content_id"):
            score_by_content_id[row["content_id"]] = score
        if row.get("url"):
            score_by_url[row["url"]] = score
    out = []
    missing = 0
    for p in pages:
        score = score_by_content_id.get(p["content_id"])
        if score is None:
            score = score_by_url.get(p["url"])
        if score is None:
            missing += 1
        out.append({
            "category": category,
            "content_id": p["content_id"],
            "relevance_score": score,
        })
    if missing:
        warnings.append(f"{missing} audited pages had no Radia relevance score (left null)")
    return out


def build_reading_age(pages: list[dict], reading_age_by_url: dict[str, dict]) -> list[dict]:
    out = []
    for p in pages:
        ra = reading_age_by_url.get(p["url"], {})
        out.append({
            "content_id": p["content_id"],
            "url": p["url"],
            "word_count": ra.get("word_count"),
            "reading_age": ra.get("reading_age"),
        })
    return out


def build_page_analytics(pages: list[dict], beatrice_content: list[dict]) -> list[dict]:
    content_by_url = {c["url"]: c for c in beatrice_content}
    out = []
    for p in pages:
        md = (content_by_url.get(p["url"], {}) or {}).get("meta_data", {}) or {}
        updated_at = md.get("updated_at")
        out.append({
            "content_id": p["content_id"],
            "last_updated_date": updated_at[:10] if updated_at else None,
            "view_count_period": md.get("view_count"),
            "period": "last_12_months",
        })
    return out


def build_subject_summary(
    category: str,
    matches: list[dict],
    pages: list[dict],
    legislation: list[dict],
    total_pages_audited: int,
) -> dict:
    counter = Counter(m["relationship"] for m in matches)
    return {
        "category": category,
        "total_pages_audited": total_pages_audited,
        "laws_found": len(legislation),
        "pages_relevant": len(pages),
        "proposition_status_counts": {k: counter.get(k, 0) for k in STATUS_ORDER},
    }


def build(
    *,
    beatrice_output: list[dict],
    guidance_input: list[dict],
    law_input: list[dict],
    beatrice_content: list[dict],
    radia_output: list[dict],
    legislation_seed: list[dict],
    legacy_law_props: list[dict],
    category: dict,
    reading_age_by_url: dict[str, dict],
) -> tuple[dict[str, object], list[str]]:
    """Run the full transform. Returns ``(files, warnings)`` where ``files`` maps
    output filename -> JSON-serialisable data. ``category`` is
    ``{"id": slug, "title": ..., "description": ...}``."""
    warnings: list[str] = []
    slug = category["id"]

    pages = build_pages(beatrice_content, slug, warnings)
    url_to_content_id = {p["url"]: p["content_id"] for p in pages}

    guidance_props, guidance_row_ids = build_guidance_propositions(
        beatrice_output, guidance_input, url_to_content_id, warnings
    )

    legislation = build_legislation(legislation_seed, slug)
    known_srids = {law["source_record_id"] for law in legislation}
    legislation_props = build_legislation_propositions(
        law_input, legacy_law_props, known_srids, warnings
    )
    law_prop_ids = {lp["id"] for lp in legislation_props}

    matches = build_proposition_matches(
        beatrice_output, guidance_row_ids, law_prop_ids, warnings
    )

    files: dict[str, object] = {
        "categories.json": [category],
        "legislation.json": legislation,
        "legislation-propositions.json": legislation_props,
        "pages.json": pages,
        "guidance-propositions.json": guidance_props,
        "proposition-matches.json": matches,
        "page-relevance.json": build_page_relevance(pages, radia_output, slug, warnings),
        "pages-reading-age.json": build_reading_age(pages, reading_age_by_url),
        "page-analytics.json": build_page_analytics(pages, beatrice_content),
        "subject-summary.json": [
            build_subject_summary(slug, matches, pages, legislation, len(radia_output))
        ],
    }
    return files, warnings
