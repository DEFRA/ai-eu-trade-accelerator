"""Deterministic proposition tier / legal-effect classification (no ML)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from .enums import LegalEffectType, PropositionTier
from .proposition_notes import (
    assign_proposition_extraction_debug,
    resolve_extraction_meta_for_proposition,
)
from .territory_normalization import (
    extract_territories_from_text,
    normalize_territory_name,
    split_territory_list,
)

_PROVISION_TYPE_TO_EFFECT: dict[str, LegalEffectType] = {
    "definition": LegalEffectType.DEFINITION,
    "exception": LegalEffectType.DEROGATION,
    "transitional": LegalEffectType.COMMENCEMENT,
    "cross_reference": LegalEffectType.CROSS_REFERENCE,
    "core": LegalEffectType.UNKNOWN,
}

_EFFECT_TO_TIER: dict[LegalEffectType, PropositionTier] = {
    LegalEffectType.CITATION: PropositionTier.INSTRUMENT_METADATA,
    LegalEffectType.COMMENCEMENT: PropositionTier.INSTRUMENT_METADATA,
    LegalEffectType.EXTENT: PropositionTier.INSTRUMENT_METADATA,
    LegalEffectType.APPLICATION_SCOPE: PropositionTier.SCOPE_RULE,
    LegalEffectType.DEFINITION: PropositionTier.DEFINITIONAL_RULE,
    LegalEffectType.CROSS_REFERENCE: PropositionTier.RELATIONSHIP_REFERENCE,
    LegalEffectType.OBLIGATION: PropositionTier.SUBSTANTIVE_RULE,
    LegalEffectType.PROHIBITION: PropositionTier.SUBSTANTIVE_RULE,
    LegalEffectType.PERMISSION: PropositionTier.SUBSTANTIVE_RULE,
    LegalEffectType.POWER: PropositionTier.SUBSTANTIVE_RULE,
    LegalEffectType.RECORDKEEPING: PropositionTier.SUBSTANTIVE_RULE,
    LegalEffectType.NOTIFICATION: PropositionTier.PROCEDURAL_RULE,
    LegalEffectType.CERTIFICATION: PropositionTier.PROCEDURAL_RULE,
    LegalEffectType.INSPECTION: PropositionTier.PROCEDURAL_RULE,
    LegalEffectType.ENFORCEMENT: PropositionTier.PROCEDURAL_RULE,
    LegalEffectType.APPEAL: PropositionTier.PROCEDURAL_RULE,
    LegalEffectType.DEROGATION: PropositionTier.SUBSTANTIVE_RULE,
    LegalEffectType.UNKNOWN: PropositionTier.UNKNOWN,
}

_COMPLIANCE_RELEVANT_EFFECTS = frozenset(
    {
        LegalEffectType.OBLIGATION,
        LegalEffectType.PROHIBITION,
        LegalEffectType.RECORDKEEPING,
        LegalEffectType.NOTIFICATION,
        LegalEffectType.CERTIFICATION,
        LegalEffectType.ENFORCEMENT,
    }
)

_COMPARISON_ANCHOR_EFFECTS = frozenset(
    {
        LegalEffectType.APPLICATION_SCOPE,
        LegalEffectType.DEFINITION,
        LegalEffectType.OBLIGATION,
        LegalEffectType.PROHIBITION,
        LegalEffectType.PERMISSION,
        LegalEffectType.POWER,
        LegalEffectType.RECORDKEEPING,
        LegalEffectType.NOTIFICATION,
        LegalEffectType.CERTIFICATION,
        LegalEffectType.INSPECTION,
        LegalEffectType.ENFORCEMENT,
        LegalEffectType.APPEAL,
        LegalEffectType.DEROGATION,
        LegalEffectType.CROSS_REFERENCE,
    }
)

@dataclass(frozen=True)
class ClassificationInputs:
    proposition_text: str
    legal_subject: str = ""
    action: str = ""
    label: str = ""
    short_name: str = ""
    object_text: str = ""
    conditions: tuple[str, ...] = ()
    affected_subjects: tuple[str, ...] = ()
    fragment_locator: str = ""
    source_locator: str = ""
    # Legacy LLM tags only — not used for tier/effect/compliance decisions.
    categories: tuple[str, ...] = ()
    provision_type: str = ""


@dataclass(frozen=True)
class ClassificationResult:
    proposition_tier: PropositionTier
    legal_effect_type: LegalEffectType
    territorial_application: list[str]
    extent: list[str]
    is_compliance_relevant: bool
    is_comparison_anchor: bool
    affected_subjects: list[str]


def _combined_text(inputs: ClassificationInputs) -> str:
    parts = [
        inputs.proposition_text,
        inputs.legal_subject,
        inputs.action,
        inputs.label,
        inputs.short_name,
        inputs.object_text,
        *inputs.conditions,
        *inputs.affected_subjects,
        inputs.fragment_locator,
        inputs.source_locator,
    ]
    return " ".join(p.strip() for p in parts if p and str(p).strip()).lower()


def _effect_from_provision_type(provision_type: str) -> LegalEffectType | None:
    pt = provision_type.strip().lower()
    if not pt:
        return None
    mapped = _PROVISION_TYPE_TO_EFFECT.get(pt)
    if mapped is None or mapped is LegalEffectType.UNKNOWN:
        return None
    return mapped


def _first_match(patterns: list[tuple[re.Pattern[str], LegalEffectType]], text: str) -> LegalEffectType | None:
    for pattern, effect in patterns:
        if pattern.search(text):
            return effect
    return None


# Instrument boilerplate in proposition_text/action — checked before LLM provision_type (often wrong).
_BOILERPLATE_TEXT_PATTERNS: list[tuple[re.Pattern[str], LegalEffectType]] = [
    (re.compile(r"\bmay be cited as\b"), LegalEffectType.CITATION),
    (re.compile(r"\bshort title\b"), LegalEffectType.CITATION),
    (re.compile(r"\bcome into force\b"), LegalEffectType.COMMENCEMENT),
    (re.compile(r"\bshall come into force\b"), LegalEffectType.COMMENCEMENT),
    (re.compile(r"\bcommencement date\b"), LegalEffectType.COMMENCEMENT),
    (re.compile(r"\bextends?\s+to\b"), LegalEffectType.EXTENT),
    (re.compile(r"\bextend(?:s|ed)?\s+to\b"), LegalEffectType.EXTENT),
    (re.compile(r"\bapply in relation to\b"), LegalEffectType.APPLICATION_SCOPE),
    # Avoid matching permissive "may apply to/for …" (operative permission, not scope boilerplate).
    (re.compile(r"(?<!\bmay )\bapply to\b"), LegalEffectType.APPLICATION_SCOPE),
    (re.compile(r"(?<!\bmay )\bapplies to\b"), LegalEffectType.APPLICATION_SCOPE),
]

# Checked before boilerplate and broad substantive patterns (permission, bare "derogation", etc.).
_PRIORITY_EFFECT_PATTERNS: list[tuple[re.Pattern[str], LegalEffectType]] = [
    (re.compile(r"\bmay apply (?:to|for)\b"), LegalEffectType.PERMISSION),
    (re.compile(r"\bmay exceed\b"), LegalEffectType.PERMISSION),
    (re.compile(r"\bmay be spread\b"), LegalEffectType.PERMISSION),
    (re.compile(r"\bthis regulation does not apply\b"), LegalEffectType.DEROGATION),
    (re.compile(r"\bregulation \d+ does not apply\b"), LegalEffectType.DEROGATION),
    (re.compile(r"\bdoes not apply in a case where\b"), LegalEffectType.DEROGATION),
    (re.compile(r"\bdoes not apply\b"), LegalEffectType.DEROGATION),
    (
        re.compile(
            r"\bthe reference in paragraph\b.+\bdoes not include\b|\bdoes not include\b.+\b(?:land|limit|holding|hectare|greenhouse|paragraph)\b"
        ),
        LegalEffectType.DEROGATION,
    ),
    (re.compile(r"\bmeans a derogation\b"), LegalEffectType.DEFINITION),
    (
        re.compile(
            r"\brequirement\b.+\b(?:silo|slurry storage tank)\b.+\bis satisfied if\b.+\bequivalent\b"
        ),
        LegalEffectType.PERMISSION,
    ),
    (
        re.compile(r"\bconform to a british standard\b.+\bis satisfied if\b"),
        LegalEffectType.PERMISSION,
    ),
]


def _boilerplate_effect_from_text(text: str) -> LegalEffectType | None:
    """Strong textual signals for reg 1-style clauses; overrides noisy extraction provision_type."""
    return _first_match(_BOILERPLATE_TEXT_PATTERNS, text)


def _priority_effect_from_text(text: str) -> LegalEffectType | None:
    """Operative permissions and carve-outs before generic boilerplate/substantive sweeps."""
    return _first_match(_PRIORITY_EFFECT_PATTERNS, text)


# Substantive modal / duty patterns — checked before cross-reference phrases and before
# extraction provision_type "cross_reference" (which often tags operative duties).
_SUBSTANTIVE_TEXT_PATTERNS: list[tuple[re.Pattern[str], LegalEffectType]] = [
    (re.compile(r"\bapply only\b"), LegalEffectType.APPLICATION_SCOPE),
    (re.compile(r"\bis designated as a nitrate vulnerable zone\b"), LegalEffectType.APPLICATION_SCOPE),
    (re.compile(r"['\"][^'\"]{1,120}['\"]\s+means\b"), LegalEffectType.DEFINITION),
    (re.compile(r"\bshall mean\b"), LegalEffectType.DEFINITION),
    (re.compile(r"\bis defined as\b"), LegalEffectType.DEFINITION),
    (re.compile(r"\bthe term\b.+\bmeans\b"), LegalEffectType.DEFINITION),
    (re.compile(r"\bhas the meaning given by\b"), LegalEffectType.DEFINITION),
    (re.compile(r"\bavailable nitrogen is \d+%"), LegalEffectType.DEFINITION),
    (re.compile(r"\bassumed to be available\b"), LegalEffectType.DEFINITION),
    (re.compile(r"\bno account is (?:to be )?taken of\b"), LegalEffectType.DEFINITION),
    (re.compile(r"\bmaximum nitrogen rate of\b"), LegalEffectType.DEFINITION),
    (
        re.compile(r"\bproduce[sd]?\s+\d+(?:\.\d+)?\s*(?:litres|grams|kilograms|kg)\b"),
        LegalEffectType.DEFINITION,
    ),
    (
        re.compile(r"\bfor (?:cattle|pig) slurry\b.*\bavailable nitrogen\b"),
        LegalEffectType.DEFINITION,
    ),
    (re.compile(r"\bperiod for compliance\b.*\b(?:days|months)\b"), LegalEffectType.OBLIGATION),
    (re.compile(r"\bby way of derogation\b"), LegalEffectType.DEROGATION),
    (re.compile(r"\bderogation granted\b"), LegalEffectType.DEROGATION),
    (re.compile(r"\bderogation ceases\b"), LegalEffectType.DEROGATION),
    (re.compile(r"\bfor a derogation\b"), LegalEffectType.DEROGATION),
    (re.compile(r"\bmust not\b|\bshall not\b|\bprohibited\b|\bmay not\b"), LegalEffectType.PROHIBITION),
    (re.compile(r"\b(?:must|shall)\s+not\b"), LegalEffectType.PROHIBITION),
    (re.compile(r"\boffence\b|\bpenalty\b|\bfine\b"), LegalEffectType.ENFORCEMENT),
    (re.compile(r"\benforce(?:ment|d|ing)?\b"), LegalEffectType.ENFORCEMENT),
    (re.compile(r"\bappeal\b"), LegalEffectType.APPEAL),
    (re.compile(r"\bnotif(?:y|ication)\b"), LegalEffectType.NOTIFICATION),
    (re.compile(r"\bcertif(?:y|icate|ication)\b"), LegalEffectType.CERTIFICATION),
    (re.compile(r"\binspect(?:ion|or)?\b"), LegalEffectType.INSPECTION),
    (
        re.compile(
            r"\b(?:make|keep|maintain|hold)\s+(?:a\s+)?records?\b|\brecords?\s+of\b|\brecord(?:s|ing)\s+(?:of|for)\b"
        ),
        LegalEffectType.RECORDKEEPING,
    ),
    (re.compile(r"\bis to be calculated\b"), LegalEffectType.OBLIGATION),
    (re.compile(r"\b(?:must|shall)\b"), LegalEffectType.OBLIGATION),
    (re.compile(r"\bare required to\b|\bis required to\b"), LegalEffectType.OBLIGATION),
    (re.compile(r"\ba person must\b|\ban?\s+\w+\s+must\b"), LegalEffectType.OBLIGATION),
    (re.compile(r"\bis permitted\b"), LegalEffectType.PERMISSION),
    (re.compile(r"\bneed not have\b"), LegalEffectType.PERMISSION),
    (re.compile(r"\bpower to\b"), LegalEffectType.POWER),
    (re.compile(r"\bmay\b"), LegalEffectType.PERMISSION),
]

_DEFINITION_LABEL_PREFIX = re.compile(r"^definition\s*:", re.IGNORECASE)
_DEFINITION_VERB_IN_TEXT = re.compile(
    r"\b("
    r"means|includes|does not include|has the meaning given|is defined as|shall mean|"
    r"is designated as|each have the meaning given|requires that|"
    r"is freshwater|is enriched by"
    r")\b",
    re.IGNORECASE,
)
_UNQUOTED_TERM_MEANS = re.compile(r"^[\w\s\-()]{1,120}\s+means\b", re.IGNORECASE)


def _definition_effect_from_label_and_text(inputs: ClassificationInputs) -> LegalEffectType | None:
    """Repair/workbench rows often use ``Definition: term`` labels with unquoted ``X means`` text."""
    label = inputs.label.strip()
    text = _combined_text(inputs)
    if _DEFINITION_LABEL_PREFIX.match(label) and _DEFINITION_VERB_IN_TEXT.search(text):
        return LegalEffectType.DEFINITION
    pt = inputs.proposition_text.strip()
    if _UNQUOTED_TERM_MEANS.match(pt):
        return LegalEffectType.DEFINITION
    action = inputs.action.strip().lower()
    if action in {"means", "includes", "has the meaning given", "defines"} and _DEFINITION_LABEL_PREFIX.match(
        label
    ):
        return LegalEffectType.DEFINITION
    return None


_CROSS_REFERENCE_TEXT_PATTERNS: list[tuple[re.Pattern[str], LegalEffectType]] = [
    (re.compile(r"\bapply as if they were provisions of\b"), LegalEffectType.CROSS_REFERENCE),
    (re.compile(r"\bthe reference to .+ is substituted\b"), LegalEffectType.CROSS_REFERENCE),
    (re.compile(r"\bis substituted with\b"), LegalEffectType.CROSS_REFERENCE),
    (re.compile(r"\bare omitted\b"), LegalEffectType.CROSS_REFERENCE),
    (re.compile(r"\bpursuant to\b"), LegalEffectType.CROSS_REFERENCE),
    (re.compile(r"\bin accordance with\b"), LegalEffectType.CROSS_REFERENCE),
    (re.compile(r"\bas (?:provided|laid down) in\b"), LegalEffectType.CROSS_REFERENCE),
    (re.compile(r"\breferred to in\b"), LegalEffectType.CROSS_REFERENCE),
]


def _substantive_effect_from_text(text: str) -> LegalEffectType | None:
    return _first_match(_SUBSTANTIVE_TEXT_PATTERNS, text)


def _cross_reference_effect_from_text(text: str) -> LegalEffectType | None:
    return _first_match(_CROSS_REFERENCE_TEXT_PATTERNS, text)


def _is_enforcement_authority_statement(inputs: ClassificationInputs) -> bool:
    """Instrument-level statement of which body enforces — not an operator compliance duty."""
    text = _combined_text(inputs)
    return bool(
        re.search(r"\bthese regulations\b", text) and re.search(r"\bare enforced by\b", text)
    )


def derive_legal_effect_type(
    *,
    proposition_text: str,
    legal_subject: str = "",
    action: str = "",
    conditions: list[str] | None = None,
    extraction_meta: dict[str, Any] | None = None,
    label: str = "",
    short_name: str = "",
    object_text: str = "",
    affected_subjects: list[str] | None = None,
    fragment_locator: str = "",
    source_locator: str = "",
    categories: list[str] | None = None,
    provision_type: str = "",
) -> LegalEffectType:
    """Classify legal effect from text, provision_type, and extraction fields (not legacy categories)."""
    meta = extraction_meta or {}
    pt_meta = str(meta.get("provision_type") or provision_type or "").strip().lower()

    inputs = ClassificationInputs(
        proposition_text=proposition_text,
        legal_subject=legal_subject,
        action=action,
        label=label,
        short_name=short_name,
        object_text=object_text,
        conditions=tuple(conditions or ()),
        affected_subjects=tuple(affected_subjects or ()),
        fragment_locator=fragment_locator,
        source_locator=source_locator,
        categories=tuple(categories or ()),
        provision_type=pt_meta,
    )
    text = _combined_text(inputs)
    if not text.strip():
        return LegalEffectType.UNKNOWN

    priority = _priority_effect_from_text(text)
    if priority is not None:
        return priority

    boilerplate = _boilerplate_effect_from_text(text)
    if boilerplate is not None:
        return boilerplate

    definition = _definition_effect_from_label_and_text(inputs)
    if definition is not None:
        return definition

    substantive = _substantive_effect_from_text(text)
    if substantive is not None:
        return substantive

    from_meta = _effect_from_provision_type(pt_meta)
    if from_meta is not None and from_meta is not LegalEffectType.CROSS_REFERENCE:
        return from_meta

    xref = _cross_reference_effect_from_text(text)
    if xref is not None:
        return xref

    if from_meta is not None:
        return from_meta

    return LegalEffectType.UNKNOWN


def derive_proposition_tier(legal_effect_type: LegalEffectType) -> PropositionTier:
    return _EFFECT_TO_TIER.get(legal_effect_type, PropositionTier.UNKNOWN)


def _extent_places(inputs: ClassificationInputs) -> list[str]:
    for src in (inputs.proposition_text, inputs.action, inputs.label):
        places = extract_territories_from_text(src, context="extent")
        if places:
            return places
    return []


ApplicationScopeKind = Literal["territorial", "subject_object", "conditional", "ambiguous"]

_UK_TERRITORY_NAME = (
    r"England|Wales|Scotland|Northern Ireland|Great Britain|United Kingdom"
)
_APPLY_VERB = r"appl(?:y|ies|ied)"
_TERRITORIAL_APPLY_IN_RE = re.compile(
    rf"\b{_APPLY_VERB}(?:\s+only)?\s+in\s+({_UK_TERRITORY_NAME})\b",
    re.IGNORECASE,
)
_TERRITORIAL_APPLY_TO_IN_RE = re.compile(
    rf"\b{_APPLY_VERB}(?:\s+in relation)?\s+to\s+.+?\s+in\s+({_UK_TERRITORY_NAME})\b",
    re.IGNORECASE,
)
_CONDITIONAL_SCOPE_RE = re.compile(
    rf"^(?:where|if)\b|\b(?:do not|does not|shall not)\s+{_APPLY_VERB}(?:\s+in relation)?\s+to\b"
    rf"|\b{_APPLY_VERB}(?:\s+in relation)?\s+to\b.+\bwhere\b",
    re.IGNORECASE,
)
_SUBJECT_SCOPE_RE = re.compile(
    rf"\b(?:{_APPLY_VERB}|do not apply|does not apply)(?:\s+only)?\s+in relation\s+to\b"
    rf"|\b(?:{_APPLY_VERB}|do not apply|does not apply)\s+to\s+(?:a|an|any|the)\b"
    rf"|\bthis\s+(?:schedule|paragraph|regulation|part)\s+applies\s+to\b"
    rf"|\bparts?\s+\d+(?:\s+to\s+\d+)?\s+apply\b",
    re.IGNORECASE,
)
_SUBJECT_OBJECT_MARKERS = (
    "silo",
    "slurry storage",
    "storage system",
    "fuel storage",
    "fuel oil",
    "holding",
    "farm",
    "agricultural land",
    "nitrate vulnerable",
    "greenhouse",
    "crops",
    "records",
    "construction",
    "contract",
    "tanker",
    "manure",
    "nitrogen",
    "grazing livestock",
    "qualifying grassland",
    "designated",
    "calculation",
)


def _application_scope_places_and_subjects(
    inputs: ClassificationInputs,
) -> tuple[list[str], list[str]]:
    territorial: list[str] = []
    scope_subjects: list[str] = []

    for src in (inputs.proposition_text, inputs.action, inputs.object_text):
        in_m = re.search(
            rf"\bappl(?:y|ies|ied)(?:\s+only)?\s+in\s+({_UK_TERRITORY_NAME})\b",
            src,
            re.IGNORECASE,
        )
        if in_m:
            place = normalize_territory_name(in_m.group(1))
            if place and place not in territorial:
                territorial.append(place)

    for src in (inputs.proposition_text, inputs.action, inputs.object_text):
        m = re.search(
            rf"\b{_APPLY_VERB}(?:\s+in relation)?\s+to\s+(.+?)(?:\.|;|$)",
            src,
            re.IGNORECASE,
        )
        if not m:
            continue
        target = m.group(1).strip()
        in_m = re.search(
            r"^(.+?)\s+in\s+(England|Wales|Scotland|Northern Ireland)(?:\s+only)?\s*$",
            target,
            re.IGNORECASE,
        )
        if in_m:
            subject_part = in_m.group(1).strip()
            place = normalize_territory_name(in_m.group(2))
            if place and place not in territorial:
                territorial.append(place)
            if subject_part and subject_part not in scope_subjects:
                scope_subjects.append(subject_part)
        else:
            places = split_territory_list(target)
            for p in places:
                if p not in territorial:
                    territorial.append(p)
            if not places and target and target not in scope_subjects:
                scope_subjects.append(target)

    return territorial[:8], scope_subjects[:8]


def _application_scope_source_text(
    *,
    proposition_text: str = "",
    action: str = "",
    label: str = "",
    object_text: str = "",
    legal_subject: str = "",
    affected_subjects: list[str] | None = None,
) -> str:
    parts = [
        proposition_text,
        action,
        label,
        object_text,
        legal_subject,
        *(affected_subjects or ()),
    ]
    return " ".join(str(p).strip() for p in parts if str(p or "").strip())


def _has_territorial_application_phrasing(text: str) -> bool:
    if _TERRITORIAL_APPLY_IN_RE.search(text):
        return True
    if _TERRITORIAL_APPLY_TO_IN_RE.search(text):
        return True
    places = extract_territories_from_text(text, context="application_scope")
    return bool(places)


def classify_application_scope_kind(
    *,
    proposition_text: str = "",
    action: str = "",
    label: str = "",
    object_text: str = "",
    legal_subject: str = "",
    affected_subjects: list[str] | None = None,
) -> ApplicationScopeKind:
    """
    Classify whether an application-scope row is territorial, subject/object scoped,
    conditional, or ambiguous.

    Territorial rows should carry ``territorial_application``; subject/object and
    conditional rows should not trigger missing-territory warnings.
    """
    text = _application_scope_source_text(
        proposition_text=proposition_text,
        action=action,
        label=label,
        object_text=object_text,
        legal_subject=legal_subject,
        affected_subjects=affected_subjects,
    )
    if not text.strip():
        return "ambiguous"

    inputs = ClassificationInputs(
        proposition_text=proposition_text,
        legal_subject=legal_subject,
        action=action,
        label=label,
        object_text=object_text,
        affected_subjects=tuple(affected_subjects or ()),
    )
    places, _ = _application_scope_places_and_subjects(inputs)
    if places or _has_territorial_application_phrasing(text):
        return "territorial"

    lowered = text.lower()
    if _CONDITIONAL_SCOPE_RE.search(text) or lowered.startswith("where "):
        return "conditional"

    if _SUBJECT_SCOPE_RE.search(text):
        return "subject_object"
    if any(marker in lowered for marker in _SUBJECT_OBJECT_MARKERS):
        return "subject_object"

    return "ambiguous"


def application_scope_requires_territory(
    *,
    proposition_text: str = "",
    action: str = "",
    label: str = "",
    object_text: str = "",
    legal_subject: str = "",
    affected_subjects: list[str] | None = None,
) -> bool:
    """True when an application-scope row is territorially scoped and needs territory metadata."""
    return (
        classify_application_scope_kind(
            proposition_text=proposition_text,
            action=action,
            label=label,
            object_text=object_text,
            legal_subject=legal_subject,
            affected_subjects=affected_subjects,
        )
        == "territorial"
    )


def derive_territorial_application(
    *,
    legal_effect_type: LegalEffectType,
    proposition_text: str,
    action: str,
    affected_subjects: list[str] | None = None,
    label: str = "",
    object_text: str = "",
    legal_subject: str = "",
    fragment_locator: str = "",
    source_locator: str = "",
    categories: list[str] | None = None,
) -> list[str]:
    if legal_effect_type is not LegalEffectType.APPLICATION_SCOPE:
        return []
    inputs = ClassificationInputs(
        proposition_text=proposition_text,
        legal_subject=legal_subject,
        action=action,
        label=label,
        object_text=object_text,
        affected_subjects=tuple(affected_subjects or ()),
        fragment_locator=fragment_locator,
        source_locator=source_locator,
        categories=tuple(categories or ()),
    )
    places, _ = _application_scope_places_and_subjects(inputs)
    return places


def derive_extent_strings(
    *,
    legal_effect_type: LegalEffectType,
    proposition_text: str,
    action: str = "",
    label: str = "",
    conditions: list[str] | None = None,
    legal_subject: str = "",
    object_text: str = "",
    fragment_locator: str = "",
    source_locator: str = "",
    categories: list[str] | None = None,
) -> list[str]:
    if legal_effect_type is not LegalEffectType.EXTENT:
        return []
    inputs = ClassificationInputs(
        proposition_text=proposition_text,
        legal_subject=legal_subject,
        action=action,
        label=label,
        object_text=object_text,
        conditions=tuple(conditions or ()),
        fragment_locator=fragment_locator,
        source_locator=source_locator,
        categories=tuple(categories or ()),
    )
    return _extent_places(inputs)


def derive_is_compliance_relevant(legal_effect_type: LegalEffectType) -> bool:
    return legal_effect_type in _COMPLIANCE_RELEVANT_EFFECTS


def derive_is_comparison_anchor(
    *,
    proposition_tier: PropositionTier,
    legal_effect_type: LegalEffectType,
) -> bool:
    if legal_effect_type in {LegalEffectType.CITATION, LegalEffectType.COMMENCEMENT, LegalEffectType.EXTENT}:
        return False
    if legal_effect_type in _COMPARISON_ANCHOR_EFFECTS:
        return True
    if proposition_tier in {
        PropositionTier.SUBSTANTIVE_RULE,
        PropositionTier.SCOPE_RULE,
        PropositionTier.DEFINITIONAL_RULE,
        PropositionTier.PROCEDURAL_RULE,
    }:
        return legal_effect_type is not LegalEffectType.UNKNOWN
    return False


def classify_extracted_proposition(
    *,
    proposition_text: str,
    legal_subject: str = "",
    action: str = "",
    conditions: list[str] | None = None,
    affected_subjects: list[str] | None = None,
    extraction_meta: dict[str, Any] | None = None,
    label: str = "",
    short_name: str = "",
    object_text: str = "",
    fragment_locator: str = "",
    source_locator: str = "",
    categories: list[str] | None = None,
    provision_type: str = "",
) -> ClassificationResult:
    """
    Deterministic post-extraction classification from LLM/heuristic fields and text rules.
    """
    meta = extraction_meta or {}
    pt = str(meta.get("provision_type") or provision_type or "").strip().lower()
    inputs = ClassificationInputs(
        proposition_text=proposition_text,
        legal_subject=legal_subject,
        action=action,
        label=label,
        short_name=short_name,
        object_text=object_text,
        conditions=tuple(conditions or ()),
        affected_subjects=tuple(affected_subjects or ()),
        fragment_locator=fragment_locator,
        source_locator=source_locator or fragment_locator,
        categories=tuple(categories or ()),
        provision_type=pt,
    )

    effect = derive_legal_effect_type(
        proposition_text=proposition_text,
        legal_subject=legal_subject,
        action=action,
        conditions=list(inputs.conditions),
        extraction_meta=meta,
        label=label,
        short_name=short_name,
        object_text=object_text,
        affected_subjects=list(inputs.affected_subjects),
        fragment_locator=fragment_locator,
        source_locator=source_locator,
        categories=list(inputs.categories),
        provision_type=pt,
    )
    tier = derive_proposition_tier(effect)

    terr = derive_territorial_application(
        legal_effect_type=effect,
        proposition_text=proposition_text,
        action=action,
        affected_subjects=list(inputs.affected_subjects),
        label=label,
        object_text=object_text,
        legal_subject=legal_subject,
        fragment_locator=fragment_locator,
        source_locator=source_locator,
        categories=list(inputs.categories),
    )
    ext = derive_extent_strings(
        legal_effect_type=effect,
        proposition_text=proposition_text,
        action=action,
        label=label,
        conditions=list(inputs.conditions),
        legal_subject=legal_subject,
        object_text=object_text,
        fragment_locator=fragment_locator,
        source_locator=source_locator,
        categories=list(inputs.categories),
    )

    merged_subjects = list(affected_subjects or [])
    if effect is LegalEffectType.APPLICATION_SCOPE:
        _, scope_subjects = _application_scope_places_and_subjects(inputs)
        for item in scope_subjects:
            if item and item not in merged_subjects:
                merged_subjects.append(item)

    compliance = derive_is_compliance_relevant(effect)
    if effect is LegalEffectType.ENFORCEMENT and _is_enforcement_authority_statement(inputs):
        compliance = False

    return ClassificationResult(
        proposition_tier=tier,
        legal_effect_type=effect,
        territorial_application=terr,
        extent=ext,
        is_compliance_relevant=compliance,
        is_comparison_anchor=derive_is_comparison_anchor(proposition_tier=tier, legal_effect_type=effect),
        affected_subjects=merged_subjects,
    )


def classification_patch_for_proposition(
    *,
    proposition_text: str,
    legal_subject: str = "",
    action: str = "",
    conditions: list[str] | None = None,
    affected_subjects: list[str] | None = None,
    extraction_meta: dict[str, Any] | None = None,
    proposition_tier: PropositionTier | str | None = None,
    legal_effect_type: LegalEffectType | str | None = None,
    territorial_application: list[str] | None = None,
    extent: list[str] | None = None,
    is_compliance_relevant: bool | None = None,
    is_comparison_anchor: bool | None = None,
    label: str = "",
    short_name: str = "",
    object_text: str = "",
    fragment_locator: str = "",
    source_locator: str = "",
    categories: list[str] | None = None,
    provision_type: str = "",
) -> dict[str, Any]:
    """Return canonical classification fields for a proposition payload."""
    result = classify_extracted_proposition(
        proposition_text=proposition_text,
        legal_subject=legal_subject,
        action=action,
        conditions=conditions,
        affected_subjects=affected_subjects,
        extraction_meta=extraction_meta,
        label=label,
        short_name=short_name,
        object_text=object_text,
        fragment_locator=fragment_locator,
        source_locator=source_locator,
        categories=categories,
        provision_type=provision_type,
    )
    patch: dict[str, Any] = {
        "proposition_tier": result.proposition_tier.value,
        "legal_effect_type": result.legal_effect_type.value,
        "territorial_application": list(territorial_application or result.territorial_application),
        "extent": list(extent or result.extent),
        "is_compliance_relevant": (
            is_compliance_relevant
            if is_compliance_relevant is not None
            else result.is_compliance_relevant
        ),
        "is_comparison_anchor": (
            is_comparison_anchor if is_comparison_anchor is not None else result.is_comparison_anchor
        ),
        "affected_subjects": result.affected_subjects,
    }
    if proposition_tier and str(proposition_tier) != PropositionTier.UNKNOWN.value:
        patch["proposition_tier"] = str(proposition_tier)
    if legal_effect_type and str(legal_effect_type) != LegalEffectType.UNKNOWN.value:
        patch["legal_effect_type"] = str(legal_effect_type)
    return patch


def apply_post_extraction_classification(model: Any) -> Any:
    """Run deterministic classification on a Proposition after extraction (in place)."""
    extraction_meta = (
        resolve_extraction_meta_for_proposition(
            notes=str(getattr(model, "notes", "") or ""),
            extraction_debug_meta=getattr(model, "extraction_debug_meta", None),
        )
        or {}
    )
    obj = ""
    affected = list(getattr(model, "affected_subjects", None) or [])
    if affected:
        obj = str(affected[0])
    result = classify_extracted_proposition(
        proposition_text=str(getattr(model, "proposition_text", "") or ""),
        legal_subject=str(getattr(model, "legal_subject", "") or ""),
        action=str(getattr(model, "action", "") or ""),
        conditions=list(getattr(model, "conditions", None) or []),
        affected_subjects=affected,
        extraction_meta=extraction_meta,
        label=str(getattr(model, "label", "") or ""),
        short_name=str(getattr(model, "short_name", "") or ""),
        object_text=obj,
        fragment_locator=str(getattr(model, "fragment_locator", "") or ""),
        source_locator=str(getattr(model, "fragment_locator", "") or ""),
        categories=list(getattr(model, "categories", None) or []),
        provision_type=str(extraction_meta.get("provision_type") or ""),
    )
    model.proposition_tier = result.proposition_tier
    model.legal_effect_type = result.legal_effect_type
    model.territorial_application = list(result.territorial_application)
    model.extent = list(result.extent)
    model.is_compliance_relevant = result.is_compliance_relevant
    model.is_comparison_anchor = result.is_comparison_anchor
    model.affected_subjects = list(result.affected_subjects)
    if result.legal_effect_type is LegalEffectType.APPLICATION_SCOPE:
        scope_kind = classify_application_scope_kind(
            proposition_text=str(getattr(model, "proposition_text", "") or ""),
            action=str(getattr(model, "action", "") or ""),
            label=str(getattr(model, "label", "") or ""),
            object_text=obj,
            legal_subject=str(getattr(model, "legal_subject", "") or ""),
            affected_subjects=affected,
        )
        meta = dict(getattr(model, "extraction_debug_meta", None) or {})
        meta["application_scope_kind"] = scope_kind
        assign_proposition_extraction_debug(model, meta)
    return model


def enrich_proposition_classification(model: Any) -> Any:
    """Model validator hook: notes separation runs first; then post-extraction classification."""
    return apply_post_extraction_classification(model)
