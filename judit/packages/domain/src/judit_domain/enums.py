from enum import StrEnum


class DivergenceType(StrEnum):
    NONE = "none"
    TEXTUAL = "textual"
    STRUCTURAL = "structural"
    DYNAMIC_REFERENCE = "dynamic_reference"
    TERRITORIAL = "territorial"
    INSTITUTIONAL = "institutional"
    PROCEDURAL = "procedural"
    ANNEX_MODEL_CERTIFICATE = "annex_model_certificate"
    DEFINITIONAL = "definitional"
    REVOCATION_REPLACEMENT = "revocation_replacement"
    UNKNOWN = "unknown"


class ConfidenceLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class PropositionTier(StrEnum):
    INSTRUMENT_METADATA = "instrument_metadata"
    SCOPE_RULE = "scope_rule"
    SUBSTANTIVE_RULE = "substantive_rule"
    PROCEDURAL_RULE = "procedural_rule"
    DEFINITIONAL_RULE = "definitional_rule"
    RELATIONSHIP_REFERENCE = "relationship_reference"
    UNKNOWN = "unknown"


class LegalEffectType(StrEnum):
    CITATION = "citation"
    COMMENCEMENT = "commencement"
    EXTENT = "extent"
    APPLICATION_SCOPE = "application_scope"
    DEFINITION = "definition"
    OBLIGATION = "obligation"
    PROHIBITION = "prohibition"
    PERMISSION = "permission"
    POWER = "power"
    RECORDKEEPING = "recordkeeping"
    NOTIFICATION = "notification"
    CERTIFICATION = "certification"
    INSPECTION = "inspection"
    ENFORCEMENT = "enforcement"
    APPEAL = "appeal"
    DEROGATION = "derogation"
    CROSS_REFERENCE = "cross_reference"
    UNKNOWN = "unknown"


class ReviewStatus(StrEnum):
    PROPOSED = "proposed"
    NEEDS_REVIEW = "needs_review"
    ACCEPTED = "accepted"
    ACCEPTED_WITH_EDITS = "accepted_with_edits"
    REJECTED = "rejected"
    NEEDS_MORE_SOURCES = "needs_more_sources"
    SUPERSEDED = "superseded"

    # Legacy statuses kept for backward compatibility.
    DRAFT = "draft"
    IN_REVIEW = "in_review"
