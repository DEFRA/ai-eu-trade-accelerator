"""ADR-0018: proposition identity vs naming — structural checks."""

import re

import pytest

from judit_domain import Proposition
from judit_pipeline.runner import (
    _build_source_derived_proposition_key,
    _derive_slug,
    _opaque_machine_proposition_id,
)


def _sample_proposition(**overrides: object) -> Proposition:
    base = dict(
        id="prop-demo-src-001",
        topic_id="topic-movement",
        source_record_id="eur-2016-429",
        source_snapshot_id="snap-1",
        source_fragment_id=None,
        fragment_locator="art-109",
        jurisdiction="EU",
        proposition_text="The keeper shall ensure identification.",
        legal_subject="keeper",
        action="ensure identification",
        notes="unit",
    )
    base.update(overrides)
    return Proposition.model_validate(base)


def test_opaque_proposition_id_is_stable_hex_suffix() -> None:
    prop = _sample_proposition(id="prop-demo-src-001")
    opaque = _opaque_machine_proposition_id(prop, "001")
    assert re.fullmatch(r"prop:[a-f0-9]{16}", opaque), opaque


def test_opaque_id_excludes_typical_taxonomy_noise() -> None:
    prop = _sample_proposition(
        id="prop-demo-src-003",
        legal_subject="equine-movement-operator",
        action="must register horses",
        categories=["movement"],
        tags=["equine"],
    )
    oid = _opaque_machine_proposition_id(prop, "003")
    lowered = oid.lower()
    assert "equine" not in lowered
    assert "movement" not in lowered
    assert "pkey:" not in lowered


def test_source_derived_proposition_key_matches_three_part_pattern() -> None:
    prop = _sample_proposition(id="prop-x-099")
    key = _build_source_derived_proposition_key(prop, "099")
    assert re.fullmatch(r"[a-z0-9-]+:[a-z0-9-]+:p\d{3}", key), key
    assert key.endswith(":p099")


def test_slug_is_not_identity_and_derived_from_label() -> None:
    prop = _sample_proposition()
    label = "Article 109 — habitual establishment requirement"
    slug = _derive_slug(label, "habitual establishment requirement")
    assert slug
    assert slug != prop.id
    assert not slug.startswith("prop:")


def test_explicit_proposition_key_preserved_when_set() -> None:
    custom = "retained-2016-429:reg-12:p001"
    prop = _sample_proposition(proposition_key=custom)
    assert prop.proposition_key == custom


@pytest.mark.parametrize(
    ("bad_fragment",),
    [
        ("equine-movement-art109-001",),
        ("obligation-category-x",),
    ],
)
def test_machine_id_never_resembles_legacy_slug_identity(bad_fragment: str) -> None:
    prop = _sample_proposition(id=f"prop-anchor-{bad_fragment}-001")
    oid = _opaque_machine_proposition_id(prop, "001")
    assert bad_fragment not in oid
