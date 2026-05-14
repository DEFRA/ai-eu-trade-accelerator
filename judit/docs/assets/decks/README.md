# Deck assets

Marp markdown files in this directory are the source of truth for decks. Generated HTML/PDF/PPTX live under `docs/assets/generated/decks/` and are committed.

## Source files

- `docs/assets/decks/judit-stakeholder-overview.md` — stakeholder-facing narrative of what Judit is and why this shape works.
- `docs/assets/decks/judit-technical-architecture.md` — engineer-facing architecture, model boundaries, and ADR pointers.
- `docs/assets/decks/judit-operator-workflows.md` — operator-facing runbook (registry, fragment selection, repair, review, equine staged corpus).

## Generated outputs

For each source `judit-<name>.md`, `just decks` writes:

- `docs/assets/generated/decks/judit-<name>.html`
- `docs/assets/generated/decks/judit-<name>.pdf`
- `docs/assets/generated/decks/judit-<name>.pptx`

Direct links (once generated):

- [Stakeholder overview - PDF](../generated/decks/judit-stakeholder-overview.pdf) · [HTML](../generated/decks/judit-stakeholder-overview.html) · [PPTX](../generated/decks/judit-stakeholder-overview.pptx)
- [Technical architecture - PDF](../generated/decks/judit-technical-architecture.pdf) · [HTML](../generated/decks/judit-technical-architecture.html) · [PPTX](../generated/decks/judit-technical-architecture.pptx)
- [Operator workflows - PDF](../generated/decks/judit-operator-workflows.pdf) · [HTML](../generated/decks/judit-operator-workflows.html) · [PPTX](../generated/decks/judit-operator-workflows.pptx)

Regenerate with `just decks`. `just docs-refresh` runs the full assets refresh and rebuilds the docs site.

## Authoring notes

- Keep deck structure and language aligned with canonical docs under `docs/canonical/` and architecture docs under `docs/architecture/`.
- Prefer wiring visuals from `docs/assets/generated/diagrams/*.svg` and `docs/assets/generated/infographics/*.svg` into deck slides rather than duplicating static images.
- Do not embed legally-binding text in slides; cite the source doc and link back.
- Do not claim complete legal-area coverage from any single staged corpus profile.
- `guidance-ready` is a promotion state earned through review; do not present it as default output.
- Lint pass means artifact integrity / quality-gate pass, not legal approval.
