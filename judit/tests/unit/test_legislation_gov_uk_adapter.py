from pathlib import Path

from judit_pipeline.sources import LegislationGovUkAuthorityAdapter, SourceIngestionService


def test_legislation_adapter_fetches_and_normalizes_single_data_xml_shape(tmp_path: Path) -> None:
    xml_payload = """
    <Legislation>
      <Title>Example Act 2024</Title>
      <LongTitle>An Act to illustrate a narrow integration path.</LongTitle>
      <ExplanatoryNotes>
        <P1>Explanatory Notes should not be treated as operative text.</P1>
      </ExplanatoryNotes>
      <Body>
        <P1 id="section-1">Section 1. Operators must maintain records.</P1>
        <P1 id="section-2">Section 2. The authority may inspect records on request.</P1>
      </Body>
    </Legislation>
    """

    def fake_fetch(source_url: str) -> tuple[str, dict[str, object]]:
        return xml_payload, {
            "status": 200,
            "content_type": "application/xml",
            "response_bytes": len(xml_payload),
            "fetched_url": source_url,
        }

    adapter = LegislationGovUkAuthorityAdapter(fetch_xml=fake_fetch)
    service = SourceIngestionService(
        cache_dir=tmp_path / "source-cache",
        adapters={"legislation_gov_uk": adapter},
    )
    result = service.ingest_sources(
        [
            {
                "authority": "legislation_gov_uk",
                "authority_source_id": "ukpga/2024/1",
                "version_id": "2024-01-01",
                "id": "src-uk-leg-001",
                "fragment_locator": "section:1",
            }
        ]
    )

    assert result.traces[0]["decision"] == "fetched_then_cached"
    assert result.traces[0]["adapter"] == "LegislationGovUkAuthorityAdapter"
    assert result.traces[0]["adapter_trace"]["http_status"] == 200
    assert result.sources[0].title == "Example Act 2024"
    assert result.sources[0].citation == "UKPGA 2024/1"
    assert result.sources[0].provenance == "authority.legislation_gov_uk"
    assert "Section 1. Operators must maintain records." in result.sources[0].authoritative_text
    assert "Explanatory Notes should not be treated as operative text." not in (
        result.sources[0].authoritative_text
    )
    # Raw fragment_locator input still takes precedence to preserve compatibility.
    assert result.sources[0].authoritative_locator == "section:1"
    assert result.sources[0].metadata["fragment_locators"] == ["xml:section-1", "xml:section-2"]
    assert result.sources[0].metadata["operative_chunk_count"] == 2
    assert result.snapshots[0].source_record_id == "src-uk-leg-001"
    assert result.fragments[0].locator == "section:1"


def test_legislation_adapter_emits_structural_schedule_fragments(tmp_path: Path) -> None:
    xml_payload = """
    <Legislation DocumentURI="http://www.legislation.gov.uk/ssi/2019/71">
      <Title>Example SSI</Title>
      <Body id="body">
        <P1 id="regulation-1">
          <Pnumber>1.</Pnumber>
          <Text>These Regulations come into force on 1 April 2019.</Text>
        </P1>
        <P1 id="schedule-1">
          <Title>Schedule 1</Title>
          <Text>Inspection powers.</Text>
        </P1>
        <P2 id="schedule-1-paragraph-3">
          <Pnumber>3.</Pnumber>
          <Text>Inspectors may enter premises for enforcement.</Text>
        </P2>
      </Body>
    </Legislation>
    """

    def fake_fetch(source_url: str) -> tuple[str, dict[str, object]]:
        return xml_payload, {
            "status": 200,
            "content_type": "application/xml",
            "response_bytes": len(xml_payload),
            "fetched_url": source_url,
        }

    adapter = LegislationGovUkAuthorityAdapter(fetch_xml=fake_fetch)
    service = SourceIngestionService(
        cache_dir=tmp_path / "source-cache",
        adapters={"legislation_gov_uk": adapter},
    )
    result = service.ingest_sources(
        [
            {
                "authority": "legislation_gov_uk",
                "authority_source_id": "ssi/2019/71",
                "version_id": "2024-01-01",
                "id": "src-ssi-leg-071",
            }
        ]
    )

    locators = [frag.locator for frag in result.fragments]
    assert "regulation:1" in locators
    assert "schedule:1" in locators
    assert "schedule:1:paragraph:3" in locators
    by_locator = {frag.locator: frag for frag in result.fragments}
    assert "Schedule 1" not in by_locator["regulation:1"].fragment_text
    assert by_locator["schedule:1:paragraph:3"].parent_fragment_id == by_locator["schedule:1"].id
    assert by_locator["schedule:1"].metadata.get("source_path") == "schedule/1"
    assert by_locator["schedule:1:paragraph:3"].metadata.get("fragment_kind") in {
        "amendment_provision",
        "operative_provision",
    }
