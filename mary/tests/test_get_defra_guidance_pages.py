"""Stage 1 discovery: reshaping a GOV.UK Search API hit into Mary's page contract."""

from get_defra_guidance_pages import to_pipeline_item

SEARCH_HIT = {
    "link": "/guidance/store-slurry-safely",
    "content_id": "abc-123",
    "title": "Store slurry safely",
    "description": "How to store slurry.",
    "public_timestamp": "2024-01-01T00:00:00Z",
    "content_store_document_type": "guidance",
    "view_count": 42,
}


def test_maps_a_search_hit_to_the_page_contract():
    item = to_pipeline_item(SEARCH_HIT)
    assert item["url"] == "https://www.gov.uk/guidance/store-slurry-safely"
    assert item["content_id"] == "abc-123"
    assert item["meta_data"]["updated_at"] == "2024-01-01T00:00:00Z"
    assert item["meta_data"]["document_type"] == "guidance"


def test_defaults_fields_missing_from_a_hit():
    item = to_pipeline_item({})
    assert item["url"] == "https://www.gov.uk"
    assert item["meta_data"]["view_count"] == 0
