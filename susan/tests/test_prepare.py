from susan.cli import _radia_to_pages


def test_radia_to_pages_keeps_on_topic_and_projects():
    rows = [
        {"url": "u1", "meta_data": {"title": "T1", "labels": {"slurry": True}}},
        {"url": "u2", "meta_data": {"title": "T2", "labels": {"slurry": False}}},
        {"url": "u3", "meta_data": {"labels": {"slurry": True}}},  # no title -> ""
        {"url": "u4", "meta_data": {}},  # no labels -> dropped
        {"url": "u5", "meta_data": {"labels": {"slurry": 1}}},  # truthy != True -> dropped
    ]
    assert _radia_to_pages(rows, "slurry") == [
        {"url": "u1", "title": "T1"},
        {"url": "u3", "title": ""},
    ]


def test_radia_to_pages_respects_category():
    rows = [
        {"url": "u1", "meta_data": {"title": "T1", "labels": {"slurry": True, "nitrate": False}}},
        {"url": "u2", "meta_data": {"title": "T2", "labels": {"nitrate": True}}},
    ]
    assert _radia_to_pages(rows, "nitrate") == [{"url": "u2", "title": "T2"}]
