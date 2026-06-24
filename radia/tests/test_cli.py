"""The `radia run` CLI writes output.json + a MODEL.md, fully offline.

`classify` (the only API-calling part) is stubbed, so this covers argument
parsing and file output without a network call or an API key.
"""

import json

from typer.testing import CliRunner

from radia.cli import app

runner = CliRunner()


def test_run_writes_the_labelled_output_and_a_model_card(tmp_path, monkeypatch, build_page):
    input_path = tmp_path / "pages.json"
    input_path.write_text(json.dumps([build_page()]))
    output_dir = tmp_path / "run"

    def fake_classify(items, categories, lexicons, **_):
        labelled = {**items[0], "meta_data": {"labels": {"slurry": True}}}
        return [labelled], {"n_routed": 1, "n_skipped": 0, "n_rescued": 0}

    monkeypatch.setattr("radia.cli.classify", fake_classify)

    result = runner.invoke(app, ["run", str(input_path), str(output_dir)])

    assert result.exit_code == 0, result.output
    written = json.loads((output_dir / "output.json").read_text())
    assert written[0]["meta_data"]["labels"]["slurry"] is True
    assert (output_dir / "MODEL.md").exists()
