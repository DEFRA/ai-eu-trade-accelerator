"""The `radia run` CLI runs both passes and writes its files, fully offline.

`classify` and `adjudicate` (the only API-calling parts) are stubbed, so this
covers argument parsing and file output without a network call or an API key.
"""

import json

from typer.testing import CliRunner

from radia.cli import app

runner = CliRunner()


def test_run_writes_output_audit_and_a_model_card(tmp_path, monkeypatch, build_page):
    input_path = tmp_path / "pages.json"
    input_path.write_text(json.dumps([build_page()]))
    output_dir = tmp_path / "run"

    def fake_classify(items, categories, lexicons, **_):
        labelled = {**items[0], "meta_data": {
            "labels": {"slurry": True}, "scores": {"slurry": 0.3},
            "reasons": {"slurry": "mentions slurry"}}}
        return [labelled], {"n_routed": 1, "n_skipped": 0, "n_rescued": 0,
                            "model": "claude-haiku-4-5-20251001",
                            "usage": {"model": "claude-haiku-4-5-20251001",
                                      "batch": {"input": 100, "output": 50},
                                      "rescue": {"input": 0, "output": 0}}}

    def fake_adjudicate(items, results, categories, **_):
        results[0]["meta_data"]["labels"]["slurry"] = False  # pass 2 drops it
        audit = [{"url": results[0]["url"], "pass2_keep": False}]
        return results, {"n_positives": 1, "n_kept": 0, "n_dropped": 1,
                         "n_unadjudicated": 0, "model": "claude-sonnet-4-6",
                         "usage": {"model": "claude-sonnet-4-6",
                                   "batch": {"input": 200, "output": 80}}}, audit

    monkeypatch.setattr("radia.cli.classify", fake_classify)
    monkeypatch.setattr("radia.cli.adjudicate", fake_adjudicate)

    result = runner.invoke(app, ["run", str(input_path), str(output_dir)])

    assert result.exit_code == 0, result.output
    written = json.loads((output_dir / "output.json").read_text())
    assert written[0]["meta_data"]["labels"]["slurry"] is False  # final, post-pass-2
    assert json.loads((output_dir / "adjudication.json").read_text())[0]["pass2_keep"] is False
    assert (output_dir / "MODEL.md").exists()

    metrics = json.loads((output_dir / "metrics.json").read_text())
    assert metrics["tokens"] == {"input": 300, "output": 130}  # 100+200 in, 50+80 out
    assert metrics["pass1"]["cost_usd"] is not None and metrics["pass2"]["cost_usd"] is not None
    assert metrics["cost_usd"] == round(
        metrics["pass1"]["cost_usd"] + metrics["pass2"]["cost_usd"], 4
    )
    assert "Total:" in (output_dir / "MODEL.md").read_text()
