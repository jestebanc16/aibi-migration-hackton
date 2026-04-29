"""Workspace deploy integration: LLM path vs heuristic (mocked)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from aibi_migrator.workspace_deploy import run_workspace_deploy


def _minimal_llm_ok() -> dict:
    return {
        "ok": True,
        "lakeview_dashboard": {
            "datasets": [
                {"name": "primary", "displayName": "Primary", "queryLines": ["SELECT 1 "]},
            ],
            "pages": [
                {
                    "name": "overview",
                    "displayName": "Overview",
                    "pageType": "PAGE_TYPE_CANVAS",
                    "layout": [],
                }
            ],
        },
        "genie_title": "Genie from LLM",
        "genie_description": "Long description",
        "genie_text_instruction": "Instruction body for Genie.",
        "sample_questions": ["Count rows?"],
        "gap_notes": [],
        "raw_assistant": "{}",
    }


@patch("aibi_migrator.workspace_deploy.run_llm_migration")
def test_run_workspace_deploy_llm_json_to_deploy_shape(mock_llm: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USE_LLM_MIGRATION", "1")
    mock_llm.return_value = _minimal_llm_ok()

    wr = MagicMock()
    wr.describe_table_columns.return_value = (["id", "name"], None)
    wr.deploy_lakeview_dashboard.return_value = {"ok": True, "dashboard_id": "dash-1"}
    wr.deploy_genie_space.return_value = {"ok": True, "space_id": "space-1"}

    out = run_workspace_deploy(
        wr,
        warehouse_id="wh-1",
        parent_path="/Workspace/Shared",
        dashboard_display_name="My Dash",
        bindings={"orders": "demo.default.orders"},
        ordered_model_tables=["orders"],
        measure_names=[],
        publish_lakeview=True,
        create_genie=True,
        serving_endpoint_name="databricks-meta-llama-3-1-70b-instruct",
        canonical_models_json='[{"source_file":"x.pbit","model":{"tables":[]}}]',
    )

    assert out["ok"] is True
    assert out["llm_assisted"] is True
    call_kw = wr.deploy_lakeview_dashboard.call_args.kwargs
    sd = json.loads(call_kw["serialized_dashboard"])
    assert "datasets" in sd and "pages" in sd
    mock_llm.assert_called_once()


@patch("aibi_migrator.workspace_deploy.run_llm_migration")
def test_run_workspace_deploy_llm_fails_closed(mock_llm: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USE_LLM_MIGRATION", "1")
    mock_llm.return_value = {"ok": False, "errors": ["bad json"], "raw_assistant": "x"}

    wr = MagicMock()
    wr.describe_table_columns.return_value = (["id"], None)

    out = run_workspace_deploy(
        wr,
        warehouse_id="wh-1",
        parent_path="/Workspace/Shared",
        dashboard_display_name="D",
        bindings={"t": "a.b.c"},
        ordered_model_tables=["t"],
        measure_names=[],
        publish_lakeview=True,
        create_genie=False,
        serving_endpoint_name="ep",
        canonical_models_json="{}",
    )

    assert out["ok"] is False
    assert out["phase"] == "llm"
    wr.deploy_lakeview_dashboard.assert_not_called()


def test_run_workspace_deploy_heuristic_skips_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("USE_LLM_MIGRATION", "0")

    wr = MagicMock()
    wr.describe_table_columns.return_value = (["region", "amt"], None)
    wr.deploy_lakeview_dashboard.return_value = {"ok": True, "dashboard_id": "d"}
    wr.deploy_genie_space.return_value = {"ok": True, "space_id": "s"}

    with patch("aibi_migrator.workspace_deploy.run_llm_migration") as mock_llm:
        out = run_workspace_deploy(
            wr,
            warehouse_id="wh",
            parent_path="/Workspace/Shared",
            dashboard_display_name="T",
            bindings={"t": "c.s.t"},
            ordered_model_tables=["t"],
            measure_names=[],
            publish_lakeview=True,
            create_genie=True,
            visual_rows=[
                {
                    "report_name": "R",
                    "page_name": "P",
                    "visual_type": "card",
                    "intent_statement": "kpi",
                }
            ],
            use_llm_migration=False,
        )
    assert out["ok"] is True
    assert out["llm_assisted"] is False
    mock_llm.assert_not_called()
