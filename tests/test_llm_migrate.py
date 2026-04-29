"""Tests for LLM migration helpers (mocked serving; no workspace calls)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from aibi_migrator.llm_migrate import (
    apply_primary_dataset_sql_rail,
    build_user_payload,
    parse_migration_response,
    run_llm_migration,
    use_llm_migration_from_env,
)


def test_use_llm_migration_from_env_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("USE_LLM_MIGRATION", raising=False)
    assert use_llm_migration_from_env() is True
    monkeypatch.setenv("USE_LLM_MIGRATION", "0")
    assert use_llm_migration_from_env() is False


def test_parse_migration_response_markdown_fence() -> None:
    raw = """Here is JSON:
```json
{"lakeview_dashboard": {"datasets": [], "pages": []}, "genie": {"title": "T", "description": "D", "text_instruction": "I"}}
```
"""
    data, err = parse_migration_response(raw)
    assert err is None
    assert data is not None
    assert data["genie"]["title"] == "T"


def test_apply_primary_dataset_sql_rail_overrides_query_lines() -> None:
    lv = {
        "datasets": [
            {"name": "primary", "displayName": "X", "queryLines": ["SELECT 999 "]},
        ],
        "pages": [],
    }
    starter = "SELECT `a` FROM `c`.`s`.`t` LIMIT 5"
    out = apply_primary_dataset_sql_rail(lv, starter_sql=starter, dataset_name="primary")
    assert out["datasets"][0]["queryLines"] != ["SELECT 999 "]


def test_run_llm_migration_end_to_end_mocked() -> None:
    payload = {
        "lakeview_dashboard": {
            "datasets": [
                {"name": "primary", "displayName": "P", "queryLines": ["SELECT 1 "]},
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
        "genie": {
            "title": "My Genie",
            "description": "Desc",
            "text_instruction": "Use the bound tables.",
            "sample_questions": ["Q1?", "Q2?"],
        },
        "gap_notes": ["note a"],
    }
    wr = MagicMock()
    wr.query_serving_endpoint_chat.return_value = (
        "```json\n" + json.dumps(payload) + "\n```",
        None,
    )
    res = run_llm_migration(
        wr,
        serving_endpoint_name="test-endpoint",
        dashboard_display_name="Dash",
        bindings={"sales": "prod.raw.sales"},
        ordered_model_tables=["sales"],
        measure_names=["rev"],
        starter_sql="SELECT `id` FROM `prod`.`raw`.`sales` LIMIT 10",
        column_names=["id"],
        visual_rows=[{"visual_type": "table", "intent_statement": "rows"}],
        canonical_models_json='[{"source_file":"a.pbit","model":{"tables":[]}}]',
        max_retries=0,
    )
    assert res["ok"] is True
    assert res["genie_title"] == "My Genie"
    assert res["sample_questions"] == ["Q1?", "Q2?"]
    assert res["gap_notes"] == ["note a"]
    ql = res["lakeview_dashboard"]["datasets"][0]["queryLines"]
    assert "sales" in "".join(ql) or "id" in "".join(ql)
    wr.query_serving_endpoint_chat.assert_called_once()


def test_run_llm_migration_query_error() -> None:
    wr = MagicMock()
    wr.query_serving_endpoint_chat.return_value = (None, "permission denied")
    res = run_llm_migration(
        wr,
        serving_endpoint_name="x",
        dashboard_display_name="D",
        bindings={"t": "a.b.c"},
        ordered_model_tables=["t"],
        measure_names=[],
        starter_sql="SELECT `x` FROM `a`.`b`.`c` LIMIT 1",
        column_names=["x"],
        visual_rows=None,
        canonical_models_json="{}",
        max_retries=0,
    )
    assert res["ok"] is False
    assert "permission" in res["errors"][0].lower()


def test_build_user_payload_includes_bindings() -> None:
    u = build_user_payload(
        dashboard_display_name="N",
        bindings={"t1": "c.s.t1"},
        ordered_model_tables=["t1"],
        measure_names=["m"],
        starter_sql="SELECT 1",
        column_names=["a"],
        visual_rows=[],
        canonical_models_json='{"k":1}',
        max_chars=50_000,
    )
    assert "c.s.t1" in u
    assert "N" in u
