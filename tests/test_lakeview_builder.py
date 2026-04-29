import json

import pytest

from aibi_migrator.lakeview.dashboard_builder import (
    build_migrated_dashboard_with_pbi_views,
    build_minimal_migrated_dashboard,
)


def test_build_minimal_migrated_dashboard_shape() -> None:
    d = build_minimal_migrated_dashboard(
        dashboard_title="T",
        subtitle="S",
        dataset_display_name="D",
        dataset_name="primary",
        starter_sql="SELECT `a`, `b` FROM `c`.`s`.`t` LIMIT 10",
        column_names=["a", "b"],
    )
    assert "datasets" in d and "pages" in d
    assert d["datasets"][0]["name"] == "primary"
    assert len(d["pages"][0]["layout"]) == 3
    json.dumps(d)


def test_build_migrated_dashboard_with_pbi_views_extra_pages() -> None:
    visual_rows = [
        {
            "source_file": "a.pbit",
            "report_name": "Sales",
            "page_name": "Summary",
            "visual_type": "card",
            "intent_statement": "Show total revenue",
        },
        {
            "source_file": "a.pbit",
            "report_name": "Sales",
            "page_name": "Details",
            "visual_type": "table",
            "intent_statement": "List orders",
        },
    ]
    d = build_migrated_dashboard_with_pbi_views(
        dashboard_title="T",
        subtitle="S",
        dataset_display_name="D",
        dataset_name="primary",
        starter_sql="SELECT `a` FROM `c`.`s`.`t` LIMIT 10",
        column_names=["a"],
        visual_rows=visual_rows,
    )
    assert len(d["pages"]) == 3
    names = [p["name"] for p in d["pages"]]
    assert names[0] == "overview"
    assert names[1] != names[2]
    assert d["pages"][1]["displayName"] == "Sales — Summary"
    assert d["pages"][2]["displayName"] == "Sales — Details"
    json.dumps(d)


def test_build_migrated_dashboard_with_pbi_views_empty_visuals() -> None:
    d = build_migrated_dashboard_with_pbi_views(
        dashboard_title="T",
        subtitle="S",
        dataset_display_name="D",
        dataset_name="primary",
        starter_sql="SELECT `a` FROM `c`.`s`.`t` LIMIT 10",
        column_names=["a"],
        visual_rows=[],
    )
    assert len(d["pages"]) == 1


def test_build_minimal_requires_columns() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        build_minimal_migrated_dashboard(
            dashboard_title="T",
            subtitle="S",
            dataset_display_name="D",
            dataset_name="primary",
            starter_sql="SELECT 1",
            column_names=[],
        )
