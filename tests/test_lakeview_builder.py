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
    # Details page uses tableEx-like mapping → Lakeview table on primary dataset.
    detail_layout = d["pages"][2]["layout"]
    table_widgets = [
        w
        for w in detail_layout
        if w.get("widget", {}).get("spec", {}).get("widgetType") == "table"
    ]
    assert len(table_widgets) >= 1
    json.dumps(d)


def test_build_migrated_dashboard_bar_chart_when_two_columns() -> None:
    d = build_migrated_dashboard_with_pbi_views(
        dashboard_title="T",
        subtitle="S",
        dataset_display_name="D",
        dataset_name="primary",
        starter_sql="SELECT `region`, `amt` FROM `c`.`s`.`t` LIMIT 10",
        column_names=["region", "amt"],
        visual_rows=[
            {
                "report_name": "R",
                "page_name": "P",
                "visual_type": "columnChart",
                "intent_statement": "Trend",
            }
        ],
    )
    page = d["pages"][1]
    bars = [
        w
        for w in page["layout"]
        if w.get("widget", {}).get("spec", {}).get("widgetType") == "bar"
    ]
    assert len(bars) == 1
    enc = bars[0]["widget"]["spec"]["encodings"]
    assert enc["x"]["fieldName"] == "region"
    assert enc["y"]["fieldName"] == "amt"


def test_build_migrated_dashboard_chart_placeholder_when_one_column() -> None:
    d = build_migrated_dashboard_with_pbi_views(
        dashboard_title="T",
        subtitle="S",
        dataset_display_name="D",
        dataset_name="primary",
        starter_sql="SELECT `a` FROM `c`.`s`.`t` LIMIT 10",
        column_names=["a"],
        visual_rows=[
            {
                "report_name": "R",
                "page_name": "P",
                "visual_type": "columnChart",
                "intent_statement": "Trend",
            }
        ],
    )
    page = d["pages"][1]
    texts = [
        w["widget"].get("multilineTextboxSpec", {}).get("lines", [])
        for w in page["layout"]
        if "multilineTextboxSpec" in w.get("widget", {})
    ]
    flat = "\n".join(" ".join(lines) for lines in texts)
    assert "Chart placeholder" in flat


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
