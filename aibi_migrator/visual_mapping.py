"""Map Power BI visual types to Lakeview parity targets and widget strategies."""

from __future__ import annotations

from aibi_migrator.canonical.models import ParityGapTarget


def _norm_vt(vt: str | None) -> str:
    return (vt or "").strip().lower()


def parity_target_for_visual_type(visual_type: str | None) -> ParityGapTarget:
    """Target Lakeview representation for parity manifest (aligned with dashboard_builder)."""
    v = _norm_vt(visual_type)
    if not v:
        return ParityGapTarget.lakeview_multiline_intent

    if "table" in v or "matrix" in v or v in ("pivotTable",):
        return ParityGapTarget.lakeview_table_preview

    chart_tokens = (
        "chart",
        "graph",
        "line",
        "bar",
        "column",
        "pie",
        "donut",
        "area",
        "scatter",
        "waterfall",
        "funnel",
        "treemap",
        "map",
        "shape",
        "ribbon",
        "box",
        "histogram",
    )
    if any(t in v for t in chart_tokens):
        return ParityGapTarget.lakeview_chart_placeholder

    return ParityGapTarget.lakeview_multiline_intent


def has_layout_bbox(row: dict) -> bool:
    """True if row has enough layout to place on a canvas grid."""
    keys = ("layout_x", "layout_y", "layout_w", "layout_h")
    return all(row.get(k) is not None for k in keys)


def is_pie_like_visual(visual_type: str | None) -> bool:
    v = _norm_vt(visual_type)
    return "pie" in v or "donut" in v


def is_line_like_visual(visual_type: str | None) -> bool:
    v = _norm_vt(visual_type)
    return "line" in v or "area" in v


def _trim_sql_columns(column_names: list[str] | None) -> list[str]:
    if not column_names:
        return []
    return [c for c in column_names if c and not str(c).startswith("#")][:48]


def resolve_chart_parity_target(visual_type: str | None, column_count: int) -> ParityGapTarget:
    """Refine chart placeholder into a concrete bound chart type when the dataset has enough columns."""
    if column_count < 2:
        return ParityGapTarget.lakeview_chart_placeholder
    if is_pie_like_visual(visual_type):
        return ParityGapTarget.lakeview_pie_chart
    if is_line_like_visual(visual_type):
        return ParityGapTarget.lakeview_line_chart
    return ParityGapTarget.lakeview_bar_chart


def x_scale_type_for_line_axis(column_name: str) -> str:
    """Heuristic: temporal vs categorical for first axis on line charts."""
    n = (column_name or "").lower()
    if any(t in n for t in ("date", "time", "month", "year", "timestamp", "_at", "day")):
        return "temporal"
    return "categorical"
