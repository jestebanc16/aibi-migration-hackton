"""Minimal Lakeview (AI/BI) dashboard JSON for post-migration deploy."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from aibi_migrator.canonical.models import ParityGapTarget
from aibi_migrator.visual_mapping import (
    _trim_sql_columns,
    has_layout_bbox,
    is_line_like_visual,
    is_pie_like_visual,
    parity_target_for_visual_type,
    x_scale_type_for_line_axis,
)

# Kept in sync with parity manifest / RULES coverage caps.
DEFAULT_MAX_EXTRA_PAGES = 28
DEFAULT_MAX_VISUALS_PER_PAGE = 45


def _sql_to_query_lines(sql: str, max_len: int = 96) -> list[str]:
    """Split SQL into queryLines fragments (Lakeview dataset contract)."""
    text = " ".join((sql or "").split())
    if not text:
        return ["SELECT 1 AS empty "]
    lines: list[str] = []
    buf: list[str] = []
    n = 0
    for w in text.split():
        if n + len(w) + 1 > max_len and buf:
            lines.append(" ".join(buf) + " ")
            buf = [w]
            n = len(w)
        else:
            buf.append(w)
            n += len(w) + (1 if len(buf) > 1 else 0)
    if buf:
        lines.append(" ".join(buf) + " ")
    return lines


def _safe_display_line(s: str | None) -> str:
    t = (s or "").replace("/", "-").replace("\\", "-").strip()
    return t if t else "—"


def _lakeview_widget_name(prefix: str, idx: int) -> str:
    raw = f"{prefix}-v-{idx}"
    x = re.sub(r"[^a-zA-Z0-9_-]+", "-", raw.lower()).strip("-")[:60]
    return x or f"v-{idx}"


def _unique_page_slug(report: str, page: str, used: set[str]) -> str:
    n = 0
    while True:
        suffix = f"-{n}" if n else ""
        s = _lakeview_name_part(f"{report}-{page}{suffix}")[:52] or f"p{n}"
        if s not in used:
            used.add(s)
            return s
        n += 1
        if n > 500:
            return _lakeview_name_part(f"page-{id(report)}-{id(page)}")[:52] or "page-extra"


def _lakeview_name_part(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", (s or "").strip().lower()).strip("-")[:44] or "x"


def _row_sort_key(row: dict[str, Any]) -> tuple[float, float, float]:
    return (
        float(row.get("layout_y") or 0.0),
        float(row.get("layout_z") or 0.0),
        float(row.get("layout_x") or 0.0),
    )


def _canvas_width_for_rows(vrows: list[dict[str, Any]]) -> float:
    wmax = 0.0
    for r in vrows:
        if not has_layout_bbox(r):
            continue
        x = float(r.get("layout_x") or 0)
        w = float(r.get("layout_w") or 0)
        wmax = max(wmax, x + w)
    return wmax if wmax > 1.0 else 1200.0


def _bbox_to_grid_columns(row: dict[str, Any], canvas_w: float) -> tuple[int, int, int]:
    """Map PBI x/width/height to Lakeview grid (6 columns wide). Returns (x, width, height)."""
    x = float(row.get("layout_x") or 0)
    w = float(row.get("layout_w") or canvas_w / 2)
    h = float(row.get("layout_h") or 120)
    canvas_w = max(canvas_w, 1.0)
    xg = int((x / canvas_w) * 6)
    xg = max(0, min(5, xg))
    wg = int((w / canvas_w) * 6 + 0.45)
    wg = max(1, min(6 - xg, wg))
    hg = max(2, min(12, int(h / 110) + 1))
    return xg, wg, hg


def _table_widget_payload(
    *,
    name: str,
    dataset_name: str,
    column_names: list[str],
) -> dict[str, Any]:
    cols = [c for c in column_names if c and not str(c).startswith("#")][:48]
    fields: list[dict[str, str]] = []
    enc_cols: list[dict[str, str]] = []
    for c in cols:
        safe = str(c).replace("`", "``")
        fields.append({"name": c, "expression": f"`{safe}`"})
        enc_cols.append({"fieldName": c, "displayName": c})
    return {
        "name": name,
        "queries": [
            {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": fields,
                    "disaggregated": True,
                },
            }
        ],
        "spec": {
            "version": 2,
            "widgetType": "table",
            "encodings": {"columns": enc_cols},
            "frame": {"title": "Data preview (primary binding)", "showTitle": True},
        },
    }


def _intent_multiline_widget(name: str, vt: str, intent: str, src: str) -> dict[str, Any]:
    lines = [f"**Visual type:** `{_safe_display_line(vt)}`", ""]
    lines.append(intent if intent else "_No intent text extracted for this visual._")
    if src:
        lines.extend(["", f"_Source file: {src}_"])
    return {"name": name, "multilineTextboxSpec": {"lines": lines}}


def _chart_placeholder_widget(name: str, vt: str, intent: str, src: str) -> dict[str, Any]:
    lines = [
        f"**Chart placeholder** (`{_safe_display_line(vt)}`)",
        "",
        "Not enough columns on the primary bound table to build a chart (need at least two), "
        "or the visual type is not mapped yet.",
        "",
        intent[:600] + ("…" if len(intent) > 600 else "") if intent else "_No intent extracted._",
    ]
    if src:
        lines.extend(["", f"_Source file: {src}_"])
    return {"name": name, "multilineTextboxSpec": {"lines": lines}}


def _field_pair(col_x: str, col_y: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for c in (col_x, col_y):
        safe = str(c).replace("`", "``")
        out.append({"name": c, "expression": f"`{safe}`"})
    return out


def _chart_frame_title(intent: str, vt: str) -> str:
    t = (intent or "").strip()
    if len(t) > 100:
        t = t[:97] + "…"
    return t or _safe_display_line(vt) or "Chart"


def _bar_chart_widget_payload(
    *,
    name: str,
    dataset_name: str,
    col_x: str,
    col_y: str,
    frame_title: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "queries": [
            {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": _field_pair(col_x, col_y),
                    "disaggregated": True,
                },
            }
        ],
        "spec": {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {
                    "fieldName": col_x,
                    "scale": {"type": "categorical"},
                    "displayName": col_x,
                },
                "y": {
                    "fieldName": col_y,
                    "scale": {"type": "quantitative"},
                    "displayName": col_y,
                },
            },
            "frame": {"title": frame_title[:120], "showTitle": True},
        },
    }


def _line_chart_widget_payload(
    *,
    name: str,
    dataset_name: str,
    col_x: str,
    col_y: str,
    frame_title: str,
) -> dict[str, Any]:
    x_scale = x_scale_type_for_line_axis(col_x)
    return {
        "name": name,
        "queries": [
            {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": _field_pair(col_x, col_y),
                    "disaggregated": True,
                },
            }
        ],
        "spec": {
            "version": 3,
            "widgetType": "line",
            "encodings": {
                "x": {
                    "fieldName": col_x,
                    "scale": {"type": x_scale},
                    "displayName": col_x,
                },
                "y": {
                    "fieldName": col_y,
                    "scale": {"type": "quantitative"},
                    "displayName": col_y,
                },
            },
            "frame": {"title": frame_title[:120], "showTitle": True},
        },
    }


def _pie_chart_widget_payload(
    *,
    name: str,
    dataset_name: str,
    col_color: str,
    col_angle: str,
    frame_title: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "queries": [
            {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": _field_pair(col_color, col_angle),
                    "disaggregated": True,
                },
            }
        ],
        "spec": {
            "version": 3,
            "widgetType": "pie",
            "encodings": {
                "color": {
                    "fieldName": col_color,
                    "scale": {"type": "categorical"},
                    "displayName": col_color,
                },
                "angle": {
                    "fieldName": col_angle,
                    "scale": {"type": "quantitative"},
                    "displayName": col_angle,
                },
            },
            "frame": {"title": frame_title[:120], "showTitle": True},
        },
    }


def _group_visual_rows(
    visual_rows: list[dict[str, Any]],
) -> list[tuple[tuple[str, str], list[dict[str, Any]]]]:
    """Preserve first-seen order of (report_name, page_name) keys."""
    order: list[tuple[str, str]] = []
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in visual_rows:
        rpt = (row.get("report_name") or "Report").strip() or "Report"
        pg = (row.get("page_name") or "Page").strip() or "Page"
        key = (rpt, pg)
        if key not in buckets:
            order.append(key)
        buckets[key].append(row)
    return [(k, buckets[k]) for k in order]


def build_migrated_dashboard_with_pbi_views(
    *,
    dashboard_title: str,
    subtitle: str,
    dataset_display_name: str,
    dataset_name: str,
    starter_sql: str,
    column_names: list[str],
    visual_rows: list[dict[str, Any]] | None,
    max_extra_pages: int = DEFAULT_MAX_EXTRA_PAGES,
    max_visuals_per_page: int = DEFAULT_MAX_VISUALS_PER_PAGE,
) -> dict[str, Any]:
    """
    Overview page (data preview) plus one Lakeview page per Power BI **report page** (grouped views),
    each visual rendered from intent, table preview, or **primary-dataset** bar/line/pie charts when
    at least two columns exist (first column × second column heuristic). Per-visual SQL is not generated yet.
    """
    base = build_minimal_migrated_dashboard(
        dashboard_title=dashboard_title,
        subtitle=subtitle,
        dataset_display_name=dataset_display_name,
        dataset_name=dataset_name,
        starter_sql=starter_sql,
        column_names=column_names,
    )
    rows = [r for r in (visual_rows or []) if isinstance(r, dict)]
    if not rows:
        return base

    groups = _group_visual_rows(rows)[:max_extra_pages]
    used_slugs: set[str] = {"overview"}
    extra_pages: list[dict[str, Any]] = []

    for (rpt, pg), vrows in groups:
        slug = _unique_page_slug(rpt, pg, used_slugs)
        disp = f"{_safe_display_line(rpt)} — {_safe_display_line(pg)}"
        layout: list[dict[str, Any]] = []
        y = 0
        layout.append(
            {
                "widget": {
                    "name": _lakeview_widget_name(slug, 0),
                    "multilineTextboxSpec": {
                        "lines": [
                            f"## {_safe_display_line(rpt)}",
                            f"### {_safe_display_line(pg)}",
                            "Each block below is one Power BI visual from this page (intent for AI/BI rebuild).",
                        ]
                    },
                },
                "position": {"x": 0, "y": y, "width": 6, "height": 2},
            }
        )
        y += 2
        page_rows = vrows[:max_visuals_per_page]
        sorted_rows = sorted(page_rows, key=_row_sort_key)
        canvas_w = _canvas_width_for_rows(page_rows)

        for i, row in enumerate(sorted_rows, start=1):
            vt = (row.get("visual_type") or "visual").strip()
            intent = (row.get("intent_statement") or "").strip()
            if len(intent) > 900:
                intent = intent[:897] + "…"
            src = (row.get("source_file") or "").strip()
            wname = _lakeview_widget_name(slug, i)
            target = parity_target_for_visual_type(vt)

            if has_layout_bbox(row):
                xg, wg, hg = _bbox_to_grid_columns(row, canvas_w)
            else:
                xg, wg, hg = 0, 6, 4 if len(intent) > 280 else 3

            if target == ParityGapTarget.lakeview_table_preview:
                layout.append(
                    {
                        "widget": _table_widget_payload(
                            name=wname,
                            dataset_name=dataset_name,
                            column_names=column_names,
                        ),
                        "position": {"x": xg, "y": y, "width": wg, "height": hg},
                    }
                )
            elif target == ParityGapTarget.lakeview_chart_placeholder:
                ch = max(hg, 4)
                cols_trim = _trim_sql_columns(column_names)
                title = _chart_frame_title(intent, vt)
                if len(cols_trim) >= 2:
                    c0, c1 = cols_trim[0], cols_trim[1]
                    if is_pie_like_visual(vt):
                        wj = _pie_chart_widget_payload(
                            name=wname,
                            dataset_name=dataset_name,
                            col_color=c0,
                            col_angle=c1,
                            frame_title=title,
                        )
                    elif is_line_like_visual(vt):
                        wj = _line_chart_widget_payload(
                            name=wname,
                            dataset_name=dataset_name,
                            col_x=c0,
                            col_y=c1,
                            frame_title=title,
                        )
                    else:
                        wj = _bar_chart_widget_payload(
                            name=wname,
                            dataset_name=dataset_name,
                            col_x=c0,
                            col_y=c1,
                            frame_title=title,
                        )
                    layout.append(
                        {
                            "widget": wj,
                            "position": {"x": xg, "y": y, "width": wg, "height": max(ch, 5)},
                        }
                    )
                    y += max(ch, 5)
                    continue
                layout.append(
                    {
                        "widget": _chart_placeholder_widget(wname, vt, intent, src),
                        "position": {"x": xg, "y": y, "width": wg, "height": ch},
                    }
                )
                y += ch
                continue
            else:
                layout.append(
                    {
                        "widget": _intent_multiline_widget(wname, vt, intent, src),
                        "position": {"x": xg, "y": y, "width": wg, "height": hg},
                    }
                )
            y += hg

        extra_pages.append(
            {
                "name": slug,
                "displayName": disp[:120],
                "pageType": "PAGE_TYPE_CANVAS",
                "layout": layout,
            }
        )

    base["pages"] = list(base.get("pages", [])) + extra_pages
    return base


def build_minimal_migrated_dashboard(
    *,
    dashboard_title: str,
    subtitle: str,
    dataset_display_name: str,
    dataset_name: str,
    starter_sql: str,
    column_names: list[str],
) -> dict[str, Any]:
    """
    Single-page AI/BI dashboard: title + subtitle + full-width table on validated starter SQL.
    column_names must match the SELECT list in ``starter_sql`` (typically from DESCRIBE TABLE).
    """
    cols = [c for c in column_names if c and not str(c).startswith("#")][:48]
    if not cols:
        raise ValueError("column_names must be non-empty; run DESCRIBE TABLE on the UC binding first.")

    fields: list[dict[str, str]] = []
    enc_cols: list[dict[str, str]] = []
    for c in cols:
        safe = str(c).replace("`", "``")
        fields.append({"name": c, "expression": f"`{safe}`"})
        enc_cols.append({"fieldName": c, "displayName": c})

    query_lines = _sql_to_query_lines(starter_sql)

    return {
        "datasets": [
            {
                "name": dataset_name,
                "displayName": dataset_display_name,
                "queryLines": query_lines,
            }
        ],
        "pages": [
            {
                "name": "overview",
                "displayName": "Overview",
                "pageType": "PAGE_TYPE_CANVAS",
                "layout": [
                    {
                        "widget": {
                            "name": "dash-title",
                            "multilineTextboxSpec": {"lines": [f"## {dashboard_title}"]},
                        },
                        "position": {"x": 0, "y": 0, "width": 6, "height": 1},
                    },
                    {
                        "widget": {
                            "name": "dash-subtitle",
                            "multilineTextboxSpec": {"lines": [subtitle]},
                        },
                        "position": {"x": 0, "y": 1, "width": 6, "height": 1},
                    },
                    {
                        "widget": {
                            "name": "primary-table",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": dataset_name,
                                        "fields": fields,
                                        "disaggregated": True,
                                    },
                                }
                            ],
                            "spec": {
                                "version": 2,
                                "widgetType": "table",
                                "encodings": {"columns": enc_cols},
                                "frame": {"title": "Data preview", "showTitle": True},
                            },
                        },
                        "position": {"x": 0, "y": 2, "width": 6, "height": 8},
                    },
                ],
            }
        ],
    }
