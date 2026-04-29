"""Minimal Lakeview (AI/BI) dashboard JSON for post-migration deploy."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

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
    each visual rendered as an intent card (type + extracted intent). No per-visual SQL yet (UC-bound
    starter dataset powers the overview table).
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
        for i, row in enumerate(vrows[:max_visuals_per_page], start=1):
            vt = (row.get("visual_type") or "visual").strip()
            intent = (row.get("intent_statement") or "").strip()
            if len(intent) > 900:
                intent = intent[:897] + "…"
            src = (row.get("source_file") or "").strip()
            lines = [f"**Visual type:** `{_safe_display_line(vt)}`", ""]
            lines.append(intent if intent else "_No intent text extracted for this visual._")
            if src:
                lines.extend(["", f"_Source file: {src}_"])
            h = 4 if len(intent) > 280 else 3
            layout.append(
                {
                    "widget": {
                        "name": _lakeview_widget_name(slug, i),
                        "multilineTextboxSpec": {"lines": lines},
                    },
                    "position": {"x": 0, "y": y, "width": 6, "height": h},
                }
            )
            y += h

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
