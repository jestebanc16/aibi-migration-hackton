"""Build deploy-time parity / gap manifest (RULES.md fidelity track)."""

from __future__ import annotations

from typing import Any

from aibi_migrator.canonical.models import (
    DeployParityManifest,
    ParityGapEntry,
    ParityGapTarget,
)
from aibi_migrator.lakeview.dashboard_builder import (
    DEFAULT_MAX_EXTRA_PAGES,
    DEFAULT_MAX_VISUALS_PER_PAGE,
    _group_visual_rows,
    _lakeview_widget_name,
    _unique_page_slug,
)


def _norm_report_page(row: dict[str, Any]) -> tuple[str, str]:
    rpt = (row.get("report_name") or "Report").strip() or "Report"
    pg = (row.get("page_name") or "Page").strip() or "Page"
    return rpt, pg


def _visual_id_for(row: dict[str, Any], rpt: str, pg: str, index: int) -> str:
    vid = (row.get("visual_id") or "").strip()
    if vid:
        return vid
    sf = (row.get("source_file") or "").strip() or "unknown"
    return f"{sf}:{rpt}:{pg}#{index}"


def build_deploy_parity_manifest(
    *,
    dashboard_display_name: str,
    visual_rows: list[dict[str, Any]] | None,
    max_extra_pages: int = DEFAULT_MAX_EXTRA_PAGES,
    max_visuals_per_page: int = DEFAULT_MAX_VISUALS_PER_PAGE,
) -> DeployParityManifest:
    """
    One ParityGapEntry per source visual row. Mirrors Lakeview builder caps so
    ``lakeview_multiline_intent`` rows match widgets that were actually emitted.
    """
    rows = [r for r in (visual_rows or []) if isinstance(r, dict)]
    if not rows:
        return DeployParityManifest(
            dashboard_display_name=dashboard_display_name,
            entries=[],
            manifest_warnings=["No visuals in session; parity manifest has no per-visual rows."],
        )

    all_groups = _group_visual_rows(rows)
    included_groups = all_groups[:max_extra_pages]
    included_keys = {k for k, _ in included_groups}

    used_slugs: set[str] = {"overview"}
    slug_by_key: dict[tuple[str, str], str] = {}
    for key, _ in included_groups:
        rpt, pg = key
        slug_by_key[key] = _unique_page_slug(rpt, pg, used_slugs)

    entries: list[ParityGapEntry] = []
    warnings: list[str] = []

    omitted_pages = len(all_groups) - len(included_groups)
    if omitted_pages > 0:
        warnings.append(
            f"{omitted_pages} report page(s) omitted from Lakeview: exceeded max_extra_pages ({max_extra_pages})."
        )

    for key, vrows in all_groups:
        rpt, pg = key
        if key not in included_keys:
            for i, row in enumerate(vrows):
                entries.append(
                    ParityGapEntry(
                        source_file=(row.get("source_file") or None),
                        report_name=rpt,
                        page_name=pg,
                        visual_id=_visual_id_for(row, rpt, pg, i),
                        visual_type=(row.get("visual_type") or None),
                        target=ParityGapTarget.gap,
                        lakeview_widget_name=None,
                        gap_reason=f"Report page not deployed: exceeded max_extra_pages ({max_extra_pages}).",
                    )
                )
            continue

        slug = slug_by_key[key]
        truncated = max(0, len(vrows) - max_visuals_per_page)
        if truncated:
            warnings.append(
                f"{rpt} / {pg}: {truncated} visual(s) omitted from Lakeview: exceeded max_visuals_per_page ({max_visuals_per_page})."
            )

        for i, row in enumerate(vrows):
            if i >= max_visuals_per_page:
                entries.append(
                    ParityGapEntry(
                        source_file=(row.get("source_file") or None),
                        report_name=rpt,
                        page_name=pg,
                        visual_id=_visual_id_for(row, rpt, pg, i),
                        visual_type=(row.get("visual_type") or None),
                        target=ParityGapTarget.gap,
                        lakeview_widget_name=None,
                        gap_reason=f"Visual not placed on Lakeview page: exceeded max_visuals_per_page ({max_visuals_per_page}).",
                    )
                )
            else:
                # Lakeview builder uses widget index 1..n for visuals (0 is page header).
                wname = _lakeview_widget_name(slug, i + 1)
                entries.append(
                    ParityGapEntry(
                        source_file=(row.get("source_file") or None),
                        report_name=rpt,
                        page_name=pg,
                        visual_id=_visual_id_for(row, rpt, pg, i),
                        visual_type=(row.get("visual_type") or None),
                        target=ParityGapTarget.lakeview_multiline_intent,
                        lakeview_widget_name=wname,
                        gap_reason=None,
                    )
                )

    return DeployParityManifest(
        dashboard_display_name=dashboard_display_name,
        entries=entries,
        manifest_warnings=warnings,
    )
