import json

from aibi_migrator.canonical.models import ParityGapTarget, estate_rollup_from_results
from aibi_migrator.classify.classifier import classify_pbit
from aibi_migrator.parity_manifest import build_deploy_parity_manifest


def test_build_deploy_parity_manifest_all_mapped() -> None:
    rows = [
        {
            "visual_id": "a",
            "source_file": "x.pbit",
            "report_name": "R1",
            "page_name": "P1",
            "visual_type": "card",
            "intent_statement": "kpi",
        },
        {
            "visual_id": "b",
            "source_file": "x.pbit",
            "report_name": "R1",
            "page_name": "P2",
            "visual_type": "table",
            "intent_statement": "rows",
        },
    ]
    m = build_deploy_parity_manifest(dashboard_display_name="Dash", visual_rows=rows)
    assert m.dashboard_display_name == "Dash"
    assert len(m.entries) == 2
    assert m.entries[0].target == ParityGapTarget.lakeview_multiline_intent
    assert m.entries[1].target == ParityGapTarget.lakeview_table_preview
    assert m.entries[0].lakeview_widget_name
    lines = m.backlog_lines()
    assert any("lakeview_multiline_intent" in ln for ln in lines)
    assert any("lakeview_table_preview" in ln for ln in lines)
    json.loads(m.model_dump_json())


def test_build_deploy_parity_manifest_visual_cap_gap() -> None:
    rows = []
    for i in range(3):
        rows.append(
            {
                "visual_id": f"v{i}",
                "source_file": "x.pbit",
                "report_name": "R",
                "page_name": "P",
                "visual_type": "card",
                "intent_statement": str(i),
            }
        )
    m = build_deploy_parity_manifest(
        dashboard_display_name="D",
        visual_rows=rows,
        max_extra_pages=28,
        max_visuals_per_page=2,
    )
    assert len(m.entries) == 3
    gaps = [e for e in m.entries if e.target == ParityGapTarget.gap]
    assert len(gaps) == 1
    assert "max_visuals_per_page" in (gaps[0].gap_reason or "")


def test_estate_rollup_merges_parity_backlog() -> None:
    from aibi_migrator.canonical.models import (
        ArtifactDisposition,
        ArtifactType,
        DispositionScores,
        MigrationArtifactResult,
    )

    r = MigrationArtifactResult(
        artifact_id="semantic_model:t.pbit",
        artifact_type=ArtifactType.semantic_model,
        recommended_disposition=ArtifactDisposition.migrate_now,
        effective_disposition=ArtifactDisposition.migrate_now,
        scores=DispositionScores(
            semantic_complexity=10,
            lakehouse_affinity=60,
            front_end_dependency=20,
            real_time_value=30,
            ai_leverage=40,
            user_change_tolerance=50,
        ),
    )
    er = estate_rollup_from_results(
        ["t.pbit"],
        [r],
        parity_backlog_lines=["[deploy:x] extra line"],
    )
    assert any("[deploy:x]" in x for x in er.parity_validation_backlog)


def test_parity_manifest_resolves_bar_with_two_columns() -> None:
    rows = [
        {
            "visual_id": "c1",
            "source_file": "x.pbit",
            "report_name": "R",
            "page_name": "P",
            "visual_type": "columnChart",
            "intent_statement": "trend",
        }
    ]
    m = build_deploy_parity_manifest(
        dashboard_display_name="D",
        visual_rows=rows,
        column_names=["a", "b"],
    )
    assert m.entries[0].target.value == "lakeview_bar_chart"


def test_parity_manifest_chart_placeholder_with_one_column() -> None:
    rows = [
        {
            "visual_id": "c1",
            "report_name": "R",
            "page_name": "P",
            "visual_type": "barChart",
            "intent_statement": "x",
        }
    ]
    m = build_deploy_parity_manifest(
        dashboard_display_name="D",
        visual_rows=rows,
        column_names=["only_one"],
    )
    assert m.entries[0].target == ParityGapTarget.lakeview_chart_placeholder


def test_classifier_rebuild_plan_mentions_fidelity_manifest() -> None:
    from aibi_migrator.canonical.models import PbitCanonicalModel

    m = PbitCanonicalModel(source_file_name="f.pbit", visuals=[])
    res = classify_pbit(m)
    joined = " ".join(res.dashboard_rebuild_plan)
    assert "parity manifest" in joined.lower()
    assert "tile-for-tile" not in joined.lower()
