"""Heuristic scoring and dispositions per RULES.md (v1)."""

from __future__ import annotations

import re

from aibi_migrator.canonical.models import (
    ArtifactDisposition,
    ArtifactType,
    DaxBucket,
    DispositionScores,
    MigrationArtifactResult,
    PbitCanonicalModel,
)


def classify_measure_dax(expression: str | None) -> DaxBucket:
    if not expression or not expression.strip():
        return DaxBucket.presentation_only
    text = expression.upper()
    if "CALCULATE" in text or "FILTER" in text or "ALL(" in text or "VALUES(" in text:
        return DaxBucket.requires_semantic_remodel
    if any(x in text for x in ("TOTALYTD", "SAMEPERIODLASTYEAR", "PARALLELPERIOD", "DATEADD")):
        return DaxBucket.metric_view_candidate
    if re.search(r"\b(SUM|AVERAGE|COUNT|MIN|MAX)\s*\(", text):
        return DaxBucket.direct_sql_candidate
    return DaxBucket.metric_view_candidate


def _scores_for_model(model: PbitCanonicalModel) -> DispositionScores:
    n_measures = len(model.measures)
    n_tables = len(model.tables)
    n_visuals = len(model.visuals)
    rel_density = min(100, model.relationships_count * 5)
    semantic = min(100, n_measures * 3 + rel_density + (10 if model.has_rls_hints else 0))
    lakehouse = 40 + min(40, n_tables * 2)
    front_end = min(100, 15 + n_visuals * 2)
    realtime = 35
    ai = 45
    tolerance = 50
    return DispositionScores(
        semantic_complexity=min(100, semantic),
        lakehouse_affinity=min(100, lakehouse),
        front_end_dependency=min(100, front_end),
        real_time_value=realtime,
        ai_leverage=ai,
        user_change_tolerance=tolerance,
    )


def classify_pbit(model: PbitCanonicalModel) -> MigrationArtifactResult:
    scores = _scores_for_model(model)
    reasons: list[str] = []
    if scores.semantic_complexity >= 70:
        rec: ArtifactDisposition = ArtifactDisposition.migrate_later
        reasons.append("High semantic complexity score; validate DAX and grain before Lakeview.")
    elif scores.front_end_dependency >= 70:
        rec = ArtifactDisposition.redesign_before_migration
        reasons.append(
            "Elevated front-end dependency; expect more Lakeview parity gaps (custom visuals, polish) "
            "while still attempting page/visual mapping and recording them in the parity manifest."
        )
    elif scores.lakehouse_affinity >= 55 and scores.semantic_complexity < 55:
        rec = ArtifactDisposition.migrate_now
        reasons.append("Moderate Lakehouse affinity with manageable semantic complexity.")
    else:
        rec = ArtifactDisposition.migrate_later
        reasons.append("Default conservative disposition pending UC binding and SQL validation.")

    translation = [
        "Map Power BI tables to curated Unity Catalog tables or views.",
        "Prefer governed metrics (metric views or shared SQL) for reusable KPIs.",
    ]
    rebuild = [
        "Fidelity track: map each Power BI report page to an AI/BI dashboard page and each visual to a target widget (Lakeview) or an explicit GAP in the parity manifest.",
        "Emit layout coordinates when extractable from Layout JSON; until then stack intent-backed multiline widgets and document limitations in the deploy parity manifest.",
        "Evolve toward counter, table, and chart widgets backed by validated SQL per visual; Overview retains the primary UC-bound dataset preview.",
    ]
    validation = [
        "Run representative SQL checks on the chosen SQL warehouse.",
        "RLS: document as unmapped in v1; no parity claim until UC row filters or secured views are defined.",
    ]
    risks = list(model.extraction_warnings)
    if model.has_rls_hints:
        risks.append("RLS roles detected; target security model must be designed in Unity Catalog.")
    risks.extend(reasons)

    artifact_id = f"semantic_model:{model.source_file_name}"
    return MigrationArtifactResult(
        artifact_id=artifact_id,
        artifact_type=ArtifactType.semantic_model,
        recommended_disposition=rec,
        effective_disposition=rec,
        binding_blockers=[],
        scores=scores,
        semantic_translation_plan=translation,
        dashboard_rebuild_plan=rebuild,
        validation_plan=validation,
        open_risks=risks,
    )


def effective_disposition(
    result: MigrationArtifactResult,
    table_bindings: dict[str, str],
    model_table_names: list[str],
) -> MigrationArtifactResult:
    """Recompute effective disposition after UC table binding (RULES)."""
    blockers: list[str] = []
    for t in model_table_names:
        mapped = table_bindings.get(t, "").strip()
        if not mapped or "." not in mapped:
            blockers.append(f"Table '{t}' is not mapped to a fully qualified catalog.schema.table (or view).")

    eff = result.recommended_disposition
    if blockers:
        if result.recommended_disposition == ArtifactDisposition.migrate_now:
            eff = ArtifactDisposition.migrate_later
        elif result.recommended_disposition == ArtifactDisposition.redesign_before_migration:
            eff = ArtifactDisposition.redesign_before_migration

    updated = result.model_copy(
        update={
            "effective_disposition": eff,
            "binding_blockers": blockers,
        }
    )
    return updated
