"""Canonical models and RULES-aligned artifact output contract (schema v1)."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


SCHEMA_VERSION = "1.0"


class ArtifactDisposition(str, Enum):
    migrate_now = "migrate_now"
    migrate_later = "migrate_later"
    retain_in_power_bi = "retain_in_power_bi"
    redesign_before_migration = "redesign_before_migration"


class ArtifactType(str, Enum):
    dashboard = "dashboard"
    report = "report"
    semantic_model = "semantic_model"
    measure = "measure"
    workspace = "workspace"


class DaxBucket(str, Enum):
    direct_sql_candidate = "direct_sql_candidate"
    metric_view_candidate = "metric_view_candidate"
    requires_semantic_remodel = "requires_semantic_remodel"
    presentation_only = "presentation_only"
    not_viable_without_redesign = "not_viable_without_redesign"


class DispositionScores(BaseModel):
    semantic_complexity: int = Field(ge=0, le=100)
    lakehouse_affinity: int = Field(ge=0, le=100)
    front_end_dependency: int = Field(ge=0, le=100)
    real_time_value: int = Field(ge=0, le=100)
    ai_leverage: int = Field(ge=0, le=100)
    user_change_tolerance: int = Field(ge=0, le=100)


class PbitSemanticRelationship(BaseModel):
    """A relationship edge from the semantic model (DataModelSchema)."""

    name: str | None = None
    from_table: str | None = None
    from_column: str | None = None
    to_table: str | None = None
    to_column: str | None = None
    is_active: bool = True
    cross_filtering_behavior: str | None = None


class PbitTableRef(BaseModel):
    """A logical table from the Power BI model."""

    name: str
    is_hidden: bool = False
    column_names: list[str] = Field(default_factory=list)
    measure_names: list[str] = Field(default_factory=list)
    #: ``data_table`` = import/M/direct storage; ``calculated_view`` = calculated table (semantic “view”).
    semantic_role: str = Field(default="data_table", description="data_table | calculated_view | unknown")


class PbitMeasureRef(BaseModel):
    name: str
    dax_expression: str | None = None
    dax_bucket: DaxBucket | None = None


class PbitVisualIntent(BaseModel):
    visual_id: str
    report_name: str | None = None
    page_name: str | None = None
    visual_type: str | None = None
    intent_statement: str
    intent_user_edited: bool = False
    # Power BI report layout (canvas units); set when Layout JSON is parsed.
    layout_x: float | None = None
    layout_y: float | None = None
    layout_z: float | None = None
    layout_w: float | None = None
    layout_h: float | None = None


class ParityGapTarget(str, Enum):
    """How a source visual maps to the deployed Lakeview JSON (fidelity track)."""

    lakeview_multiline_intent = "lakeview_multiline_intent"
    lakeview_table_preview = "lakeview_table_preview"
    lakeview_chart_placeholder = "lakeview_chart_placeholder"
    lakeview_bar_chart = "lakeview_bar_chart"
    lakeview_line_chart = "lakeview_line_chart"
    lakeview_pie_chart = "lakeview_pie_chart"
    gap = "gap"


class ParityGapEntry(BaseModel):
    """One source visual row in the deploy parity / gap manifest (RULES.md)."""

    source_file: str | None = None
    report_name: str = Field(default="Report")
    page_name: str = Field(default="Page")
    visual_id: str
    visual_type: str | None = None
    target: ParityGapTarget
    lakeview_widget_name: str | None = None
    gap_reason: str | None = None


class DeployParityManifest(BaseModel):
    """Per-deploy parity manifest: every extracted visual is listed as mapped or GAP."""

    schema_version: str = SCHEMA_VERSION
    dashboard_display_name: str
    overview_note: str = Field(
        default="Overview page uses the primary UC-bound dataset (data preview); not repeated per visual here."
    )
    entries: list[ParityGapEntry] = Field(default_factory=list)
    manifest_warnings: list[str] = Field(default_factory=list)

    def backlog_lines(self) -> list[str]:
        """One-line strings for EstateRollup.parity_validation_backlog."""
        lines: list[str] = []
        for e in self.entries:
            loc = f"{e.report_name}/{e.page_name}"
            if e.target == ParityGapTarget.gap:
                lines.append(f"[deploy:{e.visual_id}] {loc}: GAP — {e.gap_reason or 'unknown'}")
            else:
                wn = e.lakeview_widget_name or "?"
                lines.append(f"[deploy:{e.visual_id}] {loc}: {e.target.value} → `{wn}`")
        for w in self.manifest_warnings:
            lines.append(f"[deploy:manifest] {w}")
        return lines

    def summary_counts(self) -> dict[str, int]:
        gaps = sum(1 for e in self.entries if e.target == ParityGapTarget.gap)
        mapped = len(self.entries) - gaps
        by_target: dict[str, int] = {}
        for e in self.entries:
            k = e.target.value
            by_target[k] = by_target.get(k, 0) + 1
        out: dict[str, int] = {"mapped": mapped, "gap": gaps, "total": len(self.entries)}
        out.update(by_target)
        return out


class PbitCanonicalModel(BaseModel):
    """Intermediate representation after .pbit extraction."""

    schema_version: str = SCHEMA_VERSION
    source_file_name: str
    tables: list[PbitTableRef] = Field(default_factory=list)
    measures: list[PbitMeasureRef] = Field(default_factory=list)
    semantic_relationships: list[PbitSemanticRelationship] = Field(default_factory=list)
    relationships_count: int = 0
    has_rls_hints: bool = False
    rls_notes: list[str] = Field(default_factory=list)
    visuals: list[PbitVisualIntent] = Field(default_factory=list)
    raw_datamodel_present: bool = False
    extraction_warnings: list[str] = Field(default_factory=list)


class MigrationArtifactResult(BaseModel):
    """Per-artifact output aligned with RULES.md output contract."""

    artifact_id: str
    artifact_type: ArtifactType
    recommended_disposition: ArtifactDisposition
    effective_disposition: ArtifactDisposition
    binding_blockers: list[str] = Field(default_factory=list)
    scores: DispositionScores
    semantic_translation_plan: list[str] = Field(default_factory=list)
    dashboard_rebuild_plan: list[str] = Field(default_factory=list)
    validation_plan: list[str] = Field(default_factory=list)
    open_risks: list[str] = Field(default_factory=list)

    def model_dump_json_pretty(self) -> str:
        return self.model_dump_json(indent=2)


class EstateRollup(BaseModel):
    """Session-level summary when multiple .pbit files are analyzed."""

    schema_version: str = SCHEMA_VERSION
    source_files: list[str] = Field(default_factory=list)
    migration_wave_suggestion: list[str] = Field(default_factory=list)
    retained_register_summary: dict[str, int] = Field(default_factory=dict)
    consolidation_hints: list[str] = Field(default_factory=list)
    risk_register: list[str] = Field(default_factory=list)
    parity_validation_backlog: list[str] = Field(default_factory=list)

    def model_dump_json_pretty(self) -> str:
        return self.model_dump_json(indent=2)


def estate_rollup_from_results(
    file_names: list[str],
    results: list[MigrationArtifactResult],
    *,
    parity_backlog_lines: list[str] | None = None,
) -> EstateRollup:
    from collections import Counter

    dispositions = Counter(r.effective_disposition.value for r in results)
    risks: list[str] = []
    for r in results:
        risks.extend([f"[{r.artifact_id}] {x}" for x in r.open_risks])
    backlog = [f"[{r.artifact_id}] Complete validation_plan items" for r in results]
    if parity_backlog_lines:
        backlog = backlog + list(parity_backlog_lines)
    waves = [
        "Wave 1 (suggested): artifacts with effective_disposition=migrate_now after binding",
        "Wave 2: migrate_later pending UC curation or DAX work",
        "Retained: retain_in_power_bi and redesign_before_migration per register",
    ]
    hints: list[str] = []
    if len(file_names) > 1:
        hints.append("Compare duplicate measure names across templates for metric view consolidation.")
    return EstateRollup(
        source_files=file_names,
        migration_wave_suggestion=waves,
        retained_register_summary=dict(dispositions),
        consolidation_hints=hints,
        risk_register=risks[:200],
        parity_validation_backlog=backlog,
    )


# Loose JSON type for future extensions
JsonDict = dict[str, Any]
