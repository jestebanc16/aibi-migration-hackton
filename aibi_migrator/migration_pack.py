"""UC-safe SQL and migration pack JSON for PBI → AI/BI (v1)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from aibi_migrator.lakeview.minimal import build_placeholder_dashboard_spec


def sanitize_lakeview_display_name(name: str | None) -> str:
    """
    Lakeview rejects some characters in dashboard ``display_name`` (e.g. ``/`` in \"AI/BI\").
    """
    s = (name or "").strip()
    s = s.replace("/", "-").replace("\\", "-")
    s = " ".join(s.split())
    return s if s else "Migrated dashboard"


def parse_uc_fqtn(fq: str) -> tuple[str, str, str] | None:
    """Parse catalog.schema.table (or view). Returns None if invalid."""
    parts = (fq or "").strip().split(".")
    if len(parts) != 3 or not all(p.strip() for p in parts):
        return None
    return parts[0].strip(), parts[1].strip(), parts[2].strip()


def quote_uc_fqtn(fq: str) -> str:
    """Backtick-quote each UC segment for use in SQL."""
    t = parse_uc_fqtn(fq)
    if not t:
        raise ValueError(f"Not a valid catalog.schema.table: {fq!r}")
    return ".".join(f"`{c.replace('`', '``')}`" for c in t)


def validation_probe_sql(fq: str) -> str:
    """Minimal read against bound object (table or view)."""
    return f"SELECT * FROM {quote_uc_fqtn(fq)} LIMIT 1"


def limited_select_sql(fqtn: str, columns: list[str], limit: int = 500) -> str:
    """Explicit-column SELECT for Lakeview datasets (field names must match columns)."""
    qtbl = quote_uc_fqtn(fqtn)
    lim = max(1, min(int(limit), 5000))
    parts = [f"`{str(c).replace('`', '``')}`" for c in columns if c and not str(c).startswith("#")]
    if not parts:
        raise ValueError("columns must be non-empty")
    return f"SELECT {', '.join(parts)} FROM {qtbl} LIMIT {lim}"


def first_bound_fqtn(bindings: dict[str, str], ordered_table_names: list[str]) -> str | None:
    """First model table that has a valid three-part UC binding."""
    for t in ordered_table_names:
        fq = (bindings.get(t) or "").strip()
        if fq and parse_uc_fqtn(fq):
            return fq
    return None


def primary_dataset_sql(bindings: dict[str, str], ordered_table_names: list[str], limit: int = 500) -> str:
    """First fully bound model table → starter dataset SQL for AI/BI."""
    lim = max(1, min(int(limit), 5000))
    fq = first_bound_fqtn(bindings, ordered_table_names)
    if not fq:
        return "SELECT 1 AS placeholder_no_valid_bindings"
    try:
        return f"SELECT * FROM {quote_uc_fqtn(fq)} LIMIT {lim}"
    except ValueError:
        return "SELECT 1 AS placeholder_no_valid_bindings"


def build_migration_pack(
    *,
    dashboard_name: str,
    warehouse_id: str,
    bindings: dict[str, str],
    ordered_model_tables: list[str],
    source_files: list[str],
    analysis_snapshots: list[dict[str, Any]],
    validation_results: list[dict[str, Any]] | None,
    parent_path: str = "/Workspace/Users",
) -> dict[str, Any]:
    """Single JSON artifact: bindings, starter SQL, Lakeview placeholder, validation summary."""
    dataset_sql = primary_dataset_sql(bindings, ordered_model_tables)
    probes: dict[str, str] = {}
    for t in ordered_model_tables:
        fq = (bindings.get(t) or "").strip()
        if fq and parse_uc_fqtn(fq):
            try:
                probes[t] = validation_probe_sql(fq)
            except ValueError:
                probes[t] = ""

    spec = build_placeholder_dashboard_spec(
        display_name=dashboard_name,
        warehouse_id=warehouse_id,
        dataset_sql=dataset_sql,
        parent_path=parent_path,
    )

    return {
        "schema_version": "migration_pack.1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dashboard_display_name": dashboard_name,
        "warehouse_id": warehouse_id,
        "source_files": source_files,
        "table_bindings": dict(bindings),
        "validation_probe_sql_by_pbi_table": probes,
        "starter_dataset_sql": dataset_sql,
        "lakeview_placeholder": spec,
        "analysis_per_file": analysis_snapshots,
        "last_sql_validation": validation_results or [],
        "next_steps": [
            "In Databricks SQL / AI/BI: confirm starter_dataset_sql runs on the selected warehouse.",
            "Create an AI/BI dashboard, add a dataset on this warehouse, and use the starter SQL (tune joins and metrics).",
            "lakeview_placeholder is a planning skeleton until full serialized_dashboard generation is implemented.",
        ],
    }
