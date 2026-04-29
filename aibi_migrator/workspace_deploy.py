"""Create Lakeview (AI/BI) dashboard and Genie space in the workspace after SQL validation."""

from __future__ import annotations

import json
from typing import Any

from aibi_migrator.dbx_client.workspace import WorkspaceResources
from aibi_migrator.genie_copy import (
    build_genie_domain_description,
    build_genie_text_instruction,
    build_genie_title,
)
from aibi_migrator.genie_serialized import build_genie_serialized_space, suggest_genie_questions
from aibi_migrator.lakeview.dashboard_builder import build_migrated_dashboard_with_pbi_views
from aibi_migrator.llm_migrate import run_llm_migration, use_llm_migration_from_env
from aibi_migrator.migration_pack import (
    first_bound_fqtn,
    limited_select_sql,
    parse_uc_fqtn,
    sanitize_lakeview_display_name,
)
from aibi_migrator.parity_manifest import build_deploy_parity_manifest


def run_workspace_deploy(
    wr: WorkspaceResources,
    *,
    warehouse_id: str,
    parent_path: str,
    dashboard_display_name: str,
    bindings: dict[str, str],
    ordered_model_tables: list[str],
    measure_names: list[str],
    publish_lakeview: bool,
    create_genie: bool,
    source_filenames: list[str] | None = None,
    visual_rows: list[dict[str, Any]] | None = None,
    serving_endpoint_name: str | None = None,
    llm_temperature: float = 0.2,
    use_llm_migration: bool | None = None,
    canonical_models_json: str | None = None,
) -> dict[str, Any]:
    """
    Deploy Lakeview dashboard (optional publish) and optionally create a Genie space.
    Caller should ensure bindings were validated on ``warehouse_id`` first.

    ``source_filenames``: original .pbit names (used for Genie title).
    ``visual_rows``: per-visual metadata from the .pbit (report/page/type/intent) for dashboard pages + Genie context.

    When ``USE_LLM_MIGRATION`` is enabled (default), ``serving_endpoint_name`` and ``canonical_models_json``
    are required for generation; otherwise set ``use_llm_migration=False`` for heuristic-only assembly.
    """
    errs: list[str] = []
    use_llm = use_llm_migration_from_env() if use_llm_migration is None else bool(use_llm_migration)
    out: dict[str, Any] = {
        "ok": False,
        "errors": errs,
        "lakeview": None,
        "genie": None,
        "starter_sql": None,
        "parity_manifest": None,
        "parity_manifest_json": None,
        "parity_manifest_backlog_lines": [],
        "llm_assisted": use_llm,
        "llm_errors": [],
        "llm_gap_notes": [],
        "llm_raw_assistant": None,
    }

    fq_primary = first_bound_fqtn(bindings, ordered_model_tables)
    if not fq_primary:
        errs.append("No bound UC table found.")
        return out

    safe_display = sanitize_lakeview_display_name(dashboard_display_name)

    cols, cerr = wr.describe_table_columns(warehouse_id, fq_primary)
    if cerr:
        errs.append(f"DESCRIBE primary table {fq_primary}: {cerr}")
        parity = build_deploy_parity_manifest(
            dashboard_display_name=safe_display,
            visual_rows=visual_rows,
            column_names=None,
        )
        out["parity_manifest"] = parity.model_dump(mode="json")
        out["parity_manifest_json"] = parity.model_dump_json(indent=2)
        out["parity_manifest_backlog_lines"] = parity.backlog_lines()
        return out

    try:
        starter_sql = limited_select_sql(fq_primary, cols, limit=500)
    except ValueError as e:
        errs.append(str(e))
        parity = build_deploy_parity_manifest(
            dashboard_display_name=safe_display,
            visual_rows=visual_rows,
            column_names=cols,
        )
        out["parity_manifest"] = parity.model_dump(mode="json")
        out["parity_manifest_json"] = parity.model_dump_json(indent=2)
        out["parity_manifest_backlog_lines"] = parity.backlog_lines()
        return out
    out["starter_sql"] = starter_sql

    parity = build_deploy_parity_manifest(
        dashboard_display_name=safe_display,
        visual_rows=visual_rows,
        column_names=cols,
    )
    if use_llm:
        ep = (serving_endpoint_name or "").strip()
        note = f"LLM-assisted generation via serving endpoint `{ep}`." if ep else "LLM migration enabled."
        parity = parity.model_copy(update={"manifest_warnings": [*parity.manifest_warnings, note]})
    out["parity_manifest"] = parity.model_dump(mode="json")
    out["parity_manifest_json"] = parity.model_dump_json(indent=2)
    out["parity_manifest_backlog_lines"] = parity.backlog_lines()

    llm: dict[str, Any] | None = None
    if use_llm:
        ep = (serving_endpoint_name or "").strip()
        if not ep:
            errs.append("LLM migration is enabled but no chat serving endpoint was selected.")
            out["phase"] = "llm"
            return out
        cj = (canonical_models_json or "").strip()
        if not cj:
            errs.append("LLM migration requires canonical_models_json (serialized .pbit models).")
            out["phase"] = "llm"
            return out
        llm = run_llm_migration(
            wr,
            serving_endpoint_name=ep,
            dashboard_display_name=safe_display,
            bindings=dict(bindings),
            ordered_model_tables=list(ordered_model_tables),
            measure_names=list(measure_names),
            starter_sql=starter_sql,
            column_names=cols,
            visual_rows=visual_rows,
            canonical_models_json=cj,
            temperature=float(llm_temperature),
        )
        out["llm_raw_assistant"] = llm.get("raw_assistant")
        out["llm_gap_notes"] = list(llm.get("gap_notes") or [])
        if not llm.get("ok"):
            for e in llm.get("errors") or ["LLM generation failed."]:
                errs.append(f"LLM: {e}")
            out["llm_errors"] = list(llm.get("errors") or [])
            out["phase"] = "llm"
            return out
        dash_dict = llm["lakeview_dashboard"]
        assert isinstance(dash_dict, dict)
        serialized_dashboard = json.dumps(dash_dict)
    else:
        dash_dict = build_migrated_dashboard_with_pbi_views(
            dashboard_title=safe_display,
            subtitle="Created by Power BI → Databricks migrator from Unity Catalog bindings.",
            dataset_display_name="Primary bound table",
            dataset_name="primary",
            starter_sql=starter_sql,
            column_names=cols,
            visual_rows=visual_rows,
        )
        serialized_dashboard = json.dumps(dash_dict)

    dash_res: dict[str, Any] | None = None
    try:
        dash_res = wr.deploy_lakeview_dashboard(
            display_name=safe_display,
            parent_path=parent_path,
            warehouse_id=warehouse_id,
            serialized_dashboard=serialized_dashboard,
            publish=publish_lakeview,
        )
        out["lakeview"] = dash_res
        if not dash_res.get("ok"):
            errs.append(dash_res.get("error") or "Lakeview deploy failed")
    except Exception as e:  # noqa: BLE001
        errs.append(f"Lakeview: {e}")
        out["lakeview"] = {"ok": False, "error": str(e)}

    if create_genie:
        uc_list: list[str] = []
        bound_pairs: list[tuple[str, str]] = []
        for t in ordered_model_tables:
            fq = (bindings.get(t) or "").strip()
            if fq and parse_uc_fqtn(fq):
                bound_pairs.append((t, fq))
                if fq not in uc_list:
                    uc_list.append(fq)
        if not uc_list:
            errs.append("No UC tables for Genie.")
        else:
            questions: list[str] = []
            instr_body: str
            full_desc: str
            genie_title: str
            if use_llm and llm and llm.get("ok"):
                genie_title = str(llm.get("genie_title") or safe_display)[:120]
                full_desc = str(llm.get("genie_description") or "")[:4000]
                instr_body = str(llm.get("genie_text_instruction") or "")
                questions = list(llm.get("sample_questions") or [])
            else:
                questions = suggest_genie_questions(bound_tables=bound_pairs, measure_names=measure_names)
                src = list(source_filenames or [])
                intents: list[str] = []
                _seen_i: set[str] = set()
                for r in visual_rows or []:
                    if not isinstance(r, dict):
                        continue
                    t = (r.get("intent_statement") or "").strip()
                    if t and t not in _seen_i:
                        _seen_i.add(t)
                        intents.append(t)
                    if len(intents) >= 40:
                        break
                full_desc = build_genie_domain_description(
                    source_filenames=src,
                    pbi_to_uc=bound_pairs,
                    measure_names=measure_names,
                    intent_statements=intents,
                )
                instr_body = build_genie_text_instruction(full_desc)
                genie_title = build_genie_title(src, safe_display)
            if not questions:
                questions = suggest_genie_questions(bound_tables=bound_pairs, measure_names=measure_names)
            serialized_space = build_genie_serialized_space(
                table_identifiers=uc_list,
                sample_questions=questions,
                text_instruction=instr_body,
            )
            if len(genie_title) > 120:
                genie_title = genie_title[:117].rstrip() + "…"
            try:
                genie_res = wr.deploy_genie_space(
                    warehouse_id=warehouse_id,
                    serialized_space=serialized_space,
                    title=genie_title,
                    description=(full_desc or None),
                    parent_path=parent_path,
                )
                out["genie"] = genie_res
                if not genie_res.get("ok"):
                    errs.append(genie_res.get("error") or "Genie deploy failed")
            except Exception as e:  # noqa: BLE001
                errs.append(f"Genie: {e}")
                out["genie"] = {"ok": False, "error": str(e)}

    out["ok"] = len(errs) == 0
    return out
