"""
Power BI (.pbit) → Databricks workspace migration (Streamlit).

Upload a Power BI **template** (.pbit), map semantic model tables to Unity Catalog, then create
an **AI/BI (Lakeview) dashboard** and a **Genie space** in this workspace.

Databricks Apps: identity and host come from the runtime (no sign-in UI here).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

_APP_ROOT = Path(__file__).resolve().parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

import streamlit as st

from aibi_migrator.canonical.models import PbitCanonicalModel, estate_rollup_from_results
from aibi_migrator.classify.classifier import classify_measure_dax, classify_pbit, effective_disposition
from aibi_migrator.dbx_client.workspace import WorkspaceResources
from aibi_migrator.migration_pack import build_migration_pack, parse_uc_fqtn, validation_probe_sql
from aibi_migrator.pbit_extract.parser import extract_pbit_canonical
from aibi_migrator.workspace_deploy import run_workspace_deploy


def _init_session() -> None:
    defaults = {
        "warehouse_id": "",
        "warehouse_pick_version": 0,
        "dashboard_name": "Migrated AI-BI Dashboard",
        "models": [],
        "results": [],
        "bindings": {},
        "migration_validation": None,
        "migration_pack_json": None,
        "deploy_parent_path": "",
        "deploy_parent_path_seeded": False,
        "deploy_publish_lakeview": True,
        "deploy_create_genie": True,
        "deploy_result": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _workspace_resources() -> WorkspaceResources:
    """One client per browser session (Databricks Apps user / default SDK auth)."""
    if st.session_state.get("_wr") is None:
        st.session_state._wr = WorkspaceResources()
    return st.session_state._wr


def _suggest_uc_table_name(pbi_table: str) -> str:
    """Sanitized Unity Catalog table name from a Power BI table name."""
    base = re.sub(r"[^0-9a-zA-Z_]+", "_", (pbi_table or "").strip()).strip("_").lower()
    if not base:
        return "mapped_table"
    if base[0].isdigit():
        return f"t_{base}"
    return base


def _run_binding_validation(
    wr: WorkspaceResources,
    warehouse_id: str,
    bindings: dict[str, str],
    all_tables: list[str],
) -> list[dict[str, Any]]:
    """Probe each UC binding on the warehouse (SELECT … LIMIT 1)."""
    out_rows: list[dict[str, Any]] = []
    for t in all_tables:
        fq = (bindings.get(t) or "").strip()
        try:
            sql = validation_probe_sql(fq)
        except ValueError as e:
            out_rows.append(
                {
                    "pbi_table": t,
                    "uc_fqtn": fq,
                    "state": None,
                    "error_message": str(e),
                    "sql": "",
                }
            )
            continue
        r = wr.execute_sql(warehouse_id, sql, include_preview=True, max_preview_rows=3)
        out_rows.append(
            {
                "pbi_table": t,
                "uc_fqtn": fq,
                "sql": sql,
                **r,
            }
        )
    return out_rows


def main() -> None:
    st.set_page_config(page_title="Power BI → AI/BI + Genie", layout="wide")
    _init_session()

    st.title("Power BI → Databricks AI/BI + Genie")
    st.markdown(
        "Upload a **Power BI template** (`.pbit` — the portable model/report package). "
        "Map each model table to **Unity Catalog**, then **create** an **AI/BI dashboard** and a **Genie space** "
        "in this workspace from that migration. "
        "*(`.pbix` is not supported here; export or save as `.pbit` from Power BI Desktop if needed.)*"
    )

    try:
        wr = _workspace_resources()
    except Exception as e:
        st.error(
            "Could not authenticate to Databricks from this app. "
            "Check Apps permissions and that the app runs with a user or service principal that can access the workspace."
        )
        st.caption(str(e))
        st.stop()

    st.subheader("SQL warehouse (runs dashboard & Genie queries)")
    whs, wh_err = wr.list_warehouses()
    if wh_err:
        st.warning(wh_err)
    if whs:
        labels = [f"{w.name}  —  {w.warehouse_id}" + (f" ({w.state})" if w.state else "") for w in whs]
        ids = [w.warehouse_id for w in whs]
        current = (st.session_state.warehouse_id or "").strip()
        try:
            default_idx = ids.index(current) if current in ids else 0
        except ValueError:
            default_idx = 0
        pick = st.selectbox(
            "Choose warehouse",
            range(len(labels)),
            index=default_idx,
            format_func=lambda i: labels[i],
            key=f"wh_{st.session_state.warehouse_pick_version}",
        )
        st.session_state.warehouse_id = ids[int(pick)]
        if st.button("Refresh warehouses"):
            st.session_state.warehouse_pick_version += 1
            st.session_state._wr = None
            st.rerun()
    else:
        st.info("No warehouses returned for your account.")

    st.subheader("1 · Upload Power BI file (.pbit)")
    files = st.file_uploader(
        "Power BI template",
        type=["pbit"],
        accept_multiple_files=True,
        help="A .pbit file contains the semantic model and report; this app reads tables and measures to migrate.",
    )
    if files:
        models: list[tuple[str, PbitCanonicalModel]] = []
        for f in files:
            m = extract_pbit_canonical(f.name, f.getvalue())
            for ms in m.measures:
                if ms.dax_expression:
                    ms.dax_bucket = classify_measure_dax(ms.dax_expression)
            models.append((f.name, m))
        st.session_state.models = models
        st.success(f"Parsed {len(models)} file(s).")

    st.subheader("2 · Name the dashboard in Databricks")
    st.caption(
        "This becomes the **AI/BI (Lakeview)** dashboard title when you create workspace assets below. "
        "Characters like **/** are replaced automatically (Lakeview rejects them)."
    )
    st.session_state.dashboard_name = st.text_input("Dashboard display name", value=st.session_state.dashboard_name)

    if not st.session_state.models:
        st.info("Upload a `.pbit` file to continue.")
        st.stop()

    st.subheader("3 · Map Power BI model tables → Unity Catalog")
    st.caption(
        "Each table in the uploaded model must point at a `catalog.schema.table` (or view) in this workspace. "
        "Suggested UC table names are derived from Power BI names (snake_case)."
    )
    all_tables: list[str] = []
    for _, m in st.session_state.models:
        for t in m.tables:
            if t.name not in all_tables:
                all_tables.append(t.name)
    catalogs = wr.list_catalogs()
    if not catalogs:
        st.warning("No Unity Catalog catalogs returned. Check workspace permissions.")
    bindings: dict[str, str] = {}
    for t in all_tables:
        suggested = _suggest_uc_table_name(t)
        st.markdown(f"**`{t}`**")
        st.caption(f"Suggested UC table name: `{suggested}`")
        c1, c2, c3 = st.columns(3)
        with c1:
            cat_opts = [""] + catalogs
            cat = st.selectbox(
                "Catalog",
                cat_opts,
                key=f"map_cat::{t}",
                format_func=lambda x: "— Select —" if x == "" else str(x),
            )
        with c2:
            sch_opts = [""] + (wr.list_schemas(cat) if cat else [])
            sch = st.selectbox(
                "Schema",
                sch_opts,
                key=f"map_sch::{t}::{cat or '_'}",
                format_func=lambda x: "— Select —" if x == "" else str(x),
            )
        with c3:
            api_tables = wr.list_uc_tables(cat, sch) if (cat and sch) else []
            names_lower = {x.lower() for x in api_tables}
            merged = list(api_tables)
            if suggested and suggested.lower() not in names_lower:
                merged.append(suggested)
            merged = sorted(set(merged), key=str.lower)
            tbl_opts = [""] + merged

            def _tbl_label(x: str) -> str:
                if x == "":
                    return "— Select table —"
                if suggested and x.lower() == suggested.lower():
                    return f"{x} (suggested)"
                return str(x)

            tbl = st.selectbox(
                "Table",
                tbl_opts,
                key=f"map_tbl::{t}::{cat or '_'}::{sch or '_'}",
                format_func=_tbl_label,
            )
        if cat and sch and tbl:
            bindings[t] = f"{cat}.{sch}.{tbl}"
        else:
            bindings[t] = ""
    st.session_state.bindings = bindings

    # Disposition results (used for optional exports and migration pack)
    results: list[Any] = []
    for name, model in st.session_state.models:
        res = effective_disposition(
            classify_pbit(model),
            bindings,
            [tb.name for tb in model.tables],
        )
        results.append(res)
    st.session_state.results = results

    st.markdown("---")
    st.subheader("4 · Create workspace assets from this upload")
    st.caption(
        "This is the **main outcome**: an **AI/BI (Lakeview) dashboard** and a **Genie space** in this Databricks "
        "workspace, built from your `.pbit` mappings. The app first runs a quick SQL probe per table on the "
        "warehouse you selected; if every probe succeeds, it creates the assets. "
        "**The parent folder must already exist** (create it in the Workspace browser if needed). "
        "If the app runs as a **service principal** (OAuth M2M), grant that identity **Browse** on this folder "
        "(Workspace → right‑click folder → Permissions / sharing)."
    )
    if wr.is_oauth_m2m:
        _cid = wr.oauth_m2m_client_id or "unknown"
        st.info(
            f"This app uses **OAuth M2M** (service principal, client id `{_cid}`). "
            "It can only create dashboards/Genie under workspace paths **that identity can read**. "
            "If a folder exists for you in the UI but deploy fails with **path does not exist**, that usually means "
            "**permission denied** for the app—not a missing folder. "
            "Use **`/Workspace/Shared`** (or a subfolder) unless an admin granted this app **Browse** on your "
            "`/Workspace/Users/...` tree."
        )
    if not st.session_state.get("deploy_parent_path_seeded"):
        st.session_state.deploy_parent_path_seeded = True
        if not (st.session_state.get("deploy_parent_path") or "").strip():
            st.session_state.deploy_parent_path = wr.suggested_deploy_parent_path()
    st.text_input(
        "Workspace parent path (must exist and be readable by the app)",
        key="deploy_parent_path",
        help="Databricks Apps (M2M): default is /Workspace/Shared. User sessions: default is your /Workspace/Users/<email>.",
    )
    if wr.is_oauth_m2m:
        _cur_pp = (st.session_state.get("deploy_parent_path") or "").strip()
        if _cur_pp.startswith("/Workspace/Users/") and "@" in _cur_pp:
            if st.button("Use **/Workspace/Shared** (recommended for this app)", key="use_shared_parent_path"):
                st.session_state.deploy_parent_path = "/Workspace/Shared"
                st.rerun()
    _pp_eff = (st.session_state.get("deploy_parent_path") or "").strip() or (
        "/Workspace/Shared" if wr.is_oauth_m2m else "/Workspace/Users"
    )
    _path_ok, _path_err = wr.verify_workspace_parent_dir(_pp_eff)
    if not _path_ok:
        st.warning(_path_err or "Parent path check failed.")
    else:
        st.caption(
            f"Parent path check (`{_pp_eff}`): folder exists and this app identity can read it."
        )
    st.checkbox("Publish AI/BI dashboard after create", key="deploy_publish_lakeview")
    st.checkbox("Create Genie space for the same tables", key="deploy_create_genie")

    wh_ok = bool((st.session_state.warehouse_id or "").strip())
    bind_ok = all(parse_uc_fqtn((bindings.get(t) or "").strip()) is not None for t in all_tables)
    if not wh_ok:
        st.warning("Select a SQL warehouse above — it is required to create the dashboard and Genie.")
    if all_tables and not bind_ok:
        st.warning("Map **every** model table to Unity Catalog before creating assets.")

    measure_names: list[str] = []
    for _, m in st.session_state.models:
        for ms in m.measures:
            if ms.name and ms.name not in measure_names:
                measure_names.append(ms.name)

    create_clicked = st.button(
        "Create AI/BI dashboard & Genie space",
        type="primary",
        disabled=not (wh_ok and bind_ok and bool(all_tables) and _path_ok),
        help="Requires a valid workspace folder, then validates UC bindings and calls Lakeview + Genie APIs",
    )

    if create_clicked:
        wid = (st.session_state.warehouse_id or "").strip()
        out_rows = _run_binding_validation(wr, wid, bindings, all_tables)
        st.session_state.migration_validation = out_rows
        validation_all_ok = (
            bool(all_tables)
            and len(out_rows) == len(all_tables)
            and all(r.get("state") == "SUCCEEDED" for r in out_rows)
        )
        pp = (st.session_state.get("deploy_parent_path") or "").strip() or (
            "/Workspace/Shared" if wr.is_oauth_m2m else "/Workspace/Users"
        )
        if not validation_all_ok:
            errs = [
                f"{r.get('pbi_table')} → `{r.get('uc_fqtn')}`: {r.get('error_message') or r.get('state') or 'failed'}"
                for r in out_rows
                if r.get("state") != "SUCCEEDED"
            ]
            st.session_state.deploy_result = {
                "ok": False,
                "phase": "validation",
                "errors": errs or ["SQL validation failed."],
                "lakeview": None,
                "genie": None,
                "starter_sql": None,
            }
        else:
            try:
                src_names = [name for name, _ in st.session_state.models]
                visual_rows: list[dict[str, Any]] = []
                for fname, m in st.session_state.models:
                    for v in m.visuals:
                        visual_rows.append(
                            {
                                "source_file": fname,
                                "visual_id": v.visual_id,
                                "report_name": v.report_name,
                                "page_name": v.page_name,
                                "visual_type": v.visual_type,
                                "intent_statement": v.intent_statement,
                            }
                        )
                st.session_state.deploy_result = run_workspace_deploy(
                    wr,
                    warehouse_id=wid,
                    parent_path=pp,
                    dashboard_display_name=st.session_state.dashboard_name,
                    bindings=dict(bindings),
                    ordered_model_tables=list(all_tables),
                    measure_names=measure_names,
                    publish_lakeview=bool(st.session_state.get("deploy_publish_lakeview", True)),
                    create_genie=bool(st.session_state.get("deploy_create_genie", True)),
                    source_filenames=src_names,
                    visual_rows=visual_rows,
                )
            except Exception as e:  # noqa: BLE001
                st.session_state.deploy_result = {
                    "ok": False,
                    "phase": "deploy",
                    "errors": [str(e)],
                    "lakeview": None,
                    "genie": None,
                    "starter_sql": None,
                }
            if st.session_state.deploy_result.get("ok"):
                try:
                    analysis_snapshots = []
                    for (name, _), res in zip(st.session_state.models, results):
                        analysis_snapshots.append(
                            {
                                "source_file": name,
                                "artifact_id": res.artifact_id,
                                "recommended": res.recommended_disposition.value,
                                "effective": res.effective_disposition.value,
                                "binding_blockers": list(res.binding_blockers),
                            }
                        )
                    pack = build_migration_pack(
                        dashboard_name=st.session_state.dashboard_name,
                        warehouse_id=wid,
                        bindings=dict(bindings),
                        ordered_model_tables=list(all_tables),
                        source_files=[n for n, _ in st.session_state.models],
                        analysis_snapshots=analysis_snapshots,
                        validation_results=out_rows,
                    )
                    st.session_state.migration_pack_json = json.dumps(pack, indent=2, default=str)
                except Exception:
                    pass

    mv = st.session_state.get("migration_validation")
    if mv:
        st.markdown("**Last binding check (SQL warehouse)**")
        st.dataframe(
            [
                {
                    "PBI table": r.get("pbi_table"),
                    "UC binding": r.get("uc_fqtn"),
                    "State": r.get("state"),
                    "Error": r.get("error_message") or "",
                }
                for r in mv
            ],
            use_container_width=True,
            hide_index=True,
        )
        with st.expander("Probe SQL & row previews"):
            for r in mv:
                st.markdown(f"**{r.get('pbi_table')}** → `{r.get('uc_fqtn')}`")
                if r.get("sql"):
                    st.code(r["sql"], language="sql")
                prev = r.get("preview_rows") or []
                cols = r.get("columns") or []
                if prev and cols:
                    st.caption(", ".join(cols))
                    st.dataframe([dict(zip(cols, row)) for row in prev], hide_index=True)

    mpj = st.session_state.get("migration_pack_json")
    if mpj:
        st.download_button(
            "Download migration pack (JSON)",
            data=mpj,
            file_name="pbi_to_aibi_migration_pack.json",
            mime="application/json",
            key="dl_migration_pack",
        )
        try:
            preview = json.loads(mpj)
            starter = preview.get("starter_dataset_sql", "")
            if starter:
                with st.expander("Starter dataset SQL (from pack)"):
                    st.code(starter, language="sql")
        except json.JSONDecodeError:
            pass

    dr = st.session_state.get("deploy_result")
    if dr:
        if dr.get("ok"):
            st.success("AI/BI dashboard and Genie space were created in this workspace (per your options above).")
        elif dr.get("phase") == "validation":
            st.error("SQL binding checks failed. Fix Unity Catalog mappings or warehouse access, then try again.")
        else:
            st.error("Workspace create finished with one or more errors.")
        for err in dr.get("errors") or []:
            st.warning(err)
        host = wr.webapp_host
        lv = dr.get("lakeview") or {}
        gn = dr.get("genie") or {}
        if lv.get("ok") and lv.get("dashboard_id") and host:
            did = lv["dashboard_id"]
            st.markdown(f"[Open AI/BI dashboard]({host}/dashboardsv3/{did})")
        elif lv.get("dashboard_id"):
            st.caption(f"Dashboard id: `{lv.get('dashboard_id')}`")
        if gn.get("ok") and gn.get("space_id") and host:
            sid = gn["space_id"]
            st.markdown(f"[Open Genie space]({host}/genie/spaces/{sid})")
        elif gn.get("space_id"):
            st.caption(f"Genie space id: `{gn.get('space_id')}`")
        if dr.get("starter_sql"):
            with st.expander("Deployed primary dataset SQL"):
                st.code(dr["starter_sql"], language="sql")
        pmj = dr.get("parity_manifest_json")
        if pmj:
            st.download_button(
                "Download deploy parity manifest (JSON)",
                data=pmj,
                file_name="deploy_parity_manifest.json",
                mime="application/json",
                key="dl_parity_manifest",
            )
            pm = dr.get("parity_manifest") or {}
            ent = pm.get("entries") or []
            n_map = sum(1 for e in ent if (e or {}).get("target") == "lakeview_multiline_intent")
            n_gap = sum(1 for e in ent if (e or {}).get("target") == "gap")
            st.caption(f"Parity manifest: {n_map} mapped to Lakeview intent widgets, {n_gap} gap(s), {len(ent)} total visuals.")
            with st.expander("Deploy parity manifest (preview)"):
                st.json(pm)

    with st.expander("Migration analysis & JSON exports (optional)"):
        st.caption("Disposition scoring from RULES.md — useful for review; not required to create workspace assets.")
        for idx, ((name, _model), res) in enumerate(zip(st.session_state.models, results)):
            st.markdown(f"##### {idx + 1}. {name}")
            st.json(
                {
                    "warehouse_id": st.session_state.warehouse_id,
                    "dashboard_name": st.session_state.dashboard_name,
                    "recommended": res.recommended_disposition.value,
                    "effective": res.effective_disposition.value,
                    "binding_blockers": res.binding_blockers,
                }
            )
            st.download_button(
                "Download artifact JSON",
                data=res.model_dump_json_pretty(),
                file_name=f"{Path(name).stem}_artifact.json",
                mime="application/json",
                key=f"dl_{name}_artifact",
            )
            if idx < len(results) - 1:
                st.divider()
        dr_for_rollup = st.session_state.get("deploy_result") or {}
        parity_bl: list[str] | None = None
        bl = dr_for_rollup.get("parity_manifest_backlog_lines")
        if isinstance(bl, list) and bl:
            parity_bl = [str(x) for x in bl]
        rollup = estate_rollup_from_results(
            [n for n, _ in st.session_state.models],
            results,
            parity_backlog_lines=parity_bl,
        )
        st.download_button(
            "Download estate rollup JSON",
            data=rollup.model_dump_json_pretty(),
            file_name="estate_rollup.json",
            mime="application/json",
            key="dl_estate_rollup",
        )


main()
