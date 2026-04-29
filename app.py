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
from aibi_migrator.llm_migrate import use_llm_migration_from_env
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
        "serving_endpoint_name": "",
        "serving_endpoint_manual": "",
        "serving_endpoint_pick_version": 0,
        "llm_temperature": 0.2,
        "_map_tables_sig": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _workspace_resources() -> WorkspaceResources:
    """One client per browser session (Databricks Apps user / default SDK auth)."""
    if st.session_state.get("_wr") is None:
        st.session_state._wr = WorkspaceResources()
    return st.session_state._wr


def _map_tables_signature(all_tables: list[str]) -> str:
    """Detect when the set of PBI tables changed so we can reset per-table name widgets."""
    return "|".join(sorted(all_tables))


def _reset_per_table_uc_name_keys() -> None:
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and k.startswith("map_uc_tbl::"):
            del st.session_state[k]


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


def _canonical_models_json_for_llm(models: list[tuple[str, PbitCanonicalModel]]) -> str:
    """Serialize uploaded canonical models for LLM grounding (size-capped)."""
    blobs: list[dict[str, Any]] = []
    for name, m in models:
        blobs.append({"source_file": name, "model": m.model_dump(mode="json")})
    raw = json.dumps(blobs, indent=2, default=str)
    max_chars = 200_000
    if len(raw) > max_chars:
        return raw[: max_chars - 40] + "\n/* …truncated for token budget … */\n"
    return raw


def _all_tables_from_models() -> list[str]:
    names: list[str] = []
    for _, m in st.session_state.models:
        for t in m.tables:
            if t.name not in names:
                names.append(t.name)
    return names


def _disposition_results(bindings: dict[str, str]) -> list[Any]:
    out: list[Any] = []
    for name, model in st.session_state.models:
        out.append(
            effective_disposition(
                classify_pbit(model),
                bindings,
                [tb.name for tb in model.tables],
            )
        )
    return out


def main() -> None:
    st.set_page_config(page_title="Power BI → AI/BI + Genie", layout="wide")
    _init_session()

    st.title("Power BI → Databricks AI/BI + Genie")
    st.caption(
        "Upload a **.pbit**, map model tables to **Unity Catalog**, then publish a **dashboard** and **Genie space**."
    )
    with st.expander("File format & tips"):
        st.markdown(
            "- Use a Power BI **template** (`.pbit`). `.pbix` is not supported — use *Save as* template in Desktop.\n"
            "- Dashboard titles cannot contain `/` (sanitized automatically for Lakeview)."
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

    tab_connect, tab_upload, tab_map, tab_deploy = st.tabs(["Connect", "Upload", "Map", "Deploy"])

    with tab_connect:
        st.subheader("SQL warehouse")
        st.caption("Runs validation SQL, the dashboard dataset, and Genie against your chosen warehouse.")
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

        st.divider()
        st.subheader("Foundation / chat model (serving endpoint)")
        st.caption(
            "Lakeview and Genie **content** are generated by the selected **chat-capable** model serving endpoint, "
            "grounded on extracted `.pbit` data. [Serving endpoints](https://docs.databricks.com/en/machine-learning/model-serving/index.html)"
        )
        if use_llm_migration_from_env():
            eps, ep_err = wr.list_chat_serving_endpoints()
            if ep_err:
                st.warning(ep_err)
            if eps:
                labels = [
                    f"{e.name}"
                    + (f"  —  task={e.task}" if e.task else "")
                    + (f"  ({e.state_ready})" if e.state_ready else "")
                    for e in eps
                ]
                names = [e.name for e in eps]
                cur = (st.session_state.serving_endpoint_name or "").strip()
                try:
                    d_idx = names.index(cur) if cur in names else 0
                except ValueError:
                    d_idx = 0
                ip = st.selectbox(
                    "Choose serving endpoint",
                    range(len(labels)),
                    index=d_idx,
                    format_func=lambda i: labels[i],
                    key=f"sep_{st.session_state.serving_endpoint_pick_version}",
                )
                st.session_state.serving_endpoint_name = names[int(ip)]
            else:
                st.info(
                    "No endpoints returned (or list is empty). Enter an endpoint **name** manually below — "
                    "your identity needs **CAN QUERY** on that endpoint."
                )
            st.text_input(
                "Manual endpoint name (overrides dropdown when non-empty)",
                key="serving_endpoint_manual",
                help="Use when the list is empty or you have a custom endpoint name.",
            )
            eff = (st.session_state.serving_endpoint_manual or "").strip() or (
                st.session_state.serving_endpoint_name or ""
            ).strip()
            if eff and (st.session_state.serving_endpoint_manual or "").strip():
                st.caption(f"**Using:** `{eff}` (manual override)")
            elif eff:
                st.caption(f"**Using:** `{eff}`")
            st.slider(
                "LLM temperature",
                min_value=0.0,
                max_value=1.0,
                step=0.05,
                key="llm_temperature",
            )
            st.caption(
                "If the endpoint rejects `temperature` (e.g. some Claude Opus routes), the app retries without it automatically."
            )
            if st.button("Refresh serving endpoints"):
                st.session_state.serving_endpoint_pick_version += 1
                st.session_state._wr = None
                st.rerun()
        else:
            st.info(
                "LLM migration is **off** (`USE_LLM_MIGRATION=0`): deploy uses heuristic Lakeview/Genie only; "
                "no serving endpoint required."
            )

    with tab_upload:
        st.subheader("Template & dashboard name")
        files = st.file_uploader(
            "Power BI template (.pbit)",
            type=["pbit"],
            accept_multiple_files=True,
            help="Contains the semantic model and report layout used for migration.",
        )
        if files:
            parsed: list[tuple[str, PbitCanonicalModel]] = []
            for f in files:
                m = extract_pbit_canonical(f.name, f.getvalue())
                for ms in m.measures:
                    if ms.dax_expression:
                        ms.dax_bucket = classify_measure_dax(ms.dax_expression)
                parsed.append((f.name, m))
            st.session_state.models = parsed
            st.success(f"Parsed **{len(parsed)}** file(s).")
        if not st.session_state.models:
            st.info("Upload at least one `.pbit` to continue to **Map** and **Deploy**.")
        st.session_state.dashboard_name = st.text_input(
            "Dashboard display name",
            value=st.session_state.dashboard_name,
            help="Shown as the Lakeview dashboard title in the workspace.",
        )

    models: list[tuple[str, PbitCanonicalModel]] = list(st.session_state.models)
    all_tables: list[str] = _all_tables_from_models() if models else []

    with tab_map:
        if not models:
            st.info("Go to **Upload** and add a `.pbit` file first.")
        else:
            st.caption(
                "Pick **one catalog and schema** for the whole model, then confirm each **table name** "
                "(defaults are filled in — edit only when your UC name differs)."
            )
            catalogs = wr.list_catalogs()
            if not catalogs:
                st.warning("No Unity Catalog catalogs returned. Check workspace permissions.")

            sig = _map_tables_signature(all_tables)
            if st.session_state.get("_map_tables_sig") != sig:
                st.session_state._map_tables_sig = sig
                _reset_per_table_uc_name_keys()

            bindings: dict[str, str] = {t: "" for t in all_tables}
            if catalogs:
                if "map_uc_catalog" not in st.session_state or st.session_state.map_uc_catalog not in catalogs:
                    st.session_state.map_uc_catalog = catalogs[0]
                st.selectbox(
                    "Catalog (shared by all tables below)",
                    catalogs,
                    key="map_uc_catalog",
                )
                cat_val = (st.session_state.map_uc_catalog or "").strip()
                schemas = wr.list_schemas(cat_val) if cat_val else []
                if not schemas:
                    st.warning(f"No schemas listed for catalog `{cat_val}`. Check permissions or pick another catalog.")
                    sch_val = ""
                    st.session_state.map_uc_schema = ""
                else:
                    if "map_uc_schema" not in st.session_state or st.session_state.map_uc_schema not in schemas:
                        st.session_state.map_uc_schema = schemas[0]
                    st.selectbox(
                        "Schema (shared by all tables below)",
                        schemas,
                        key="map_uc_schema",
                    )
                    sch_val = (st.session_state.map_uc_schema or "").strip()

                st.divider()
                hdr1, hdr2, hdr3 = st.columns((2.2, 2.2, 3.6))
                hdr1.markdown("**Power BI table**")
                hdr2.markdown("**UC table / view name**")
                hdr3.markdown("**Mapped path**")

                for t in all_tables:
                    suggested = _suggest_uc_table_name(t)
                    key_part = f"map_uc_tbl::{t}"
                    if key_part not in st.session_state:
                        st.session_state[key_part] = suggested
                    c1, c2, c3 = st.columns((2.2, 2.2, 3.6))
                    with c1:
                        st.markdown(f"`{t}`")
                    with c2:
                        st.text_input(
                            "Table name",
                            label_visibility="collapsed",
                            key=key_part,
                            help=f"Default `{suggested}` from the PBI table name. Must exist as `{cat_val}.{sch_val}.<name>`.",
                        )
                    with c3:
                        part = (st.session_state.get(key_part) or "").strip()
                        if cat_val and sch_val and part:
                            fq = f"{cat_val}.{sch_val}.{part}"
                            st.caption(f"`{fq}`")
                            bindings[t] = fq
                        else:
                            st.caption("— incomplete —")
                            bindings[t] = ""
            st.session_state.bindings = bindings
            mapped_n = sum(1 for x in bindings.values() if parse_uc_fqtn((x or "").strip()))
            st.caption(f"Mapping progress: **{mapped_n}** / **{len(all_tables)}** tables have a full UC path.")

    bindings = dict(st.session_state.bindings or {})
    results: list[Any] = _disposition_results(bindings) if models else []
    if models:
        st.session_state.results = results

    with tab_deploy:
        st.subheader("Publish to workspace")
        st.caption(
            "Runs a quick SQL probe per mapped table, then creates the dashboard and Genie (if selected). "
            "The parent folder must already exist and be readable by this app."
        )
        if not models:
            st.info("Upload a `.pbit` in **Upload**, then map tables in **Map**, then return here to publish.")
        if wr.is_oauth_m2m:
            _cid = wr.oauth_m2m_client_id or "unknown"
            with st.expander("Service principal (OAuth M2M) — paths & permissions"):
                st.markdown(
                    f"This app runs as **OAuth M2M** (client id `{_cid}`). It can only write under paths that identity "
                    "can **browse**. If deploy says *path does not exist* but the folder is in the UI, it is usually "
                    "**permission denied** — prefer **`/Workspace/Shared`** unless an admin granted access under "
                    "`/Workspace/Users/...`."
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
        manual_ep = (st.session_state.get("serving_endpoint_manual") or "").strip()
        dropdown_ep = (st.session_state.get("serving_endpoint_name") or "").strip()
        endpoint_eff = manual_ep or dropdown_ep
        llm_on = use_llm_migration_from_env()
        ep_ok = bool(endpoint_eff) if llm_on else True
        if not wh_ok:
            st.warning("Select a SQL warehouse in the **Connect** tab — it is required to publish.")
        if llm_on and not ep_ok:
            st.warning(
                "Choose or enter a **chat serving endpoint** in **Connect** — required for LLM-assisted generation."
            )
        if all_tables and not bind_ok:
            st.warning("In the **Map** tab, map **every** model table to Unity Catalog before publishing.")

        measure_names: list[str] = []
        for _, m in st.session_state.models:
            for ms in m.measures:
                if ms.name and ms.name not in measure_names:
                    measure_names.append(ms.name)
    
        create_clicked = st.button(
            "Create AI/BI dashboard & Genie space",
            type="primary",
            disabled=not (wh_ok and ep_ok and bind_ok and bool(all_tables) and _path_ok),
            help="Requires warehouse, LLM endpoint (when enabled), UC mappings, and a valid workspace folder.",
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
                                    "layout_x": v.layout_x,
                                    "layout_y": v.layout_y,
                                    "layout_z": v.layout_z,
                                    "layout_w": v.layout_w,
                                    "layout_h": v.layout_h,
                                }
                            )
                    cjson = _canonical_models_json_for_llm(list(st.session_state.models))
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
                        serving_endpoint_name=endpoint_eff or None,
                        llm_temperature=float(st.session_state.get("llm_temperature", 0.2)),
                        canonical_models_json=cjson if use_llm_migration_from_env() else None,
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
            elif dr.get("phase") == "llm":
                st.error("LLM generation or validation failed before workspace create.")
            else:
                st.error("Workspace create finished with one or more errors.")
            for err in dr.get("errors") or []:
                st.warning(err)
            llm_errs = dr.get("llm_errors") or []
            if llm_errs:
                st.error("**LLM generation failed** — fix the endpoint or try again after reviewing errors below.")
                for e in llm_errs:
                    st.code(str(e), language="text")
            gaps = dr.get("llm_gap_notes") or []
            if gaps:
                with st.expander("LLM gap notes"):
                    for g in gaps:
                        st.markdown(f"- {g}")
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
                n_gap = sum(1 for e in ent if (e or {}).get("target") == "gap")
                n_mapped = len(ent) - n_gap
                st.caption(
                    f"Parity manifest: {n_mapped} mapped to Lakeview widgets (intent / table preview / chart placeholder), "
                    f"{n_gap} gap(s), {len(ent)} total visuals."
                )
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
