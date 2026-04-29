"""Databricks WorkspaceClient helpers: warehouses, UC browse, SQL execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.workspace import ObjectType


@dataclass
class WarehouseOption:
    warehouse_id: str
    name: str
    state: str | None = None


@dataclass
class VolumeOption:
    catalog_name: str
    schema_name: str
    name: str
    full_name: str  # catalog.schema.volume


def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


def get_workspace_client_from_config(cfg: Config | None = None) -> WorkspaceClient:
    if cfg is None:
        return WorkspaceClient()
    return WorkspaceClient(config=cfg)


class WorkspaceResources:
    def __init__(self, client: WorkspaceClient | None = None) -> None:
        self._w = client or WorkspaceClient()

    @property
    def webapp_host(self) -> str | None:
        """Workspace URL host (e.g. https://…cloud.databricks.com), no trailing slash."""
        h = getattr(self._w.config, "host", None)
        return str(h).rstrip("/") if h else None

    @property
    def is_oauth_m2m(self) -> bool:
        """True when this client uses machine-to-machine OAuth (typical for Databricks Apps with a service principal)."""
        at = (getattr(self._w.config, "auth_type", None) or "").strip().lower()
        return at == "oauth-m2m"

    @property
    def oauth_m2m_client_id(self) -> str | None:
        cid = getattr(self._w.config, "client_id", None)
        return str(cid).strip() if cid else None

    def suggested_deploy_parent_path(self) -> str:
        """
        Default parent folder for Lakeview / Genie.

        **OAuth M2M (Databricks Apps):** defaults to ``/Workspace/Shared`` — the app's service principal
        usually cannot read another user's ``/Workspace/Users/you@…`` home unless an admin grants it.

        **User auth:** ``/Workspace/Users/<userName>`` when ``current_user.me()`` has an email-style userName;
        else ``/Workspace/Users``.
        """
        if self.is_oauth_m2m:
            return "/Workspace/Shared"
        try:
            me = self._w.current_user.me()
            un = getattr(me, "user_name", None)
            if un and "@" in str(un):
                return f"/Workspace/Users/{str(un).strip()}"
        except Exception:
            pass
        return "/Workspace/Users"

    def verify_workspace_parent_dir(self, path: str) -> tuple[bool, str | None]:
        """
        Returns (True, None) if ``path`` exists as a workspace **directory** and this client can read it.
        Otherwise (False, error hint). Use before Lakeview/Genie deploy so missing folders fail fast.
        """
        p = (path or "").strip().rstrip("/")
        if not p.startswith("/"):
            return False, "Path must be an absolute workspace path starting with / (e.g. /Workspace/Users/you@company.com)."
        try:
            info = self._w.workspace.get_status(p)
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            hint = (
                "Note: APIs often say the path \"doesn't exist\" when the real issue is **no read/browse permission** "
                "for **this app's identity** (e.g. OAuth M2M service principal). "
                "Try ``/Workspace/Shared`` or a subfolder there, or ask an admin to grant **Browse** on your folder."
            )
            return (
                False,
                f"Folder not found or not readable by this app identity: {msg}\n\n{hint}",
            )
        ot = getattr(info, "object_type", None)
        is_dir = ot == ObjectType.DIRECTORY or getattr(ot, "value", None) == ObjectType.DIRECTORY.value
        if not is_dir:
            return False, f"Path exists but is not a directory (type={ot}). Choose a folder path, not a file."
        return True, None

    def list_warehouses(self) -> tuple[list[WarehouseOption], str | None]:
        """Return (warehouses, api_error). api_error is set when the list API fails."""
        out: list[WarehouseOption] = []
        try:
            for wh in self._w.warehouses.list():
                wid = getattr(wh, "id", None) or getattr(wh, "warehouse_id", None)
                name = getattr(wh, "name", None) or str(wid)
                state = getattr(wh, "state", None)
                if wid:
                    out.append(WarehouseOption(warehouse_id=str(wid), name=str(name), state=str(state) if state else None))
        except Exception as e:
            return [], f"Could not list SQL warehouses: {e}"
        return sorted(out, key=lambda x: x.name.lower()), None

    def list_catalogs(self) -> list[str]:
        names: list[str] = []
        try:
            for c in self._w.catalogs.list():
                n = getattr(c, "name", None)
                if n:
                    names.append(str(n))
        except Exception:
            pass
        return sorted(set(names), key=str.lower)

    def list_schemas(self, catalog_name: str) -> list[str]:
        names: list[str] = []
        try:
            for s in self._w.schemas.list(catalog_name=catalog_name):
                n = getattr(s, "name", None)
                if n:
                    names.append(str(n))
        except Exception:
            pass
        return sorted(set(names), key=str.lower)

    def list_uc_tables(self, catalog_name: str, schema_name: str, max_results: int = 500) -> list[str]:
        """Unity Catalog table names in a schema (for binding dropdowns)."""
        if not catalog_name or not schema_name:
            return []
        names: list[str] = []
        try:
            for tbl in self._w.tables.list(
                catalog_name=catalog_name,
                schema_name=schema_name,
                max_results=max_results,
            ):
                n = getattr(tbl, "name", None)
                if n:
                    names.append(str(n))
        except Exception:
            pass
        return sorted(set(names), key=str.lower)

    def list_volumes(self, catalog_name: str, schema_name: str) -> list[VolumeOption]:
        out: list[VolumeOption] = []
        try:
            vol_api = getattr(self._w, "volumes", None)
            if vol_api is None:
                return out
            for v in vol_api.list(catalog_name=catalog_name, schema_name=schema_name):
                name = getattr(v, "name", None)
                cat = getattr(v, "catalog_name", None) or catalog_name
                sch = getattr(v, "schema_name", None) or schema_name
                if name:
                    full = f"{cat}.{sch}.{name}"
                    out.append(
                        VolumeOption(
                            catalog_name=str(cat),
                            schema_name=str(sch),
                            name=str(name),
                            full_name=full,
                        )
                    )
        except Exception:
            pass
        return sorted(out, key=lambda x: x.full_name.lower())

    def execute_sql(
        self,
        warehouse_id: str,
        statement: str,
        wait_timeout: str = "50s",
        *,
        include_preview: bool = False,
        max_preview_rows: int = 5,
    ) -> dict[str, Any]:
        """Run a SQL statement; returns status, optional error, optional small INLINE preview."""
        se = self._w.statement_execution
        resp = se.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            wait_timeout=wait_timeout,
        )
        state = getattr(resp, "status", None)
        st = getattr(state, "state", None) if state else None
        err = getattr(state, "error", None) if state else None
        err_msg = getattr(err, "message", None) if err else None
        st_str: str | None
        if st is None:
            st_str = None
        elif isinstance(st, StatementState):
            st_str = st.value
        elif hasattr(st, "value"):
            st_str = str(getattr(st, "value"))
        else:
            st_str = str(st)

        out: dict[str, Any] = {
            "statement_id": getattr(resp, "statement_id", None),
            "state": st_str,
            "error_message": err_msg,
        }

        succeeded = st == StatementState.SUCCEEDED or getattr(st, "value", None) == StatementState.SUCCEEDED.value
        if include_preview and succeeded:
            cols: list[str] = []
            man = getattr(resp, "manifest", None)
            schema = getattr(man, "schema", None) if man else None
            col_objs = getattr(schema, "columns", None) if schema else None
            if col_objs:
                for c in col_objs:
                    n = getattr(c, "name", None)
                    if n:
                        cols.append(str(n))
            res = getattr(resp, "result", None)
            da = getattr(res, "data_array", None) if res else None
            out["columns"] = cols
            out["preview_rows"] = (da or [])[: max(0, int(max_preview_rows))]
        elif include_preview:
            out["columns"] = []
            out["preview_rows"] = []  # not SUCCEEDED or no INLINE chunk

        return out

    def describe_table_columns(self, warehouse_id: str, fqtn: str) -> tuple[list[str], str | None]:
        """Return physical column names from DESCRIBE TABLE (stops at # partition section)."""
        from aibi_migrator.migration_pack import quote_uc_fqtn

        try:
            q = f"DESCRIBE TABLE {quote_uc_fqtn(fqtn)}"
        except ValueError as e:
            return [], str(e)
        r = self.execute_sql(
            warehouse_id,
            q,
            wait_timeout="50s",
            include_preview=True,
            max_preview_rows=500,
        )
        if r.get("state") != "SUCCEEDED":
            return [], (r.get("error_message") or r.get("state") or "DESCRIBE TABLE failed")
        rows = r.get("preview_rows") or []
        names: list[str] = []
        for row in rows:
            if not row:
                continue
            cell0 = str(row[0]).strip()
            if not cell0 or cell0.startswith("#"):
                break
            if cell0.lower() in ("col_name", "column_name"):
                continue
            names.append(cell0)
        return names, None

    def deploy_lakeview_dashboard(
        self,
        *,
        display_name: str,
        parent_path: str,
        warehouse_id: str,
        serialized_dashboard: str,
        publish: bool = True,
    ) -> dict[str, Any]:
        """Create draft Lakeview dashboard and optionally publish it."""
        pp = (parent_path or "").strip().rstrip("/")
        dash = Dashboard(
            display_name=display_name,
            parent_path=pp,
            serialized_dashboard=serialized_dashboard,
            warehouse_id=warehouse_id,
        )
        created = self._w.lakeview.create(dash)
        did = getattr(created, "dashboard_id", None)
        if not did:
            return {"ok": False, "error": "Lakeview create returned no dashboard_id"}
        if publish:
            self._w.lakeview.publish(dashboard_id=did, warehouse_id=warehouse_id)
        return {
            "ok": True,
            "dashboard_id": did,
            "display_name": getattr(created, "display_name", None) or display_name,
            "published": publish,
        }

    def deploy_genie_space(
        self,
        *,
        warehouse_id: str,
        serialized_space: str,
        title: str,
        description: str | None = None,
        parent_path: str | None = None,
    ) -> dict[str, Any]:
        """Create Genie space from serialized_space JSON string."""
        pp = (parent_path or "").strip().rstrip("/") or None
        sp = self._w.genie.create_space(
            warehouse_id=warehouse_id,
            serialized_space=serialized_space,
            title=title,
            description=description,
            parent_path=pp,
        )
        sid = getattr(sp, "space_id", None)
        if not sid:
            return {"ok": False, "error": "Genie create returned no space_id"}
        return {"ok": True, "space_id": sid, "title": getattr(sp, "title", None) or title}
