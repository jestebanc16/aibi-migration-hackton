"""Databricks WorkspaceClient helpers: warehouses, UC browse, SQL execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
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


@dataclass
class ServingEndpointOption:
    """Chat / foundation-model style serving endpoint (for LLM migration)."""

    name: str
    task: str | None = None
    state_ready: str | None = None


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

    def list_chat_serving_endpoints(self) -> tuple[list[ServingEndpointOption], str | None]:
        """
        List workspace serving endpoints suitable for chat completions (foundation or external chat models).

        Prefers endpoints whose ``task`` suggests chat; if none match, returns all listable endpoints that
        look ready so the UI can still offer a manual pick.
        """
        try:
            raw = list(self._w.serving_endpoints.list())
        except Exception as e:  # noqa: BLE001
            return [], f"Could not list serving endpoints: {e}"

        def _ready_label(ep: Any) -> str | None:
            st = getattr(ep, "state", None)
            if not st:
                return None
            r = getattr(st, "ready", None)
            if r is None:
                return None
            return str(getattr(r, "value", r))

        def _config_updating(ep: Any) -> bool:
            st = getattr(ep, "state", None)
            if not st:
                return False
            cu = getattr(st, "config_update", None)
            if cu is None:
                return False
            v = str(getattr(cu, "value", cu))
            return v == "IN_PROGRESS"

        def _not_ready(ep: Any) -> bool:
            st = getattr(ep, "state", None)
            if not st:
                return False
            r = getattr(st, "ready", None)
            if r is None:
                return False
            v = str(getattr(r, "value", r))
            return v == "NOT_READY"

        def _is_chat_task(task: str | None) -> bool:
            t = (task or "").upper()
            return "CHAT" in t or "LLM" in t or "COMPLETION" in t

        stable = [e for e in raw if not _config_updating(e)]
        chatish = [e for e in stable if _is_chat_task(getattr(e, "task", None)) and not _not_ready(e)]
        pool = chatish if chatish else [e for e in stable if not _not_ready(e)]
        if not pool:
            pool = list(raw)

        out: list[ServingEndpointOption] = []
        for ep in pool:
            n = getattr(ep, "name", None)
            if not n:
                continue
            out.append(
                ServingEndpointOption(
                    name=str(n),
                    task=getattr(ep, "task", None),
                    state_ready=_ready_label(ep),
                )
            )
        return sorted(out, key=lambda x: x.name.lower()), None

    @staticmethod
    def _assistant_text_from_query_response(resp: Any) -> tuple[str | None, str | None]:
        choices = getattr(resp, "choices", None) or []
        if not choices:
            return None, "Serving endpoint returned no choices"
        ch0 = choices[0]
        msg = getattr(ch0, "message", None)
        if msg is not None:
            content = getattr(msg, "content", None)
            if content:
                return str(content).strip(), None
        txt = getattr(ch0, "text", None)
        if txt:
            return str(txt).strip(), None
        return None, "Serving endpoint returned empty message content"

    @staticmethod
    def _serving_error_suggests_omit_temperature(message: str) -> bool:
        m = message.lower()
        if "temperature" in m or "top_p" in m or "top p" in m:
            return True
        if "does not allow" in m and "parameter" in m:
            return True
        return False

    @staticmethod
    def _serving_error_suggests_omit_max_tokens(message: str) -> bool:
        m = message.lower()
        return "max_tokens" in m or "max tokens" in m or "max_completion_tokens" in m

    def query_serving_endpoint_chat(
        self,
        endpoint_name: str,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float | None = 0.2,
        max_tokens: int | None = 16000,
    ) -> tuple[str | None, str | None]:
        """
        OpenAI-style chat completion via ``POST /serving-endpoints/{name}/invocations``.

        Some models (e.g. Claude Opus on certain routes) reject ``temperature`` or ``max_tokens``;
        this method retries with those fields omitted when the error message indicates an unsupported
        parameter.

        Returns (assistant_text, error_message).
        """
        name = (endpoint_name or "").strip()
        if not name:
            return None, "serving endpoint name is empty"

        messages = [
            ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
            ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
        ]

        t_val = float(temperature) if temperature is not None else None
        mt_val = int(max_tokens) if max_tokens is not None else None

        configs: list[tuple[float | None, int | None]] = []
        if t_val is not None:
            configs.append((t_val, mt_val))
        if mt_val is not None:
            configs.append((None, mt_val))
        configs.append((None, None))

        seen: set[tuple[float | None, int | None]] = set()
        ordered: list[tuple[float | None, int | None]] = []
        for c in configs:
            if c not in seen:
                seen.add(c)
                ordered.append(c)

        last_err = ""
        for t_u, mt_u in ordered:
            kwargs: dict[str, Any] = {"name": name, "messages": messages}
            if t_u is not None:
                kwargs["temperature"] = t_u
            if mt_u is not None:
                kwargs["max_tokens"] = mt_u
            try:
                resp = self._w.serving_endpoints.query(**kwargs)
            except Exception as e:  # noqa: BLE001
                last_err = str(e)
                el = last_err.lower()
                strip_temp = t_u is not None and self._serving_error_suggests_omit_temperature(last_err)
                strip_mt = mt_u is not None and self._serving_error_suggests_omit_max_tokens(last_err)
                if strip_temp or strip_mt:
                    continue
                return None, f"Serving endpoint query failed: {last_err}"
            return self._assistant_text_from_query_response(resp)

        return None, f"Serving endpoint query failed: {last_err}" if last_err else "Serving endpoint query failed"

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
