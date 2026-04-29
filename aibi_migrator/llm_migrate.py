"""LLM-assisted Lakeview + Genie generation from extracted .pbit context (foundation / chat serving endpoint)."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from aibi_migrator.dbx_client.workspace import WorkspaceResources
from aibi_migrator.knowledge import converter_knowledge_excerpt
from aibi_migrator.lakeview.dashboard_builder import _sql_to_query_lines


def use_llm_migration_from_env() -> bool:
    """When true (default), deploy expects a chat serving endpoint and uses the LLM path."""
    v = (os.environ.get("USE_LLM_MIGRATION") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _truncate(s: str, max_chars: int) -> str:
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 20] + "\n…[truncated]…\n"


def _extract_json_object(text: str) -> str | None:
    """Pull outermost {...} from model output (strips optional markdown fences)."""
    t = (text or "").strip()
    if "```" in t:
        m = re.search(r"```(?:json)?\s*([\s\S]*?)```", t, re.IGNORECASE)
        if m:
            t = m.group(1).strip()
    start = t.find("{")
    end = t.rfind("}")
    if start < 0 or end <= start:
        return None
    return t[start : end + 1]


def _repair_json_once(broken: str) -> str | None:
    """Lightweight fix attempts (trailing commas)."""
    s = broken.strip()
    s = re.sub(r",\s*}", "}", s)
    s = re.sub(r",\s*]", "]", s)
    try:
        json.loads(s)
        return s
    except json.JSONDecodeError:
        return None


SYSTEM_PROMPT = """You are a migration assistant for Power BI → Databricks AI/BI (Lakeview) and Genie.

You MUST read and respect the **semantic model** sections of the user payload (tables, calculated semantic “views”, columns, measures, relationships) extracted from the source `.pbit` **DataModelSchema**, plus **report visuals** with page and layout coordinates.

**1:1 fidelity (whenever the platform allows it):**
- **Pages:** Create **one Lakeview canvas page per distinct (source_file, report_name, page_name)** from the visual inventory when `page_name` is present. Reuse readable `displayName` text aligned with the Power BI page title.
- **Visuals:** Emit **one Lakeview widget per visual row** in the payload, in **canvas order** (`layout_y`, then `layout_x`, then `layout_z`). Do not merge multiple Power BI visuals into one widget unless Lakeview cannot represent that visual type—in that case use the closest supported widget and explain in `gap_notes`.
- **Semantic tables & calculated views:** Treat each listed semantic table as authoritative. **Calculated tables** (`semantic_role=calculated_view`) are semantic-layer views—preserve their role in narrative and relationships even though UC bindings point at physical tables.
- **Relationships:** Use the listed **semantic_relationships** to explain join logic in Genie and to justify chart/table encodings; do not invent join paths that contradict the model.
- **Columns & measures:** Only reference column and measure names that appear in the semantic inventory or DESCRIBE list. Map logical PBI tables to the provided UC `catalog.schema.table` bindings.

Rules:
- Output a single JSON object only (no markdown outside JSON).
- Ground every widget and instruction on the user payload. Do not invent `catalog.schema.table` names or columns not listed in bindings + semantic inventory + DESCRIBE.
- The Lakeview primary SQL dataset must be named "primary" unless you keep one dataset only; `queryLines` for that dataset are replaced server-side with validated SQL — still emit coherent placeholder `queryLines` using ONLY columns from the DESCRIBE list.
- `lakeview_dashboard` shape: top-level keys "datasets" (list) and "pages" (list). Each page: name, displayName, pageType (`PAGE_TYPE_CANVAS`), layout (list of {widget, position}).
- `genie`: title (<=120 chars), description (<=4000), `text_instruction` must summarize the **semantic model + UC mapping** and relationships, sample_questions (<=12).
- Apply **PBI→AI/BI reference excerpts** in the user message (from *pbi-aibi-converter* knowledge: visual type → widget mapping, DAX→Spark SQL placement, widget `version` / field-name rules). Prefer **Spark SQL** idioms compatible with Databricks SQL warehouses.

JSON schema:
{
  "lakeview_dashboard": { "datasets": [...], "pages": [...] },
  "genie": {
    "title": "string",
    "description": "string",
    "text_instruction": "string",
    "sample_questions": ["string", ...]
  },
  "gap_notes": ["optional short strings"]
}
"""


def _semantic_model_digest_markdown(
    canonical_models_json: str,
    bindings: dict[str, str],
) -> str:
    """Human-readable digest of semantic tables, calculated views, relationships, and UC mapping."""
    try:
        blob = json.loads(canonical_models_json)
    except json.JSONDecodeError:
        return "_Could not parse canonical_models_json for semantic digest._\n"
    if not isinstance(blob, list):
        return ""
    lines: list[str] = []
    for entry in blob:
        if not isinstance(entry, dict):
            continue
        sf = entry.get("source_file") or "?"
        m = entry.get("model") or {}
        if not isinstance(m, dict):
            continue
        lines.append(f"### Source file: `{sf}`")
        rels = m.get("semantic_relationships") or []
        if isinstance(rels, list) and rels:
            lines.append("**Semantic relationships (from DataModelSchema):**")
            for rel in rels[:120]:
                if not isinstance(rel, dict):
                    continue
                ft = rel.get("from_table") or "?"
                fc = rel.get("from_column") or "*"
                tt = rel.get("to_table") or "?"
                tc = rel.get("to_column") or "*"
                act = "" if rel.get("is_active", True) else " (inactive)"
                lines.append(f"- `{ft}[{fc}]` → `{tt}[{tc}]`{act}")
        lines.append("**Semantic tables & calculated views:**")
        for tb in m.get("tables") or []:
            if not isinstance(tb, dict):
                continue
            tn = str(tb.get("name") or "?")
            role = str(tb.get("semantic_role") or "data_table")
            uc = (bindings.get(tn) or "").strip() or "(not bound)"
            cols = tb.get("column_names") or []
            ms = tb.get("measure_names") or []
            if not isinstance(cols, list):
                cols = []
            if not isinstance(ms, list):
                ms = []
            cshow = ", ".join(str(c) for c in cols[:36])
            if len(cols) > 36:
                cshow += ", …"
            mshow = ", ".join(str(x) for x in ms[:20])
            if len(ms) > 20:
                mshow += ", …"
            lines.append(
                f"- **{tn}** — `semantic_role={role}` — UC: `{uc}` — columns: {cshow or '—'} — table measures: {mshow or '—'}"
            )
        meas = m.get("measures") or []
        if isinstance(meas, list) and meas:
            lines.append("**Measures (model-wide; DAX truncated):**")
            for mu in meas[:80]:
                if not isinstance(mu, dict):
                    continue
                nm = str(mu.get("name") or "?")
                dax = str(mu.get("dax_expression") or "")[:200].replace("\n", " ")
                lines.append(f"- `{nm}`: {dax or '—'}")
        lines.append("")
    return _truncate("\n".join(lines).strip() + "\n", 28_000)


def _visual_page_layout_guide(visual_rows: list[dict[str, Any]] | None, *, max_visuals: int = 200) -> str:
    """Group visuals by page for 1:1 layout instructions."""
    from collections import defaultdict

    pages: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in visual_rows or []:
        if not isinstance(r, dict):
            continue
        sf = str(r.get("source_file") or "")
        rp = str(r.get("report_name") or "")
        pg = str(r.get("page_name") or "")
        pages[(sf, rp, pg)].append(r)

    def sort_key(row: dict[str, Any]) -> tuple[float, float, float, str]:
        return (
            float(row.get("layout_y") or 0.0),
            float(row.get("layout_x") or 0.0),
            float(row.get("layout_z") or 0.0),
            str(row.get("visual_id") or ""),
        )

    blocks: list[dict[str, Any]] = []
    n = 0
    for (sf, rp, pg), rows in sorted(pages.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
        if n >= max_visuals:
            break
        rows_sorted = sorted(rows, key=sort_key)
        seq: list[dict[str, Any]] = []
        for r in rows_sorted:
            if n >= max_visuals:
                break
            seq.append(
                {
                    "visual_id": r.get("visual_id"),
                    "visual_type": r.get("visual_type"),
                    "intent_statement": r.get("intent_statement"),
                    "layout": {
                        "x": r.get("layout_x"),
                        "y": r.get("layout_y"),
                        "z": r.get("layout_z"),
                        "w": r.get("layout_w"),
                        "h": r.get("layout_h"),
                    },
                }
            )
            n += 1
        if seq:
            blocks.append(
                {
                    "source_file": sf or None,
                    "report_name": rp or None,
                    "page_name": pg or None,
                    "visual_count": len(seq),
                    "visuals_in_reading_order": seq,
                }
            )
    return _truncate(json.dumps(blocks, indent=2, default=str), 40_000)


def build_user_payload(
    *,
    dashboard_display_name: str,
    bindings: dict[str, str],
    ordered_model_tables: list[str],
    measure_names: list[str],
    starter_sql: str,
    column_names: list[str],
    visual_rows: list[dict[str, Any]] | None,
    canonical_models_json: str,
    max_chars: int = 120_000,
) -> str:
    parts: list[str] = []
    parts.append("## Target dashboard display name\n" + dashboard_display_name)
    parts.append(
        "## Semantic model digest (from .pbit DataModelSchema — authoritative)\n"
        "Use this for table/column/measure names, **calculated_view** tables, and **join relationships**. "
        "UC bindings show where each logical table lands in Unity Catalog.\n"
    )
    parts.append(_semantic_model_digest_markdown(canonical_models_json, bindings))
    parts.append("## Unity Catalog bindings (Power BI semantic table → catalog.schema.table)\n")
    parts.append(json.dumps({k: bindings.get(k) for k in ordered_model_tables}, indent=2))
    parts.append("\n## Measure names (flattened list, all tables)\n" + json.dumps(measure_names[:200], indent=2))
    parts.append("\n## DESCRIBE columns (primary bound UC table)\n" + json.dumps(column_names[:500], indent=2))
    parts.append("\n## Validated starter SQL (must be semantically respected)\n```sql\n" + starter_sql + "\n```")
    parts.append(
        "\n## Report pages & visuals in reading order (1:1 layout target)\n"
        "Recreate **one Lakeview page per entry** and **one widget per visual** in `visuals_in_reading_order` when possible.\n"
    )
    parts.append(_visual_page_layout_guide(visual_rows))
    kb = converter_knowledge_excerpt(max_chars=min(22_000, max(10_000, max_chars // 5)))
    if kb.strip():
        parts.append(
            "\n## Reference excerpts: PBI → Databricks AI/BI (pedrozanlorensi/pbi-aibi-converter knowledge)\n"
            "Use for visual→widget mapping, dataset vs widget-expression placement, widget spec versions, and pitfalls.\n"
        )
        parts.append(kb)
    parts.append("\n## Visual / layout rows (full detail, may be truncated)\n")
    parts.append(json.dumps(visual_rows or [], indent=2, default=str)[:55_000])
    parts.append("\n## Canonical model(s) JSON (full extraction; for cross-check)\n")
    parts.append(canonical_models_json[:42_000])
    body = "\n".join(parts)
    return _truncate(body, max_chars)


def parse_migration_response(assistant_text: str) -> tuple[dict[str, Any] | None, str | None]:
    raw = _extract_json_object(assistant_text)
    if not raw:
        return None, "Could not find a JSON object in the model response"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        fixed = _repair_json_once(raw)
        if fixed:
            try:
                data = json.loads(fixed)
            except json.JSONDecodeError:
                return None, f"Invalid JSON from model: {e}"
        else:
            return None, f"Invalid JSON from model: {e}"
    if not isinstance(data, dict):
        return None, "Model JSON root must be an object"
    return data, None


def apply_primary_dataset_sql_rail(
    lakeview: dict[str, Any],
    *,
    starter_sql: str,
    dataset_name: str = "primary",
) -> dict[str, Any]:
    """Force validated starter_sql into the primary dataset queryLines (mutates copy)."""
    import copy

    d = copy.deepcopy(lakeview)
    datasets = d.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        return d
    target: dict[str, Any] | None = None
    for ds in datasets:
        if isinstance(ds, dict) and (ds.get("name") or "").strip() == dataset_name:
            target = ds
            break
    if target is None and isinstance(datasets[0], dict):
        target = datasets[0]
    if target is None:
        return d
    target["queryLines"] = _sql_to_query_lines(starter_sql)
    return d


def run_llm_migration(
    wr: WorkspaceResources,
    *,
    serving_endpoint_name: str,
    dashboard_display_name: str,
    bindings: dict[str, str],
    ordered_model_tables: list[str],
    measure_names: list[str],
    starter_sql: str,
    column_names: list[str],
    visual_rows: list[dict[str, Any]] | None,
    canonical_models_json: str,
    temperature: float = 0.2,
    max_tokens: int = 16000,
    max_retries: int = 2,
) -> dict[str, Any]:
    """
    Call the workspace chat endpoint and return parsed lakeview + genie fields.

    On failure, ``ok`` is False and ``errors`` lists human-readable reasons.
    """
    out: dict[str, Any] = {
        "ok": False,
        "errors": [],
        "lakeview_dashboard": None,
        "genie_title": None,
        "genie_description": None,
        "genie_text_instruction": None,
        "sample_questions": [],
        "gap_notes": [],
        "raw_assistant": None,
    }
    user_prompt = build_user_payload(
        dashboard_display_name=dashboard_display_name,
        bindings=bindings,
        ordered_model_tables=ordered_model_tables,
        measure_names=measure_names,
        starter_sql=starter_sql,
        column_names=column_names,
        visual_rows=visual_rows,
        canonical_models_json=canonical_models_json,
    )

    assistant: str | None = None
    err: str | None = None
    for attempt in range(max_retries + 1):
        extra = ""
        if attempt > 0:
            extra = (
                "\n\nYour previous reply was not valid JSON or missed required keys. "
                "Reply with ONE JSON object only matching the schema (lakeview_dashboard + genie)."
            )
        assistant, err = wr.query_serving_endpoint_chat(
            serving_endpoint_name,
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt + extra,
            temperature=temperature if attempt == 0 else None,
            max_tokens=max_tokens,
        )
        if err:
            out["errors"].append(err)
            return out
        assert assistant is not None
        out["raw_assistant"] = assistant
        parsed, perr = parse_migration_response(assistant)
        if perr:
            out["errors"] = [perr]
            continue
        assert parsed is not None
        lv = parsed.get("lakeview_dashboard")
        gen = parsed.get("genie")
        if not isinstance(lv, dict) or "datasets" not in lv or "pages" not in lv:
            out["errors"] = ["lakeview_dashboard must be an object with datasets and pages"]
            continue
        if not isinstance(gen, dict):
            out["errors"] = ["genie must be an object"]
            continue
        title = (gen.get("title") or "").strip()
        desc = (gen.get("description") or "").strip()
        instr = (gen.get("text_instruction") or "").strip()
        if not title or not instr:
            out["errors"] = ["genie.title and genie.text_instruction are required"]
            continue
        sq = gen.get("sample_questions")
        questions: list[str] = []
        if isinstance(sq, list):
            for q in sq:
                if isinstance(q, str) and q.strip():
                    questions.append(q.strip())
        gaps = parsed.get("gap_notes")
        gap_list: list[str] = []
        if isinstance(gaps, list):
            for g in gaps:
                if isinstance(g, str) and g.strip():
                    gap_list.append(g.strip())

        lv_safe = apply_primary_dataset_sql_rail(lv, starter_sql=starter_sql, dataset_name="primary")
        out["ok"] = True
        out["errors"] = []
        out["lakeview_dashboard"] = lv_safe
        out["genie_title"] = title[:120]
        out["genie_description"] = desc[:4000]
        out["genie_text_instruction"] = instr
        out["sample_questions"] = questions[:12]
        out["gap_notes"] = gap_list
        return out

    return out
