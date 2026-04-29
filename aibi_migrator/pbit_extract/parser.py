"""Extract a canonical model from a Power BI template (.pbit) ZIP archive."""

from __future__ import annotations

import io
import json
import re
import zipfile
from typing import Any

from aibi_migrator.canonical.models import (
    DaxBucket,
    PbitCanonicalModel,
    PbitMeasureRef,
    PbitSemanticRelationship,
    PbitTableRef,
    PbitVisualIntent,
)


def _decode_json_bytes(data: bytes) -> dict[str, Any]:
    for encoding in ("utf-16", "utf-8-sig", "utf-8"):
        try:
            text = data.decode(encoding)
            return json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    raise ValueError("DataModelSchema is not valid JSON in utf-16, utf-8-sig, or utf-8")


def _find_datamodel_schema_member(z: zipfile.ZipFile) -> str | None:
    for name in z.namelist():
        base = name.rsplit("/", 1)[-1]
        if base == "DataModelSchema" or base.lower() == "datamodelschema":
            return name
    return None


def _parse_measures_from_tmsl(model: dict[str, Any]) -> list[PbitMeasureRef]:
    measures: list[PbitMeasureRef] = []
    tables = model.get("tables") or model.get("Tables") or []
    for table in tables:
        if not isinstance(table, dict):
            continue
        for m in table.get("measures") or table.get("Measures") or []:
            if not isinstance(m, dict):
                continue
            name = m.get("name") or m.get("Name") or "measure"
            expr = m.get("expression") or m.get("Expression")
            if isinstance(expr, str):
                dax = expr.strip()
            else:
                dax = None
            measures.append(PbitMeasureRef(name=str(name), dax_expression=dax, dax_bucket=None))
    return measures


def _infer_semantic_role_for_table(table: dict[str, Any]) -> str:
    """Classify semantic model table: calculated tables behave like persisted views in the model."""
    partitions = table.get("partitions") or table.get("Partitions") or []
    types_lower: set[str] = set()
    for p in partitions:
        if not isinstance(p, dict):
            continue
        src = p.get("source") or p.get("Source") or {}
        if not isinstance(src, dict):
            continue
        st = str(src.get("type") or src.get("Type") or "").strip().lower()
        if st:
            types_lower.add(st)
    if "calculated" in types_lower:
        return "calculated_view"
    dataish = {"m", "query", "structured", "azurestorage", "web", "entity", "policy"}
    if types_lower & dataish:
        return "data_table"
    cols = table.get("columns") or table.get("Columns") or []
    if isinstance(cols, list) and cols:
        return "data_table"
    return "unknown"


def _parse_tables_from_tmsl(model: dict[str, Any]) -> list[PbitTableRef]:
    tables_out: list[PbitTableRef] = []
    tables = model.get("tables") or model.get("Tables") or []
    for table in tables:
        if not isinstance(table, dict):
            continue
        name = table.get("name") or table.get("Name") or "table"
        hidden = bool(table.get("isHidden") or table.get("IsHidden"))
        cols: list[str] = []
        for c in table.get("columns") or table.get("Columns") or []:
            if isinstance(c, dict):
                cn = c.get("name") or c.get("Name")
                if cn:
                    cols.append(str(cn))
        measure_names = [
            str(m.get("name") or m.get("Name") or "")
            for m in (table.get("measures") or table.get("Measures") or [])
            if isinstance(m, dict)
        ]
        measure_names = [x for x in measure_names if x]
        role = _infer_semantic_role_for_table(table)
        tables_out.append(
            PbitTableRef(
                name=str(name),
                is_hidden=hidden,
                column_names=cols,
                measure_names=measure_names,
                semantic_role=role,
            )
        )
    return tables_out


def _parse_semantic_relationships_from_tmsl(model: dict[str, Any]) -> list[PbitSemanticRelationship]:
    out: list[PbitSemanticRelationship] = []
    rels = model.get("relationships") or model.get("Relationships") or []
    if not isinstance(rels, list):
        return out
    for r in rels:
        if not isinstance(r, dict):
            continue
        ft = r.get("fromTable") or r.get("FromTable")
        fc = r.get("fromColumn") or r.get("FromColumn")
        tt = r.get("toTable") or r.get("ToTable")
        tc = r.get("toColumn") or r.get("ToColumn")
        frm = r.get("from") or r.get("From")
        if isinstance(frm, dict):
            ft = ft or frm.get("table") or frm.get("Table")
            fc = fc or frm.get("column") or frm.get("Column")
        to = r.get("to") or r.get("To")
        if isinstance(to, dict):
            tt = tt or to.get("table") or to.get("Table")
            tc = tc or to.get("column") or to.get("Column")
        if not ft or not tt:
            continue
        nm = r.get("name") or r.get("Name")
        active = r.get("isActive", r.get("IsActive", True))
        if isinstance(active, str):
            active = str(active).lower() in ("true", "1", "yes")
        cf = r.get("crossFilteringBehavior") or r.get("CrossFilteringBehavior")
        out.append(
            PbitSemanticRelationship(
                name=str(nm) if nm else None,
                from_table=str(ft),
                from_column=str(fc) if fc else None,
                to_table=str(tt),
                to_column=str(tc) if tc else None,
                is_active=bool(active),
                cross_filtering_behavior=str(cf) if cf else None,
            )
        )
    return out


def _rls_notes(model: dict[str, Any]) -> tuple[bool, list[str]]:
    notes: list[str] = []
    roles = model.get("roles") or model.get("Roles") or []
    if isinstance(roles, list) and roles:
        notes.append(f"Model defines {len(roles)} role(s); RLS mapping is out of scope for v1 automation.")
        return True, notes
    return False, notes


def _visual_type_to_intent(visual_type: str | None, title: str | None) -> str:
    vt = (visual_type or "visual").lower()
    ttl = (title or "").strip()
    base = f"Analytic intent: summarize or compare metrics using a {vt} visual"
    if ttl:
        return f"{base} (title hint: {ttl})"
    return base


def _parse_visual_config(config_raw: Any) -> tuple[str | None, str | None, str | None]:
    """
    Parse Power BI visualContainer ``config`` (stringified JSON or dict).
    Returns (visual_type, title_text, stable_name).
    """
    cfg: dict[str, Any] | None = None
    if isinstance(config_raw, str) and config_raw.strip():
        try:
            cfg = json.loads(config_raw)
        except json.JSONDecodeError:
            return None, None, None
    elif isinstance(config_raw, dict):
        cfg = config_raw
    else:
        return None, None, None

    name = cfg.get("name")
    name_s = str(name) if name is not None else None

    sv = cfg.get("singleVisual")
    if isinstance(sv, dict):
        vt = sv.get("visualType") or sv.get("VisualType")
        if isinstance(vt, str):
            vtype = vt
        else:
            vtype = None
    else:
        vtype = cfg.get("visualType") if isinstance(cfg.get("visualType"), str) else None

    title: str | None = None
    for path in (
        ("singleVisual", "vcObjects", "title", "properties", "text", "expr", "Literal", "Value"),
    ):
        cur: Any = cfg
        for key in path:
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(key)
        if isinstance(cur, str) and cur.strip():
            title = cur.strip()
            break

    if title is None:
        t0 = cfg.get("title")
        if isinstance(t0, dict):
            tx = t0.get("text")
            if isinstance(tx, str) and tx.strip():
                title = tx.strip()

    return (vtype if isinstance(vtype, str) else None), title, name_s


def _num(v: Any) -> float | None:
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _extract_visuals_from_layout_json(root: dict[str, Any], report_name: str) -> list[PbitVisualIntent]:
    """Structured Report/Layout JSON: sections[].visualContainers[]."""
    out: list[PbitVisualIntent] = []
    sections = root.get("sections")
    if not isinstance(sections, list):
        return out

    for sec in sections:
        if not isinstance(sec, dict):
            continue
        page_name = sec.get("displayName") or sec.get("name") or sec.get("Name")
        page_s = str(page_name).strip() if page_name else None

        containers = sec.get("visualContainers") or sec.get("VisualContainers") or []
        if not isinstance(containers, list):
            continue

        for ci, vc in enumerate(containers):
            if not isinstance(vc, dict):
                continue
            vt, title, cfg_name = _parse_visual_config(vc.get("config"))
            if not vt:
                continue
            lx = _num(vc.get("x"))
            ly = _num(vc.get("y"))
            lz = _num(vc.get("z"))
            lw = _num(vc.get("width"))
            lh = _num(vc.get("height"))
            vid = f"{report_name}:{cfg_name}" if cfg_name else f"{report_name}:visual:{page_s or 'page'}:{ci}"
            out.append(
                PbitVisualIntent(
                    visual_id=vid,
                    report_name=report_name,
                    page_name=page_s,
                    visual_type=vt,
                    intent_statement=_visual_type_to_intent(vt, title),
                    layout_x=lx,
                    layout_y=ly,
                    layout_z=lz,
                    layout_w=lw,
                    layout_h=lh,
                )
            )
    return out


def _extract_visuals_from_report_layout_regex(text: str, report_name: str) -> list[PbitVisualIntent]:
    """Regex fallback when Layout is not structured JSON."""
    intents: list[PbitVisualIntent] = []
    for i, m in enumerate(re.finditer(r'"visualType"\s*:\s*"([^"]+)"', text)):
        vt = m.group(1)
        snippet = text[max(0, m.start() - 200) : m.start() + 200]
        title_m = re.search(r'"title"\s*:\s*\{[^}]*"text"\s*:\s*"([^"]*)"', snippet)
        title = title_m.group(1) if title_m else None
        vid = f"{report_name}:visual:{i}"
        intents.append(
            PbitVisualIntent(
                visual_id=vid,
                report_name=report_name,
                page_name=None,
                visual_type=vt,
                intent_statement=_visual_type_to_intent(vt, title),
            )
        )
    return intents


def _extract_visuals_from_report_layout(report_bytes: bytes, report_name: str) -> list[PbitVisualIntent]:
    """Parse Report/Layout: prefer JSON (pages + positions); fall back to regex on raw text."""
    try:
        text = report_bytes.decode("utf-8-sig", errors="replace")
    except Exception:
        return []

    stripped = text.lstrip("\ufeff").strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            root = json.loads(stripped)
        except json.JSONDecodeError:
            root = None
        if isinstance(root, dict):
            structured = _extract_visuals_from_layout_json(root, report_name)
            if structured:
                return structured

    return _extract_visuals_from_report_layout_regex(text, report_name)


def extract_pbit_canonical(file_name: str, file_bytes: bytes) -> PbitCanonicalModel:
    warnings: list[str] = []
    visuals: list[PbitVisualIntent] = []
    tables: list[PbitTableRef] = []
    measures: list[PbitMeasureRef] = []
    relationships_count = 0
    has_rls = False
    rls_notes: list[str] = []
    raw_present = False
    semantic_relationships: list[PbitSemanticRelationship] = []

    try:
        z = zipfile.ZipFile(io.BytesIO(file_bytes))
    except zipfile.BadZipFile as e:
        return PbitCanonicalModel(
            source_file_name=file_name,
            extraction_warnings=[f"Invalid ZIP (.pbit): {e}"],
        )

    with z:
        dm_path = _find_datamodel_schema_member(z)
        if not dm_path:
            warnings.append("No DataModelSchema member found in archive.")
        else:
            raw = z.read(dm_path)
            raw_present = True
            try:
                root = _decode_json_bytes(raw)
            except ValueError as e:
                warnings.append(f"Could not parse DataModelSchema as JSON: {e}")
                root = {}
            model = root.get("model") or root.get("Model") or root
            if not isinstance(model, dict):
                warnings.append("Unexpected DataModelSchema structure: missing model object.")
            else:
                tables = _parse_tables_from_tmsl(model)
                measures = _parse_measures_from_tmsl(model)
                semantic_relationships = _parse_semantic_relationships_from_tmsl(model)
                relationships_count = len(semantic_relationships)
                has_rls, rls_notes = _rls_notes(model)

        # Layout under Report/<reportId>/Layout
        for name in z.namelist():
            norm = name.replace("\\", "/")
            parts = norm.split("/")
            if len(parts) < 3 or parts[0] != "Report" or parts[-1] != "Layout":
                continue
            report_name = parts[1] if len(parts) > 2 else "report"
            try:
                layout_bytes = z.read(name)
                visuals.extend(_extract_visuals_from_report_layout(layout_bytes, report_name))
            except KeyError:
                pass

    if not visuals and tables:
        warnings.append("No visuals parsed from Layout; intent may be model-only until report layout is supported.")

    return PbitCanonicalModel(
        source_file_name=file_name,
        tables=tables,
        measures=measures,
        semantic_relationships=semantic_relationships,
        relationships_count=relationships_count,
        has_rls_hints=has_rls,
        rls_notes=rls_notes,
        visuals=visuals,
        raw_datamodel_present=raw_present,
        extraction_warnings=warnings,
    )
