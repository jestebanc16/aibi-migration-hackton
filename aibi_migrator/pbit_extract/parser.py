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
        tables_out.append(
            PbitTableRef(name=str(name), is_hidden=hidden, column_names=cols, measure_names=measure_names)
        )
    return tables_out


def _relationship_count(model: dict[str, Any]) -> int:
    rels = model.get("relationships") or model.get("Relationships") or []
    if isinstance(rels, list):
        return len(rels)
    return 0


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


def _extract_visuals_from_report_layout(report_bytes: bytes, report_name: str) -> list[PbitVisualIntent]:
    """Best-effort: scan Layout JSON for visual containers (structure varies by PBIX version)."""
    intents: list[PbitVisualIntent] = []
    try:
        text = report_bytes.decode("utf-8-sig", errors="replace")
    except Exception:
        return intents
    # Heuristic: find "visualType" and nearby config
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


def extract_pbit_canonical(file_name: str, file_bytes: bytes) -> PbitCanonicalModel:
    warnings: list[str] = []
    visuals: list[PbitVisualIntent] = []
    tables: list[PbitTableRef] = []
    measures: list[PbitMeasureRef] = []
    relationships_count = 0
    has_rls = False
    rls_notes: list[str] = []
    raw_present = False

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
                relationships_count = _relationship_count(model)
                has_rls, rls_notes = _rls_notes(model)

        # Layout under Report/Layout
        for name in z.namelist():
            if name.endswith("/Layout") and "/Report/" in name.replace("\\", "/"):
                parts = name.replace("\\", "/").split("/")
                report_name = "report"
                if "Report" in parts:
                    idx = parts.index("Report")
                    if idx + 1 < len(parts):
                        report_name = parts[idx + 1]
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
        relationships_count=relationships_count,
        has_rls_hints=has_rls,
        rls_notes=rls_notes,
        visuals=visuals,
        raw_datamodel_present=raw_present,
        extraction_warnings=warnings,
    )
