"""Build Genie Space ``serialized_space`` JSON (API version 2)."""

from __future__ import annotations

import json
import uuid
from typing import Any


def _genie_uid() -> str:
    return uuid.uuid4().hex


def build_genie_serialized_space(
    *,
    table_identifiers: list[str],
    sample_questions: list[str],
    text_instruction: str,
) -> str:
    """
    Minimal valid serialized_space for POST /api/2.0/genie/spaces.
    ``table_identifiers`` are catalog.schema.table strings (no backticks).
    """
    tables: list[dict[str, Any]] = []
    for tid in table_identifiers:
        t = tid.strip()
        if not t:
            continue
        tables.append({"identifier": t, "description": [f"Unity Catalog table `{t}` (from PBI → AI/BI migrator)."]})

    # Genie API requires data_sources.tables sorted by identifier (proto validation).
    tables.sort(key=lambda row: str(row.get("identifier", "")).lower())

    questions: list[dict[str, Any]] = []
    for q in sample_questions:
        qt = (q or "").strip()
        if not qt:
            continue
        questions.append({"id": _genie_uid(), "question": [qt]})

    payload: dict[str, Any] = {
        "version": 2,
        "config": {"sample_questions": questions},
        "data_sources": {"tables": tables, "metric_views": []},
        "instructions": {
            "text_instructions": [{"id": _genie_uid(), "content": [text_instruction]}],
            "example_question_sqls": [],
            "sql_functions": [],
            "join_specs": [],
            "sql_snippets": {"filters": [], "expressions": [], "measures": []},
        },
        "benchmarks": {"questions": []},
    }
    return json.dumps(payload)


def suggest_genie_questions(
    *,
    bound_tables: list[tuple[str, str]],
    measure_names: list[str],
    max_questions: int = 10,
) -> list[str]:
    """
    ``bound_tables``: (pbi_table_name, catalog.schema.table) pairs.
    """
    out: list[str] = []
    for pbi, fq in bound_tables[:6]:
        short = fq.split(".")[-1] if fq else pbi
        out.append(f"How many rows are in {short}?")
        out.append(f"Show 10 sample rows from {short}.")
        out.append(f"What are the key dimensions and metrics available in {short}?")
    for m in measure_names[:5]:
        mn = (m or "").strip()
        if mn:
            out.append(f"How would I approximate the Power BI measure '{mn}' in SQL?")
    dedup: list[str] = []
    seen: set[str] = set()
    for q in out:
        k = q.lower()
        if k not in seen and q.strip():
            seen.add(k)
            dedup.append(q.strip())
        if len(dedup) >= max_questions:
            break
    return dedup
