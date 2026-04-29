"""Minimal Lakeview-style JSON skeleton for v1 (extend with widget specs after SQL validation)."""

from __future__ import annotations

import json
from typing import Any


def build_placeholder_dashboard_spec(
    display_name: str,
    warehouse_id: str,
    dataset_sql: str,
    parent_path: str = "/Workspace/Users",
) -> dict[str, Any]:
    """
    Returns a dict suitable for serialization to Lakeview dashboard APIs.
    v1: placeholder structure; replace with full widget+dataset contract from AI/BI spec.
    """
    return {
        "display_name": display_name,
        "parent_path": parent_path,
        "warehouse_id": warehouse_id,
        "lifecycle_note": "v1 placeholder — replace with validated serialized_dashboard per AI/BI widget rules",
        "datasets": [
            {
                "name": "primary_dataset",
                "warehouse_id": warehouse_id,
                "query": dataset_sql,
            }
        ],
    }


def dumps_spec(spec: dict[str, Any]) -> str:
    return json.dumps(spec, indent=2)
