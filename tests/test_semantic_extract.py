"""Semantic model extraction: relationships and calculated tables."""

from __future__ import annotations

import io
import json
import zipfile

from aibi_migrator.llm_migrate import _semantic_model_digest_markdown
from aibi_migrator.pbit_extract.parser import extract_pbit_canonical


def test_semantic_relationships_and_calculated_view() -> None:
    model = {
        "model": {
            "tables": [
                {
                    "name": "Fact",
                    "columns": [{"name": "id"}],
                    "measures": [],
                    "partitions": [{"name": "Fact", "source": {"type": "m", "expression": "let x=1 in x"}}],
                },
                {
                    "name": "CalcView",
                    "columns": [{"name": "n"}],
                    "measures": [{"name": "M1", "expression": "COUNTROWS(Fact)"}],
                    "partitions": [{"name": "CalcView", "source": {"type": "calculated", "expression": "ROW(\"n\",1)"}}],
                },
            ],
            "relationships": [
                {
                    "name": "FactToCalc",
                    "fromTable": "Fact",
                    "fromColumn": "id",
                    "toTable": "CalcView",
                    "toColumn": "n",
                    "isActive": True,
                }
            ],
        }
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("DataModelSchema", json.dumps(model).encode("utf-8"))
    m = extract_pbit_canonical("sem.pbit", buf.getvalue())
    assert len(m.semantic_relationships) == 1
    r0 = m.semantic_relationships[0]
    assert r0.from_table == "Fact" and r0.to_table == "CalcView"
    roles = {t.name: t.semantic_role for t in m.tables}
    assert roles["Fact"] == "data_table"
    assert roles["CalcView"] == "calculated_view"


def test_semantic_digest_includes_bindings() -> None:
    canonical = json.dumps(
        [
            {
                "source_file": "x.pbit",
                "model": {
                    "tables": [
                        {
                            "name": "Sales",
                            "semantic_role": "data_table",
                            "column_names": ["a"],
                            "measure_names": ["m"],
                        }
                    ],
                    "semantic_relationships": [],
                    "measures": [],
                },
            }
        ]
    )
    digest = _semantic_model_digest_markdown(canonical, {"Sales": "prod.raw.sales"})
    assert "Sales" in digest
    assert "prod.raw.sales" in digest
    assert "semantic_role=data_table" in digest
