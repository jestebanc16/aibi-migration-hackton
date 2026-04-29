import io
import json
import zipfile

from aibi_migrator.pbit_extract.parser import extract_pbit_canonical


def _minimal_pbit_bytes() -> bytes:
    model = {
        "model": {
            "tables": [
                {
                    "name": "Sales",
                    "columns": [{"name": "Amount"}],
                    "measures": [{"name": "Total", "expression": "SUM(Sales[Amount])"}],
                }
            ],
            "relationships": [],
        }
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("DataModelSchema", json.dumps(model).encode("utf-8"))
    return buf.getvalue()


def test_extract_minimal_pbit():
    raw = _minimal_pbit_bytes()
    m = extract_pbit_canonical("test.pbit", raw)
    assert m.raw_datamodel_present is True
    assert len(m.tables) == 1
    assert m.tables[0].name == "Sales"
    assert len(m.measures) == 1
    assert m.measures[0].name == "Total"


def test_extract_utf16_datamodel():
    model = {"model": {"tables": [{"name": "T", "columns": [], "measures": []}], "relationships": []}}
    text = json.dumps(model)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("DataModelSchema", text.encode("utf-16"))
    m = extract_pbit_canonical("u16.pbit", buf.getvalue())
    assert m.tables[0].name == "T"


def test_extract_report_layout_json_pages_and_positions():
    model = {"model": {"tables": [{"name": "T", "columns": [{"name": "x"}], "measures": []}], "relationships": []}}
    layout = {
        "sections": [
            {
                "displayName": "Summary",
                "visualContainers": [
                    {
                        "x": 10,
                        "y": 20,
                        "z": 0,
                        "width": 600,
                        "height": 200,
                        "config": json.dumps(
                            {"name": "v1", "singleVisual": {"visualType": "tableEx"}}
                        ),
                    }
                ],
            }
        ]
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("DataModelSchema", json.dumps(model).encode("utf-8"))
        z.writestr("Report/MyReport/Layout", json.dumps(layout).encode("utf-8"))
    m = extract_pbit_canonical("with_layout.pbit", buf.getvalue())
    assert len(m.visuals) == 1
    v = m.visuals[0]
    assert v.page_name == "Summary"
    assert v.visual_type == "tableEx"
    assert v.layout_x == 10 and v.layout_y == 20 and v.layout_w == 600 and v.layout_h == 200
