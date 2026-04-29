import json

from aibi_migrator.genie_serialized import build_genie_serialized_space, suggest_genie_questions


def test_build_genie_serialized_space_json() -> None:
    s = build_genie_serialized_space(
        table_identifiers=["z.s.b", "a.s.a", "m.s.m"],
        sample_questions=["How many rows?", "Top 5?"],
        text_instruction="Hello",
    )
    d = json.loads(s)
    assert d["version"] == 2
    assert len(d["data_sources"]["tables"]) == 3
    ids = [t["identifier"] for t in d["data_sources"]["tables"]]
    assert ids == sorted(ids, key=str.lower)
    assert len(d["config"]["sample_questions"]) == 2


def test_suggest_genie_questions_cap() -> None:
    q = suggest_genie_questions(
        bound_tables=[("A", "c.s.a"), ("B", "c.s.b")],
        measure_names=["Revenue"],
        max_questions=5,
    )
    assert len(q) <= 5
    assert any("rows" in x.lower() for x in q)
