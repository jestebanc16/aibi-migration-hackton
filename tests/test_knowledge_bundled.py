from aibi_migrator.knowledge import converter_knowledge_excerpt


def test_converter_knowledge_excerpt_non_empty() -> None:
    s = converter_knowledge_excerpt(max_chars=15_000)
    assert "CONVERSION_GUIDE" in s or "Step" in s or "Power BI" in s
    assert len(s) > 500
