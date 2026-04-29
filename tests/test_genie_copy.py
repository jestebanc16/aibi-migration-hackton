from aibi_migrator.genie_copy import (
    build_genie_domain_description,
    build_genie_text_instruction,
    build_genie_title,
)


def test_build_genie_title_from_file() -> None:
    assert build_genie_title(["Regional_Sales.pbit"], "Dash") == "Regional_Sales — Genie"


def test_build_genie_title_multi() -> None:
    t = build_genie_title(["a.pbit", "b.pbit"], "Dash")
    assert "a" in t
    assert "+1 more" in t


def test_build_genie_title_fallback() -> None:
    assert build_genie_title([], "My / Dashboard") == "My - Dashboard — Genie"


def test_build_genie_domain_description() -> None:
    d = build_genie_domain_description(
        source_filenames=["demo.pbit"],
        pbi_to_uc=[("Orders", "c.s.orders")],
        measure_names=["Total Sales"],
        intent_statements=["Revenue by month"],
        max_chars=5000,
    )
    assert "demo.pbit" in d
    assert "Orders" in d and "c.s.orders" in d
    assert "Total Sales" in d
    assert "Revenue by month" in d


def test_build_genie_text_instruction_truncates() -> None:
    long = "x" * 2000
    short = build_genie_text_instruction(long, max_chars=100)
    assert len(short) <= 100
    assert short.endswith("…")
