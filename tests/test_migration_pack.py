from aibi_migrator.migration_pack import (
    build_migration_pack,
    first_bound_fqtn,
    limited_select_sql,
    parse_uc_fqtn,
    primary_dataset_sql,
    quote_uc_fqtn,
    sanitize_lakeview_display_name,
    validation_probe_sql,
)


def test_sanitize_lakeview_display_name() -> None:
    assert sanitize_lakeview_display_name("Migrated AI/BI Dashboard") == "Migrated AI-BI Dashboard"
    assert "/" not in sanitize_lakeview_display_name("a/b/c")
    assert sanitize_lakeview_display_name("   ") == "Migrated dashboard"


def test_parse_uc_fqtn() -> None:
    assert parse_uc_fqtn("a.b.c") == ("a", "b", "c")
    assert parse_uc_fqtn("  cat.sch.tbl  ") == ("cat", "sch", "tbl")
    assert parse_uc_fqtn("a.b") is None
    assert parse_uc_fqtn("") is None


def test_quote_uc_fqtn() -> None:
    assert quote_uc_fqtn("cat.sch.my_table") == "`cat`.`sch`.`my_table`"
    assert quote_uc_fqtn("c.s.t") == "`c`.`s`.`t`"


def test_validation_probe_sql() -> None:
    sql = validation_probe_sql("c.s.t")
    assert sql == "SELECT * FROM `c`.`s`.`t` LIMIT 1"


def test_primary_dataset_sql_order() -> None:
    b = {"t1": "a.b.one", "t2": "a.b.two"}
    assert "one" in primary_dataset_sql(b, ["t1", "t2"])
    assert "two" in primary_dataset_sql(b, ["t2", "t1"])


def test_first_bound_fqtn() -> None:
    b = {"x": "a.b.c", "y": "d.e.f"}
    assert first_bound_fqtn(b, ["y", "x"]) == "d.e.f"
    assert first_bound_fqtn(b, ["nope"]) is None


def test_limited_select_sql() -> None:
    sql = limited_select_sql("a.b.t", ["id", "name"], limit=10)
    assert "`a`.`b`.`t`" in sql
    assert "LIMIT 10" in sql


def test_build_migration_pack_keys() -> None:
    p = build_migration_pack(
        dashboard_name="Dash",
        warehouse_id="wh",
        bindings={"x": "c.s.t"},
        ordered_model_tables=["x"],
        source_files=["f.pbit"],
        analysis_snapshots=[],
        validation_results=[],
    )
    assert p["warehouse_id"] == "wh"
    assert "starter_dataset_sql" in p
    assert "lakeview_placeholder" in p
    assert p["table_bindings"]["x"] == "c.s.t"
