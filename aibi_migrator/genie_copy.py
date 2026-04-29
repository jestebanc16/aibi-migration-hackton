"""Genie Space title and description derived from imported .pbit context."""

from __future__ import annotations

from pathlib import Path

from aibi_migrator.migration_pack import sanitize_lakeview_display_name


def build_genie_title(source_filenames: list[str], fallback_dashboard_name: str) -> str:
    """
    Title similar to the imported file(s), e.g. ``Sales_Report — Genie``.
    ``fallback_dashboard_name`` is used when no filenames are provided (already Lakeview-safe).
    """
    stems: list[str] = []
    for fn in source_filenames:
        fn = (fn or "").strip()
        if not fn:
            continue
        stems.append(sanitize_lakeview_display_name(Path(fn).stem))
    stems = [s for s in stems if s]
    if not stems:
        return f"{sanitize_lakeview_display_name(fallback_dashboard_name)} — Genie"
    if len(stems) == 1:
        return f"{stems[0]} — Genie"
    return f"{stems[0]} (+{len(stems) - 1} more) — Genie"


def build_genie_domain_description(
    *,
    source_filenames: list[str],
    pbi_to_uc: list[tuple[str, str]],
    measure_names: list[str],
    intent_statements: list[str],
    max_chars: int = 3500,
) -> str:
    """
    Plain-text description for the Genie Space API: sources, UC mappings, measures, analytic intents.
    """
    lines: list[str] = []
    lines.append(
        "This Genie space was created from a Power BI (.pbit) template migrated to Databricks. "
        "Ask questions in natural language; always validate important SQL before production use."
    )

    if source_filenames:
        names = [Path(f).name for f in source_filenames if (f or "").strip()]
        if names:
            lines.append("")
            lines.append(f"Imported files: {', '.join(names)}")

    if pbi_to_uc:
        lines.append("")
        lines.append("Power BI model tables mapped to Unity Catalog:")
        for pbi, uc in pbi_to_uc[:40]:
            lines.append(f"  - {pbi}  ->  {uc}")

    if measure_names:
        lines.append("")
        lines.append("Semantic model measures (names from Power BI):")
        chunk = ", ".join(mn for mn in measure_names[:40] if (mn or "").strip())
        if chunk:
            lines.append(f"  {chunk}")

    if intent_statements:
        lines.append("")
        lines.append("Analytic intent from report visuals (heuristic summaries):")
        for it in intent_statements[:15]:
            t = (it or "").strip()
            if t:
                lines.append(f"  - {t}")

    text = "\n".join(lines).strip()
    if len(text) > max_chars:
        text = text[: max_chars - 30].rstrip() + "\n… (description truncated)"
    return text


def build_genie_text_instruction(full_description: str, max_chars: int = 1200) -> str:
    """Shorter text for serialized_space instructions (Genie still benefits from the full description on create)."""
    t = (full_description or "").strip()
    if len(t) <= max_chars:
        return t
    return t[: max_chars - 25].rstrip() + "…"
