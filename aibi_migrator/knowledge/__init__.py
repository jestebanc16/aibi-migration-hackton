"""Vendored PBI→AI/BI reference excerpts (see README.md for source and updates)."""

from __future__ import annotations

import re
from pathlib import Path


def _strip_skill_frontmatter(text: str) -> str:
    t = text.lstrip("\ufeff")
    if not t.startswith("---"):
        return t
    parts = t.split("---", 2)
    if len(parts) >= 3:
        return parts[2].lstrip("\n")
    return t


def _remove_mcp_tools_section(text: str) -> str:
    """Upstream skill embeds MCP tool names; this app validates SQL separately."""
    return re.sub(
        r"## Available MCP Tools\s*\n[\s\S]*?(?=\n## )",
        "\n## Runtime note (this app)\n"
        "Validate dataset SQL against the workspace warehouse before publish; "
        "MCP tool names in the upstream reference do not apply here.\n\n",
        text,
        count=1,
    )


def converter_knowledge_excerpt(*, max_chars: int = 22_000) -> str:
    """
    Token-budget-friendly excerpts from vendored ``*.md`` files for LLM prompts.

    Priority: conversion mapping → DAX/SQL placement → AI/BI widget patterns.
    """
    root = Path(__file__).resolve().parent
    budget = max(4000, int(max_chars))
    chunks: list[str] = []

    def take(rel: str, cap: int, *, strip_frontmatter: bool = False, strip_mcp: bool = False) -> None:
        nonlocal budget
        p = root / rel
        if not p.is_file():
            return
        raw = p.read_text(encoding="utf-8", errors="replace")
        if strip_frontmatter:
            raw = _strip_skill_frontmatter(raw)
        if strip_mcp:
            raw = _remove_mcp_tools_section(raw)
        cap = min(cap, budget)
        if cap <= 0:
            return
        snippet = raw[:cap]
        if len(raw) > cap:
            snippet = snippet.rstrip() + "\n\n… [reference truncated for token budget] …\n"
        chunks.append(f"### Source: {rel}\n\n{snippet}")
        budget -= len(snippet)

    take("CONVERSION_GUIDE.md", min(11_000, max_chars * 6 // 10))
    take("DAX_TO_SQL_GUIDE.md", min(8000, max_chars * 25 // 100))
    take(
        "AIBI_DASHBOARD_SKILL.md",
        min(7000, max_chars * 15 // 100),
        strip_frontmatter=True,
        strip_mcp=True,
    )
    return "\n\n".join(chunks) if chunks else ""
