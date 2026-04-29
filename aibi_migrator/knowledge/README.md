# Bundled conversion knowledge

Markdown files in this folder are copied from the public repository **[pbi-aibi-converter](https://github.com/pedrozanlorensi/pbi-aibi-converter)** (`app_for_conversions/knowledge/`), used as **reference excerpts** for LLM-assisted Lakeview + Genie generation in `aibi_migrator.llm_migrate`.

- [CONVERSION_GUIDE.md](https://github.com/pedrozanlorensi/pbi-aibi-converter/blob/master/app_for_conversions/knowledge/CONVERSION_GUIDE.md) — PBI → AI/BI dashboard mapping
- [DAX_TO_SQL_GUIDE.md](https://github.com/pedrozanlorensi/pbi-aibi-converter/blob/master/app_for_conversions/knowledge/DAX_TO_SQL_GUIDE.md) — DAX → Spark SQL placement
- [AIBI_DASHBOARD_SKILL.md](https://github.com/pedrozanlorensi/pbi-aibi-converter/blob/master/app_for_conversions/knowledge/AIBI_DASHBOARD_SKILL.md) — AI/BI widget patterns (upstream file may include Cursor skill frontmatter; we strip it at load time)

Refresh vendored copies:

```bash
cd aibi_migrator/knowledge
curl -fsSL -O "https://raw.githubusercontent.com/pedrozanlorensi/pbi-aibi-converter/master/app_for_conversions/knowledge/CONVERSION_GUIDE.md"
curl -fsSL -O "https://raw.githubusercontent.com/pedrozanlorensi/pbi-aibi-converter/master/app_for_conversions/knowledge/DAX_TO_SQL_GUIDE.md"
curl -fsSL -O "https://raw.githubusercontent.com/pedrozanlorensi/pbi-aibi-converter/master/app_for_conversions/knowledge/AIBI_DASHBOARD_SKILL.md"
```
