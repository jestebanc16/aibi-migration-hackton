# aibi-tooling

Field tooling for **Power BI (.pbit) → Databricks AI/BI** migration, guided by [RULES.md](RULES.md).

## App

**Streamlit** (`app.py`):

- **Databricks auth is implicit** — `WorkspaceClient()` uses the Databricks Apps runtime identity (or your local `DATABRICKS_*` / profile). No workspace URL field and no sign-in buttons in the UI.
- **SQL warehouse** — dropdown populated from the SQL warehouses API for the signed-in identity; **Refresh** reloads the list.
- **Upload `.pbit`**, map model tables to `catalog.schema.table`, view **analysis** and download **JSON** (per file + estate rollup).

There is **no ad-hoc SQL runner** in the app.

### Local run

Configure `DATABRICKS_HOST` and credentials the SDK understands (see [Databricks auth](https://docs.databricks.com/en/dev-tools/auth.html)), then:

```bash
cd /path/to/aibi-tooling
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
streamlit run app.py
```

### Tests

```bash
pytest -q
```

### Databricks Apps (bundle)

```bash
databricks bundle deploy
```

See [databricks.yml](databricks.yml), [app.yaml](app.yaml), [resources/pbi_aibi_migrator.app.yml](resources/pbi_aibi_migrator.app.yml).

## References

See links at the end of [RULES.md](RULES.md).
