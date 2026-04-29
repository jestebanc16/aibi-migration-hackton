# RULES.md — Power BI to Databricks AI/BI Migration Rules

## Purpose

This document defines the operating rules, architectural truths, decision criteria, and output expectations for a migration app that analyzes an existing Power BI estate and recommends or generates migrations to Databricks AI/BI Dashboards.[cite:13][cite:20][cite:25][cite:28]

**Generation** of target Lakeview dashboards and Genie spaces is **LLM-assisted** (user-selected workspace **chat serving endpoint**), **grounded** on extracted `.pbit` facts and validated Unity Catalog SQL; **extraction** of those facts remains **deterministic** (ZIP and JSON parse), not model-inferred.

The app must treat migration as **both** a semantic and architectural translation problem **and** an opportunity for **maximum attainable layout- and visual-level fidelity** to source Power BI reports (every page and visual represented in the target model where Databricks AI/BI supports it), without claiming guaranteed pixel-identical runtime or full feature parity.[cite:14][cite:23][cite:25]

---

## Non-Negotiable Truths

### 1. Semantic meaning is the center of gravity

In Power BI, business meaning is primarily expressed through semantic models that contain tables, relationships, measures, hierarchies, and security definitions.[cite:24][cite:27][cite:30]

In Databricks, the closest target for that meaning is curated Lakehouse data plus Unity Catalog business semantics, including governed metrics, metric views, and related semantic metadata.[cite:25][cite:28][cite:31]

The app must therefore resolve semantic meaning before attempting to generate any target dashboard, visual, or app.[cite:23][cite:25][cite:28]

### 2. Platform fidelity differs from design intent

Power BI dashboards are downstream artifacts built on top of reports, semantic models, refresh settings, service objects, and workspace distribution.[cite:1][cite:2][cite:27]

Databricks AI/BI Dashboards are Lakehouse-native dashboard artifacts built directly against Delta tables, SQL, and Unity Catalog semantics inside the Databricks platform.[cite:10][cite:13][cite:28]

There is therefore **no guarantee** of pixel-identical runtime or feature-for-feature behavior, but **page-for-page and visual-for-visual target mapping is a first-class migration mode**: the app must attempt to reproduce report structure, visual inventory, ordering, titles, and layout coordinates where extractable, and must document deltas in a **parity or gap manifest** rather than treat layout-level migration as out of scope by default.[cite:14][cite:23]

### 3. Intent and visuals are complementary migration units

Each source artifact must be decomposed into business questions, KPIs, dimensions, time grain, filters, interaction patterns, and audience before target generation begins.[cite:23][cite:25]

**Analytic intent** (captured per visual) drives semantic translation, SQL validation, and metric parity. **Visuals and pages** are simultaneously **first-class migration units** for the fidelity track: each extracted visual must map to a target dashboard visual or an explicitly recorded gap, not only to an intent string.[cite:23]

### 4. Governance must move down-stack

If governance currently depends on Power BI workspaces, semantic-model permissions, or service-level controls, the app must attempt to remap that logic into Unity Catalog permissions and governed semantic objects wherever possible.[cite:1][cite:25][cite:28]

---

## Scope Rules

### In scope

- Inventorying and analyzing Power BI semantic models, reports, dashboards, refresh modes, and distribution constructs.[cite:1][cite:24][cite:30]
- Mapping business logic from DAX, Power Query, and semantic models into Databricks target objects.[cite:2][cite:23][cite:28]
- Recommending one of four outcomes for each artifact: `migrate_now`, `migrate_later`, `retain_in_power_bi`, or `redesign_before_migration`.[cite:14][cite:23]
- Generating a semantic translation plan and dashboard rebuild plan for qualified migration candidates.[cite:23][cite:25]
- Emitting a **session estate rollup** when multiple source packages are analyzed in one session, aligned with the required estate-level outputs.[cite:14][cite:23]
- **Automated report-structure and layout fidelity** for analyzed **interactive** Power BI reports: the target AI/BI dashboard must **cover every extracted report page and visual**—ordering, grouping, titles, and **layout coordinates where extractable**—mapped to the closest supported Lakeview widget types and encodings.[cite:11][cite:13][cite:14]
- **Definitions (product language):** **Blind** means the default pipeline derives pages, visuals, and layout **without a manual wireframe step**: the **user-selected Databricks foundation / chat serving endpoint** proposes Lakeview structure and Genie copy from **deterministically extracted** `.pbit` artifacts (ZIP, `DataModelSchema`, report `Layout` JSON) plus validated Unity Catalog bindings—operators review before publish. **Pixel-perfect** means **maximum fidelity subject to Databricks AI/BI capabilities**; gaps (custom visuals, themes or fonts, some interaction patterns, print or **paginated (RDL)** parity, and similar) must appear in a **per-report parity or gap manifest** (including **LLM vs platform** limitations where relevant) and feed the **parity-validation backlog**.[cite:11][cite:14][cite:23]

### Out of scope

- Assumption that all DAX can be translated directly to SQL.[cite:23][cite:24]
- Automatic retirement of paginated or highly polished executive reporting without explicit validation.[cite:11][cite:14]
- **Version 1:** Automated translation of Power BI row-level security into equivalent Unity Catalog enforcement with a claim of parity (document and plan only).[cite:23][cite:25]

---

## Source Architecture Rules

### Source object model

The app must model the Power BI estate as distinct layers rather than as a flat list of reports.[cite:1][cite:24][cite:30]

The source model must include at least the following object types:

- `data_source`
- `power_query_transformation`
- `dataflow`
- `semantic_model`
- `table`
- `relationship`
- `measure`
- `hierarchy`
- `security_rule`
- `refresh_configuration`
- `report`
- `report_page`
- `visual`
- `dashboard`
- `dashboard_tile`
- `workspace`
- `distribution_app` [cite:1][cite:24][cite:27][cite:30]

### Source extraction requirements

For every source artifact, the app must extract or infer:

- Business domain
- Audience/persona
- Data sources
- Storage mode or refresh mode
- Dimensions and grain
- Measures and calculation logic
- Relationships and join paths
- Filters and slicers
- Security behavior
- Visual interaction behavior
- Operational criticality
- Export or print requirements [cite:23][cite:24][cite:27][cite:30]

The app must convert each visual into an **analytic intent statement** such as: `monthly gross margin by region with product-category filter and YoY comparison`.[cite:23]

### Migration generation (implementation standard)

- **Lakeview dashboard JSON** and **Genie narrative** (title, description, text instructions, suggested questions, and gap reasoning) are **LLM-assisted** using a **user-chosen Databricks chat-capable serving endpoint** (foundation or custom), invoked with **OpenAI-compatible** chat completions against that endpoint.
- The model must be **grounded** on **deterministic extraction** only: serialized canonical model(s), report/visual layout facts, Unity Catalog bindings, `DESCRIBE` column lists, and **warehouse-validated** starter SQL. It must **not invent** tables, columns, or measures that are absent from that context.
- Outputs must be **structured** (JSON): a `lakeview_dashboard` object (full `datasets` + `pages` shape consumable by the Lakeview API) and a `genie` object (`title`, `description`, `text_instruction`, optional `sample_questions`). **Per-visual heuristic intent strings** remain useful as **extracted hints** in the payload but are not the sole generation path.
- **Human review** before publish: operators choose warehouse, endpoint, and bindings in the app; invalid or non-JSON LLM output fails closed with actionable errors (no silent publish).
- **Heuristic-only dashboard assembly** may remain available only when explicitly disabled for LLM (e.g. environment flag for fallback); it is not the primary path when LLM migration is enabled.

---

## Target Architecture Rules

### Target object model

The app must model the Databricks target as a Lakehouse-first architecture.[cite:13][cite:20][cite:28]

The target model must include at least the following object types:

- `delta_table`
- `curated_view`
- `sql_transformation`
- `metric_view`
- `business_semantic_definition`
- `unity_catalog_permission`
- `dashboard_definition`
- `dashboard_page`
- `dashboard_visual`
- `genie_space_reference`
- `app_package` [cite:13][cite:25][cite:28][cite:31]

### Target design principles

Reusable business logic should be placed in curated Delta tables, SQL views, metric views, or Unity Catalog semantic objects rather than embedded repeatedly in dashboards.[cite:25][cite:28][cite:31]

Dashboard definitions should contain presentation logic, lightweight filters, and user interaction behavior, but not the primary copy of reusable KPI logic.[cite:13][cite:19][cite:28]

---

## Equivalence Rules

The app must use the following conceptual mappings, while recognizing that they are not feature-identical.[cite:20][cite:25][cite:28]


| Power BI concept              | Preferred Databricks target                                                      | Rule                                                                                                                                                    |
| ----------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Semantic model [cite:27]      | Curated Delta tables/views + Unity Catalog business semantics [cite:28][cite:31] | Treat as the primary semantic migration unit                                                                                                            |
| DAX measure [cite:24]         | Metric view, SQL metric, or precomputed transformation [cite:28]                 | Classify before translating                                                                                                                             |
| Power Query/Dataflow [cite:2] | SQL transformation / ETL-ELT pipeline on Databricks [cite:20][cite:23]           | Move reusable shaping upstream                                                                                                                          |
| Report page [cite:24]         | AI/BI dashboard page or Genie-enabled analytic flow [cite:13][cite:32]           | Map each source page to a dashboard page with visual-for-visual placement where Lakeview supports it; document unsupported cases in the parity manifest |
| Dashboard tile [cite:2]       | Dashboard visual/section [cite:13][cite:19]                                      | Recompose when source dashboard aggregates multiple reports                                                                                             |
| Workspace/app [cite:1]        | Workspace + permissions + Databricks Apps packaging [cite:13][cite:14]           | Treat as deployment and access model                                                                                                                    |


---

## Classification Rules

### Artifact dispositions

Every analyzed artifact must end in exactly one disposition for **lifecycle and reporting**:

- `migrate_now`
- `migrate_later`
- `retain_in_power_bi`
- `redesign_before_migration` [cite:14][cite:23]

The `**effective_disposition`** (after binding and gates) is the controlling disposition for **automation** such as dashboard generation; `**recommended_disposition`** remains the scored hypothesis before binding.

### Recommended default logic


| Pattern                                                         | Default disposition                                           | Reason                                                                                                                                                                              |
| --------------------------------------------------------------- | ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Simple KPI dashboard on Databricks-backed data [cite:20]        | `migrate_now` [cite:13][cite:25]                              | Strong Lakehouse fit and lower semantic risk                                                                                                                                        |
| DirectQuery-heavy operational dashboard [cite:5]                | `migrate_now` [cite:10][cite:13]                              | Good candidate for Lakehouse-native live analytics                                                                                                                                  |
| DAX-heavy finance or planning model [cite:24][cite:33]          | `migrate_later` or `retain_in_power_bi` [cite:14][cite:23]    | High translation and validation burden                                                                                                                                              |
| Paginated or print-perfect reporting [cite:11][cite:14]         | `retain_in_power_bi` [cite:11][cite:14]                       | Front-end mismatch for **paginated (RDL)** and print-centric rigs vs interactive AI/BI; in-scope fidelity rules here target **interactive** reports, not identical paginated output |
| Analyst-facing exploratory dashboard [cite:6][cite:10]          | `migrate_now` [cite:13][cite:32]                              | Strong fit for AI-assisted workflows                                                                                                                                                |
| Executive dashboard with heavy custom polish [cite:11][cite:19] | `redesign_before_migration` or `retain_in_power_bi` [cite:14] | Requires explicit UX simplification decision                                                                                                                                        |


### Recommended disposition vs effective disposition

- `**recommended_disposition`**: The outcome from classification and scoring **before** Unity Catalog binding is complete (heuristics-only readiness).
- `**effective_disposition`**: The outcome **after** applying binding completeness and critical coverage rules (for example, fact or dimension tables required by pinned visuals or measures must map to fully qualified `catalog.schema.table` or governed views).

The app must persist **both** values per artifact whenever binding is part of the workflow. If `recommended_disposition` is `migrate_now` but required tables remain unmapped or ambiguous, `effective_disposition` must be downgraded (typically to `migrate_later`) and `**binding_blockers`** must list the concrete gaps.

**Automatic dashboard generation** (including **data-bound** Lakeview datasets) is permitted only when `effective_disposition` is `migrate_now`, all refuse-when-ambiguous gates pass, SQL validation succeeds for queries that execute against Unity Catalog, and—where the target AI/BI experience **intentionally simplifies** source UX relative to a fidelity-first layout—the operator has recorded **explicit sign-off** per the UX and product rules below. **Layout skeletons** or a **full page-and-visual tree** (including placeholders where SQL is not yet validated) may be emitted per product rules once the canonical model and extractable layout exist, without bypassing gates for **published** data-bound datasets.

---

## Scoring Rules

The app must score each artifact on the following axes before making a recommendation.[cite:14][cite:23]

### 1. Semantic complexity score

Estimate the complexity of migration based on measure count, relationship density, hierarchy usage, time intelligence, and DAX sophistication.[cite:24][cite:33]

Higher score means lower short-term migration readiness.[cite:23][cite:24]

### 2. Lakehouse affinity score

Estimate how naturally the artifact fits Databricks based on whether the data already resides in Databricks, can be curated there, or depends on Lakehouse-native patterns.[cite:20][cite:25]

Higher score means stronger migration fit.[cite:20][cite:28]

### 3. Front-end dependency score

Estimate how much the artifact depends on Power BI-specific front-end strengths such as custom visuals, polished layouts, print requirements, or advanced report presentation behavior.[cite:11][cite:14][cite:19]

Higher score means lower replacement readiness.[cite:11][cite:14] A higher score **lowers automatic parity confidence** but **does not** excuse omitting layout or visual mapping attempts: the app must still **attempt** mapped visuals and record gaps in the parity manifest.[cite:11][cite:14]

### 4. Real-time value score

Estimate whether current refresh or DirectQuery pain would be materially reduced by moving to Databricks AI/BI over live or near-real-time Lakehouse objects.[cite:3][cite:10][cite:13]

Higher score means stronger migration value.[cite:10][cite:13]

### 5. AI leverage score

Estimate whether natural-language analytics, governed semantic context, or AI-assisted dashboarding would materially improve user outcomes.[cite:10][cite:25][cite:28]

Higher score means stronger AI/BI justification.[cite:10][cite:13]

### 6. User change tolerance score

Estimate whether the target user base can absorb a platform and UX shift.[cite:11][cite:14][cite:32]

Higher score means greater readiness for replacement.[cite:14][cite:32]

---

## DAX Translation Rules

The app must treat DAX as a first-class migration challenge.[cite:23][cite:24]

Every DAX measure must be classified into one of the following buckets before translation:

- `direct_sql_candidate`
- `metric_view_candidate`
- `requires_semantic_remodel`
- `presentation_only`
- `not_viable_without_redesign` [cite:23][cite:24][cite:28]

### DAX handling rules

- Additive KPIs should preferentially translate into governed SQL or metric-view definitions when reusable.[cite:28][cite:31]
- Ratios and time-intelligence measures must be validated carefully against target grain and filter semantics.[cite:24][cite:33]
- Measures that depend on Power BI-specific evaluation context must be flagged for semantic remodel instead of naive SQL generation.[cite:23][cite:24]
- The app must never claim full parity unless metric outputs have been validated across representative slices and dates.[cite:23][cite:30]

---

## Transformation Rules

### Upstream-first rule

If Power Query or report logic performs reusable data cleansing, shaping, conformance, or enrichment, the target should usually be an upstream Databricks transformation rather than repeated dashboard logic.[cite:2][cite:20][cite:23]

### Reuse-first rule

If multiple reports depend on the same KPI or business definition, the target should be a governed semantic object or metric view, not duplicated SQL across dashboard definitions.[cite:25][cite:28][cite:31]

### Presentation-only rule

Sorting defaults, label formatting, lightweight filters, and narrative arrangement may remain in the target dashboard layer if they do not define business meaning.[cite:13][cite:19]

### Refuse-when-ambiguous rule

The app must refuse automatic dashboard generation when joins, metric definitions, grain, or security mappings remain unresolved.[cite:23][cite:25][cite:28]

Unmapped or invalid Unity Catalog targets for tables or views required by the migration path count as **unresolved semantic binding** and must surface as `binding_blockers` until cleared.

---

## Row-level security and execution identity (v1)

- The app must extract **RLS-related metadata** from the semantic model when present (roles, filter expressions, table permissions) and attach it to the canonical model and `**open_risks`**.
- **Version 1 scope:** The app must **not** claim row-level **security parity** between Power BI and Databricks. The `validation_plan` must list RLS as **unmapped** until the operator documents an intended target (for example Unity Catalog row filters, secured views, or workspace-level access) and accepts residual risk.
- **Execution identity:** AI/BI dashboard datasets run as **Databricks SQL warehouse / interactive identity** (for example the signed-in user under OAuth), not as Power BI role names. The product must document that migrated dashboards inherit **that** access model until a dedicated RLS remapping workflow exists.

---

## Validation Rules

### Validation order

The app must validate migrations in this order:

1. Source extraction completeness
2. Semantic translation completeness
3. Metric parity
4. Security parity (for v1 automated migration: **document gaps and target approach**; do not assert parity for RLS or role semantics—see Row-level security and execution identity)
5. Filter behavior parity
6. Dashboard behavior and UX acceptance [cite:23][cite:25][cite:28]

### Metric parity requirements

Every migrated KPI must be validated against the source using representative date ranges, filters, segments, and drill paths.[cite:23][cite:30]

The app must store:

- Source metric identifier
- Source logic summary
- Target metric object
- Validation samples run
- Observed variances
- Disposition of variance: `expected_difference`, `open_issue`, or `validated_match` [cite:23][cite:25]

### Explainability rule

Every transformed metric must have machine-readable lineage from source measure to target object, including any known semantic deviations.[cite:25][cite:28]

---

## UX and Product Rules

### Product principles

- Optimize for **rationalization**, not raw migration volume.[cite:14][cite:25]
- Prefer fewer governed semantic objects over many duplicated dashboard-local definitions.[cite:25][cite:28][cite:31]
- Prefer coexistence when replacement would reduce user value.[cite:20][cite:29]
- Require explicit sign-off when a target dashboard intentionally simplifies the source UX.[cite:14][cite:23]

### Required outputs per artifact

For every analyzed artifact, the app must produce:

1. A migration recommendation
2. A semantic translation plan
3. A dashboard rebuild plan
4. A validation plan [cite:23][cite:25]

### Required estate-level outputs

For the full Power BI estate, the app must produce:

- A migration wave plan
- A retained-artifact register
- A semantic consolidation roadmap
- A risk register
- A parity-validation backlog [cite:14][cite:23][cite:25]

When the app analyzes **more than one** source package in a single session (for example multiple `.pbit` uploads), it must also emit a **session estate rollup** (structured JSON and may include CSV) that condenses the same themes: suggested migration ordering or waves, retained items by disposition, cross-package semantic consolidation hints, a combined risk register, and pointers into the parity-validation backlog. For a **single** source package, the app may emit a **one-row or minimal summary** so export shapes stay consistent across sessions.

---

## Cursor Operating Rules

Use these rules if Cursor is generating code or decisions for the migration app itself.

### System behavior

- Treat Power BI to Databricks AI/BI migration as an **architecture translation workflow** that includes **layout- and visual-level fidelity** as an explicit product goal.[cite:27][cite:28]
- Always build and persist a **canonical intermediate model** before emitting target assets.[cite:23][cite:24]
- Never publish **data-bound** dashboard datasets against Unity Catalog before semantic, grain, and permission issues are resolved for those queries; do not bypass `binding_blockers` or SQL validation for executed datasets.[cite:25][cite:28]
- For **LLM-produced** Lakeview JSON, never publish without the same **SQL probe / DESCRIBE** gates; if the model emits dataset SQL, **override or strip** it with **validated** `starter_sql` for the primary dataset before create/publish. If LLM output is **not valid JSON** or does not match the required shape after a bounded repair attempt, **fail closed** and surface errors in the same spirit as `binding_blockers` (no partial publish of an unvalidated dashboard).
- After the canonical model exists, the app may emit **layout skeletons** or a **full page-and-visual tree** for Lakeview per product rules; **data-bound** definitions remain subject to the same binding and SQL validation gates before publish.[cite:23][cite:25]
- Prefer Unity Catalog semantic assets and curated SQL objects over dashboard-local business logic.[cite:25][cite:28][cite:31]
- Optimize for hybrid coexistence and phased replacement while still pursuing **high page and visual coverage** in target artifacts where in scope—without equating coverage with unsafe semantic shortcuts.[cite:20][cite:29]
- After Unity Catalog binding, **recompute and persist `effective_disposition`**; do not enable deploy based on `recommended_disposition` alone when binding or validation would forbid it.
- For **multi-source sessions**, emit the **session estate rollup** required under estate-level outputs.

### Output contract

For every source artifact, Cursor-generated app logic must return a structure equivalent to:

```json
{
  "artifact_id": "string",
  "artifact_type": "dashboard|report|semantic_model|measure|workspace",
  "recommended_disposition": "migrate_now|migrate_later|retain_in_power_bi|redesign_before_migration",
  "effective_disposition": "migrate_now|migrate_later|retain_in_power_bi|redesign_before_migration",
  "binding_blockers": [],
  "scores": {
    "semantic_complexity": 0,
    "lakehouse_affinity": 0,
    "front_end_dependency": 0,
    "real_time_value": 0,
    "ai_leverage": 0,
    "user_change_tolerance": 0
  },
  "semantic_translation_plan": [],
  "dashboard_rebuild_plan": [],
  "validation_plan": [],
  "open_risks": []
}
```

`binding_blockers` must be a list of human-readable strings (empty when no binding gaps). When binding is not yet performed, `effective_disposition` may equal `recommended_disposition` and `binding_blockers` may be empty or populated with `pending_binding` style messages—product logic must define consistent behavior.

The migration app may extend this contract, but it must preserve the same decision structure, explicit disposition model, and the **recommended vs effective** distinction whenever UC binding is in scope.[cite:14][cite:23]

---

## Final Rule

The migration app succeeds when it reduces duplicated semantics, preserves trusted business meaning, improves Lakehouse alignment, **covers extracted interactive report pages and visuals in target AI/BI artifacts (with parity manifests for every documented gap),** and applies coexistence or phased replacement where that optimizes user value—**not** as an excuse to skip in-scope fidelity-track work for analyzed reports.[cite:20][cite:25][cite:28][cite:29]

Here are some additional resources that you can use for reference:

[https://github.com/topics/powerbi-dashboards](https://github.com/topics/powerbi-dashboards)
[https://www.linkedin.com/posts/eirini-papakosta_now-easier-than-ever-migrating-a-power-bi-activity-7427728728195223552-LCf1/?utm_medium=ios_app&rcm=ACoAACq1P_EBaylgPAX5lj9M3Q8DhUgd7a51iio&utm_source=social_share_send&utm_campaign=whatsapp](https://www.linkedin.com/posts/eirini-papakosta_now-easier-than-ever-migrating-a-power-bi-activity-7427728728195223552-LCf1/?utm_medium=ios_app&rcm=ACoAACq1P_EBaylgPAX5lj9M3Q8DhUgd7a51iio&utm_source=social_share_send&utm_campaign=whatsapp)
[https://github.com/pedrozanlorensi/pbi-aibi-converter/tree/master](https://github.com/pedrozanlorensi/pbi-aibi-converter/tree/master)
[https://github.com/databricks-solutions/databricks-genie-workbench](https://github.com/databricks-solutions/databricks-genie-workbench)
[https://github.com/pclee-demo/genie-assessment-toolkit](https://github.com/pclee-demo/genie-assessment-toolkit)
[https://github.com/databricks-solutions/technical-services-solutions/blob/main/data-warehousing/dbrx-business-semantics/1_CreateMetricView.ipynb](https://github.com/databricks-solutions/technical-services-solutions/blob/main/data-warehousing/dbrx-business-semantics/1_CreateMetricView.ipynb)
[https://docs.databricks.com/aws/en/business-semantics/metric-views](https://docs.databricks.com/aws/en/business-semantics/metric-views)
[https://docs.databricks.com/aws/en/ai-bi/release-notes/2026](https://docs.databricks.com/aws/en/ai-bi/release-notes/2026)
[https://docs.databricks.com/aws/en/ai-bi](https://docs.databricks.com/aws/en/ai-bi)
[https://docs.databricks.com/aws/en/ai-bi/concepts](https://docs.databricks.com/aws/en/ai-bi/concepts)