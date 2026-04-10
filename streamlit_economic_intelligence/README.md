# Economic Intelligence - Streamlit + Cortex Analyst

This app provides a conversational BI interface on top of curated economic and company-relationship views in Snowflake.

It supports:
- natural-language questions through **Cortex Analyst** and a versioned **semantic model** on a Snowflake stage,
- **SQL transparency** on every answer (governance, debugging, trust),
- **Cortex COMPLETE** narratives with selectable **personas** (e.g. executive voice),
- **verified SQL fallbacks** and **layered Consumer Price Index recovery** so demos stay alive when generation fails,
- a curated **macro wide panel** for **same-timeline** multi-indicator questions,
- **company relationship** queries on the same stack as macro analytics,
- **anomaly highlights**, **correlation-style insights**, and **PDF brief export** for deliverables.

**Deployment:** see [Section 8 — Deployment — Streamlit in Snowflake](#8-deployment--streamlit-in-snowflake-snowsight) below (Snowsight / Streamlit in Snowflake).

---

## Why this submission stands out (for judges)

| Pillar | What we built | Why it matters |
|--------|----------------|----------------|
| **Snowflake-native intelligence** | Streamlit in Snowflake + Snowpark + Cortex Analyst REST + Cortex COMPLETE | End-to-end inside the **Snowflake Data Cloud**—security, governance, and warehouse economics stay unified. |
| **Semantic-first NL→SQL** | `semantic_model.yaml` on `@HACKATHON.DATA.SEMANTIC_MODELS` with verified queries and custom instructions | Analyst behavior is **repeatable and improvable** like software, not a one-off prompt. |
| **Real-world data complexity** | Curated views over **Bureau of Labor Statistics**, **Bureau of Economic Analysis**, **Federal Reserve**, Census retail, marketplace feeds, plus **parent–subsidiary** edges | Shows mastery of **messy public data** and **enterprise-style** corporate graph questions in **one** app. |
| **Resilience under pressure** | Intent router + **Consumer Price Index** source ladder + **Gross Domestic Product** via **`V_GDP`** in fallbacks | Hackathon demos fail on a single bad SQL generation—we **engineered around** that. |
| **Analyst-grade UX** | Workspace modes, tabbed starter prompts, charts, follow-ups, PDF export | Feels like a **product**, not a thin wrapper around `SELECT *`. |

**Suggested live demo questions (copy-paste):** (1) *Show monthly headline Consumer Price Index from 2019 through 2024.* (2) *Show US real Gross Domestic Product by quarter for the last five years.* (3) *Which company owns the most subsidiaries?* (4) *What is the US unemployment trend from 2020 to 2024?* — plus *Compare unemployment and Consumer Price Index on the same monthly timeline since 2020* for the **wide panel**.

---

## Innovation and differentiation (technical depth)

This section documents **everything distinctive** in the submission: data engineering, Cortex usage, application resilience, analytics UX, and trust.

### Data and semantic layer

- **Curated Snowflake views** on Snowflake **public data / marketplace** listings (`SNOWFLAKE_PUBLIC_DATA_FREE` and documented **Cybersyn** alternatives in SQL comments) so Cortex Analyst and fallback SQL target **stable names** (`V_UNEMPLOYMENT`, `V_RETAIL_SALES`, `V_INTEREST_RATES`, `V_INDUSTRIAL_PRODUCTION`, `V_CPI`, `V_GDP`, `V_COMPANY_RELATIONSHIPS`) instead of raw listing sprawl.
- **`V_CPI` tiered logic (strict tiers A/B plus tier C):** headline **Consumer Price Index** (**CPI-U**-style, **seasonally adjusted**, core exclusions) first; a **controlled permissive** monthly branch with value bands and **`QUALIFY`** deduplication fills gaps when metadata differs across feeds—**robust to real catalog variance**.
- **`ECONOMIC_INDICATORS_WIDE` and semantic `macro_wide`:** one row per **`GEO_ID`** + **`OBSERVATION_DATE`** aligning unemployment, retail, rates, industrial production, **Consumer Price Index**, and **Gross Domestic Product** so users can ask **multi-series comparisons on one calendar** without hand-joining five sources in the UI.
- **Pragmatic split for prices vs output:** **Consumer Price Index** in the wide panel flows from **`V_CPI`**; **Gross Domestic Product** in the wide panel is **aggregated inline** from the same **Bureau of Economic Analysis** filters as **`V_GDP`**, while the app’s **Gross Domestic Product** fallback SQL reads **`V_GDP`** directly—balances **panel completeness** with **simple series access**.
- **Company graph on the same platform:** **`V_COMPANY_RELATIONSHIPS`** (parent edges) so **macro** and **subsidiary** questions share one semantic model and one Streamlit experience.

### Cortex Analyst and Cortex COMPLETE

- **Cortex Analyst** over **REST** (`/api/v2/cortex/analyst/message`) with **`SEMANTIC_MODEL_FILE`** pointing at staged YAML—**versioned contract** for tables, synonyms, measures, dimensions, and **verified_queries**.
- **Cortex COMPLETE** (`generate_narrative`) with **`PERSONAS`**—users pick a **response voice** (e.g. executive); hints in **Voice details** explain each mode.
- **Optional environment overrides:** `CORTEX_COMPLETE_MODEL`, `SEMANTIC_MODEL_FILE` for accounts where defaults differ.

### Resilience and intelligent routing

- **`_fallback_sql_for_question`:** normalized-text **intent router** mapping high-value questions to **hand-verified SQL** (retail leaders, **Treasury** windows, industrial production, **Consumer Price Index** / **Gross Domestic Product** windows, **macro wide** compares, subsidiary patterns, etc.) when Analyst returns no SQL or execution fails.
- **`_recover_cpi_from_marketplace`:** layered **Consumer Price Index** recovery—**`ECONOMIC_INDICATORS_WIDE`**, public **CPIAUCSL**-style series, **PUBLIC_DATA_FREE** and **CYBERSYN** “best monthly series” pickers with optional value band—**appends a transparency note** when a fallback source is used.
- **`SPELLING_MAP` and normalization** (e.g. `_normalize_question`) improve matching on typos; **`_analyst_text_is_question_echo`** helps avoid treating useless Analyst echoes as real answers where used in the flow.

### Analytics and UX beyond a static table

- **Native Streamlit charts** (`st.line_chart`, `st.bar_chart`)—**no Plotly** in the baseline—so **Streamlit in Snowflake** package profiles stay simple and portable.
- **`_zscore_anomaly_rows`:** flags extreme points on time series (configurable **z-score** threshold) for **visual anomaly emphasis** in charts.
- **`_correlation_insight_line`:** when results are **multi-series** on a date column, adds a **short quantitative read** alongside the chart.
- **Economic context** (where wired in `app.py`): situates series relative to **events** or periods when applicable—supports **storytelling**, not only plotting.
- **PDF export (`reportlab`):** **Export brief (PDF)** packages question, narrative, and **SQL transparency** into a **shareable artifact** for stakeholders.

### Trust, governance, and iteration

- **SQL transparency** panel shows **what actually ran**—essential for **audit**, **debugging**, and **teaching**.
- **Documented improvement loop** (see Section 5): prefer **semantic model** changes; add **fallbacks** only for repeated high-value failures—treats the system as **evolving software**.
- **Operational validation** (Section 10): view checks, stage listing, **golden natural-language smoke tests**, and **`hackathon/QUERY_LOG.md`** for rubric-style regression after YAML changes.
- **`hackathon/notebooks/Economic_Modeling_Decisions.ipynb`:** modeling narrative for **why** logical tables and joins are shaped as they are.

### Dashboard and product polish

- **Three-column layout:** left rail (examples + tabbed starter prompts), center (ask, thread, results), right (context, SQL, follow-ups).
- **Workspace switcher:** **Economic analytics** vs **Company relationships** reorders examples and tabs without duplicating codepaths.
- **Tabbed starter prompts** (**Macro**, **Consumer Price Index / Gross Domestic Product**, **Wide**, **Companies**) with **full-width suggestion chips** for **mobile-friendly** demos.
- **Suggested follow-ups:** heuristic **`heuristic_followups`** merged with **COMPLETE**-generated suggestions where enabled—keeps the conversation **going** after the first answer.

---

## 1) High-level architecture

User question → Streamlit UI (`app.py`) → Cortex Analyst REST (`/api/v2/cortex/analyst/message`) → SQL → Snowflake tables/views → DataFrame/chart/table → Cortex COMPLETE narrative.

Core data objects:
- `HACKATHON.DATA.V_UNEMPLOYMENT`
- `HACKATHON.DATA.V_RETAIL_SALES`
- `HACKATHON.DATA.V_INTEREST_RATES`
- `HACKATHON.DATA.V_INDUSTRIAL_PRODUCTION`
- `HACKATHON.DATA.V_CPI` — headline **Consumer Price Index** for **All Urban Consumers**, **All Items**, **seasonally adjusted** (**CPI-U**, **SA**), **Bureau of Labor Statistics** (**BLS**)
- `HACKATHON.DATA.V_GDP` — **Bureau of Economic Analysis** (**BEA**) **Gross Domestic Product** (**GDP**) level series (quarterly and annual rows in the view; classic marketplace match)
- `HACKATHON.DATA.V_COMPANY_RELATIONSHIPS`
- `HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE` — curated macro panel (one row per `GEO_ID` + `OBSERVATION_DATE`) for multi-indicator comparisons; semantic logical table **`macro_wide`**. **Consumer Price Index** comes from **`V_CPI`**; **Gross Domestic Product** is aggregated inline from the same **BEA** marketplace filters as in `hackathon/sql/02_economic_indicators_wide.sql` (not only from `V_GDP` row counts).
- Semantic model on stage: `@HACKATHON.DATA.SEMANTIC_MODELS/semantic_model.yaml`

## 2) Component responsibilities

### `streamlit_economic_intelligence/app.py`

Main application logic:
- builds UI (chat, suggested prompts, chart area, SQL panel),
- calls Cortex Analyst over REST,
- executes generated SQL with Snowpark,
- generates executive-style narrative with `SNOWFLAKE.CORTEX.COMPLETE`,
- applies intent-based SQL fallbacks when Analyst output is missing or broken.

Important internal units:
- `_connection_auth()`: obtains host/token from active Snowpark connection.
- `call_cortex_analyst()`: REST call for NL-to-SQL.
- `run_sql()`: SQL executor to pandas DataFrame.
- `generate_narrative()`: narrative generation prompt + COMPLETE call.
- `_fallback_sql_for_question()`: normalized intent router for safety-net queries.

### `hackathon/semantic_models/semantic_model.yaml`

Cortex semantic contract:
- table-level metadata for **eight logical tables** (unemployment, retail_sales, interest_rates, industrial_production, cpi, gdp, macro_wide, company_relationships),
- dimensions/time/measures and synonyms,
- custom routing instructions,
- verified query examples for key ask patterns.

This file strongly influences how Analyst maps text to SQL. The **`macro_wide`** logical table maps to `ECONOMIC_INDICATORS_WIDE` for join-style questions across measures on the same timeline.

### `hackathon/economic_indicators_views.sql`

Creates granular source views used by Analyst and fallback SQL, including **`V_CPI`** (**BLS** headline **Consumer Price Index**) and **`V_GDP`** (**BEA** **Gross Domestic Product**).

### `hackathon/sql/02_economic_indicators_wide.sql`

Builds `ECONOMIC_INDICATORS_WIDE` by aggregating each `V_*` domain to a consistent grain. **Consumer Price Index** is taken from **`V_CPI`**; **Gross Domestic Product** is computed inline from **BEA** marketplace **financial_economic_indicators** tables with the same filters as **`V_GDP`**.

### `hackathon/sql/03_semantic_stage.sql`

Creates stage for semantic model YAML.

### `streamlit_economic_intelligence/streamlit_app.py`

Thin loader used when Snowsight defaults main entry to `streamlit_app.py`; it executes `app.py`.

## 3) Dashboard UI reference (full names)

The main dashboard uses a three-column layout (left rail, center ask/thread/results, right context). Below are the **left-rail** pieces and what the abbreviations mean in full.

### Glossary (abbreviations used in the UI)

| Shorthand | Full form |
|-----------|-----------|
| **CPI** | **Consumer Price Index** |
| **GDP** | **Gross Domestic Product** |
| **BEA** | **Bureau of Economic Analysis** |
| **BLS** | **Bureau of Labor Statistics** |
| **SA** | **Seasonally adjusted** |
| **Cos.** (tab label) | **Companies** (corporate parent–subsidiary prompts) |
| **Wide** (tab label) | **Macro wide panel** — questions that join multiple indicators on one timeline via **`ECONOMIC_INDICATORS_WIDE`** |

### Workspace

**Control:** `Workspace` drop-down (`st.selectbox`).

**Options:**
- **Economic analytics** — default; left rail emphasizes chart-ready macro examples and orders starter tabs **Macro → Consumer Price Index / Gross Domestic Product → Wide → Companies**.
- **Company relationships** — emphasizes company graph examples; tab order becomes **Companies → Macro → Consumer Price Index / Gross Domestic Product → Wide**.

Same semantic model and `app.py` logic apply in both modes; only example ordering and helper copy change.

### Chart-ready examples (above the expander)

Section label comes from `_workspace_quick_examples()` (e.g. icon + short heading). **Buttons** here are **`EXAMPLE_QUICK`** or **`EXAMPLE_QUICK_COMPANY`** tuples: each row is a full-sentence question. Clicking sets a pending question and reruns the app so the center **Ask & analyze** flow executes it (same as typing in the text box).

### “More starter prompts (tabs)”

**Control:** Streamlit **`st.expander`**, title **“More starter prompts (tabs)”**, opened by default (`expanded=True`).

Purpose: extra categorized prompts without crowding the top of the left column.

### Tab bar inside the expander

Built with **`st.tabs`**. Labels are short for space; meanings:

| Tab label | Full meaning | Prompt list (constant in `app.py`) |
|-----------|----------------|-------------------------------------|
| **Macro** | Core macro series (unemployment, interest rates, retail, industrial production, demographics splits) | `SUGGESTED_CORE` |
| **CPI/GDP** | Headline **Consumer Price Index** and **Gross Domestic Product** questions | `SUGGESTED_PRICES_GDP` |
| **Wide** | Multi-metric compares on **`ECONOMIC_INDICATORS_WIDE`** (e.g. unemployment vs **Consumer Price Index** on the same dates) | `SUGGESTED_MACRO_WIDE` |
| **Cos.** | **Companies** — subsidiaries and parent counts | `SUGGESTED_COMPANIES` |

Only one tab’s list is visible at a time; the underline indicates the active tab.

### Suggestion chips (full-width buttons)

**Implementation:** `_render_suggestion_chips(questions, key_prefix)` — one **`st.button`** per question, **`use_container_width=True`**.

**Behavior:** on click, sets **`st.session_state.pending_question`** to that string and calls **`st.rerun()`** so the main analysis pipeline runs as if the user had submitted that text.

### Response voice

**Control:** `Response voice` **`st.selectbox`** — chooses the persona string passed into Cortex COMPLETE for narrative tone.

**“Voice details”** — nested **`st.expander`** with `_render_persona_hints()` for the selected persona.

### Center column (brief)

- **Ask & analyze** — form with **Your question** text input and **Run analysis** submit button.
- **Conversation thread** — scrollable container for question/answer bubbles.
- **Results** — charts (native Streamlit charts), tables, correlation notes, SQL transparency when applicable.

### Right column

Contextual panels (semantic hints, SQL transparency, follow-ups) as implemented in `_render_dashboard_layout()` — see `app.py` for current widgets.

## 4) End-to-end process flow

1. User enters a question in the chat UI.
2. App sends question + semantic model path to Cortex Analyst REST.
3. If Analyst returns SQL:
   - app runs SQL,
   - displays chart/table,
   - shows generated SQL in transparency panel,
   - generates narrative from top result rows.
4. If Analyst returns no SQL or bad SQL:
   - app invokes intent router (`_fallback_sql_for_question`) and runs verified fallback SQL when matched.
5. App renders final answer as:
   - narrative,
   - chart/data table,
   - SQL transparency block.

## 5) How iterative improvements are done

We use a two-layer improvement loop:

### Layer A: semantic model first (preferred)
- Add/adjust synonyms in `semantic_model.yaml`.
- Clarify `custom_instructions` for ambiguous language.
- Add representative `verified_queries` for repeated failures.
- Re-upload YAML to stage and retest.

### Layer B: safety-net fallback routing (minimal but reliable)
- Normalize user question text.
- Map high-value intents to stable verified SQL templates.
- Execute fallback only when Analyst output fails (missing SQL / SQL error).

This keeps the app usable in demos while continuously improving native Analyst quality.

## 6) Common traps we found and fixes in place

### Trap 1: Cortex generates invalid CTE aliases for company queries
Symptom:
- SQL references `company_name` / `related_company_name` after aliasing them differently.
Fix:
- strengthened semantic instructions for company SQL shape,
- fallback routes for subsidiary leaderboard and "subsidiaries does X own" patterns.

### Trap 2: "Requires more information than available" on complex comparative asks
Symptom:
- Analyst refuses multi-step prompts (before/after comparisons, growth windows).
Fix:
- added explicit semantic instructions and verified queries,
- intent router fallback for specific comparative patterns.

### Trap 3: Runtime mismatch between repo and Snowsight files
Symptom:
- errors from stale `streamlit_app.py` template (e.g., old imports).
Fix:
- keep `streamlit_app.py` as a loader to execute current `app.py`,
- redeploy both files together when needed.

### Trap 4: Plotly package availability differences in Streamlit in Snowflake
Symptom:
- `ModuleNotFoundError: plotly` in some runtime setups.
Fix:
- app uses native Streamlit charts in the clean baseline.

### Trap 5: Deprecated Snowflake connector warning
Symptom:
- warning around old env var names.
Fix:
- operational note only; does not block app behavior.

## 7) Current fallback intent coverage

The router currently covers high-frequency failure classes such as:
- most subsidiaries,
- subsidiaries for a specific company,
- companies with more than N subsidiaries (currently `>5` pattern),
- top/biggest retail categories in 2023,
- auto retail trend 2019-2023,
- treasury/interest windows (2020+, 2022-2023),
- industrial top sectors (2023),
- aerospace production trend,
- retail growth before vs after 2022 hike cycle.

## 8) Deployment — Streamlit in Snowflake (Snowsight)

The app is designed to run **inside Snowflake** (Streamlit in Snowflake, often shortened **SiS**). It uses `get_active_session()` and is **not** a local-only Streamlit app without adaptation.

Official overview: [Create your Streamlit app](https://docs.snowflake.com/en/developer-guide/streamlit/getting-started/create-streamlit-ui.html).

### 8.1 One-time data setup (SQL worksheet)

Run as a role that can create objects in `HACKATHON` and read **`SNOWFLAKE_PUBLIC_DATA_FREE`** (or your org’s equivalent Cybersyn listing — see comments in `economic_indicators_views.sql`).

1. `hackathon/economic_indicators_views.sql`
2. `hackathon/sql/02_economic_indicators_wide.sql` — creates **`ECONOMIC_INDICATORS_WIDE`** for the semantic **`macro_wide`** logical table and multi-indicator questions.
3. `hackathon/sql/03_semantic_stage.sql`

Upload the semantic model YAML from the repo (path is under **`hackathon/semantic_models/`**, not the repo root). In a worksheet, set **`LOCAL`** to the directory that **contains** the `hackathon` folder (usually your clone root), then run:

```sql
PUT file://hackathon/semantic_models/semantic_model.yaml @HACKATHON.DATA.SEMANTIC_MODELS
  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

Alternatively, use **Data → Stages** in Snowsight and upload `semantic_model.yaml` to `@HACKATHON.DATA.SEMANTIC_MODELS`.

Sanity checks:

```sql
LIST @HACKATHON.DATA.SEMANTIC_MODELS;
SELECT COUNT(*) FROM HACKATHON.DATA.V_UNEMPLOYMENT;
SELECT COUNT(*) FROM HACKATHON.DATA.V_CPI;
SELECT COUNT(*) FROM HACKATHON.DATA.V_GDP;
SELECT COUNT(*) FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE;
```

### 8.2 Create the Streamlit app

1. Open **[Snowsight](https://app.snowflake.com)** for your account.
2. Create a **Streamlit** app (menu names vary by Snowflake release; look for **Streamlit** under **Projects** or **Apps**).
3. Set **database** and **schema** where the app object should live (for example `HACKATHON.DATA` if your role may **CREATE STREAMLIT** there).
4. Attach a **warehouse** that stays available while the app runs.
5. Choose the default **Streamlit in Snowflake** runtime (**Warehouse**-backed) unless your organization requires a different option.

### 8.3 Application files

In the app editor (**Files**):

- **Option A — single entry file:** upload **`streamlit_economic_intelligence/app.py`** and set it as the **main** Python file.
- **Option B — loader pattern:** upload both **`app.py`** and **`streamlit_app.py`** from `streamlit_economic_intelligence/` into the **same** app folder, set main to **`streamlit_app.py`** (it imports and runs `app.py`). Do **not** leave the default Snowsight template that assumes Plotly unless you intend to change dependencies.

Save, then **Run**.

### 8.4 Python packages

In app **Settings → Packages** (or equivalent), add the same packages as `requirements.txt`, one per line:

```
streamlit>=1.31.0
pandas>=2.0.0
requests>=2.31.0
snowflake-snowpark-python>=1.11.0
reportlab>=4.0.0
```

Charts use native **`st.line_chart`** / **`st.bar_chart`** (no Plotly in the baseline).

### 8.5 Optional environment variables

If your app settings expose **environment variables** or **secrets**:

| Name | Purpose |
|------|---------|
| `SEMANTIC_MODEL_FILE` | Override semantic path, e.g. `@HACKATHON.DATA.SEMANTIC_MODELS/semantic_model.yaml` |
| `CORTEX_COMPLETE_MODEL` | Model name for `COMPLETE` if the default in `app.py` is not enabled in your account |

### 8.6 Troubleshooting

| Symptom | What to check |
|--------|----------------|
| No active session / `get_active_session` fails | Run the app from **Streamlit in Snowflake**, not a local `streamlit run`. |
| Cortex Analyst HTTP errors | Role needs **USAGE** on database/schema, **READ** on the semantic stage, and org policy allowing the Analyst API. |
| `COMPLETE` errors | Set `CORTEX_COMPLETE_MODEL` to a model enabled for your account. |
| Permission denied on `HACKATHON` | Grant **USAGE** on database/schema, **SELECT** on views, **READ** on stage. |
| `ModuleNotFoundError: requests` (or others) | Add missing packages in app settings to match `requirements.txt`. |

### 8.7 Git integration (optional)

If your account has **Git for Streamlit**, connect this repository, set the app root to the folder that contains `app.py`, and set the main file to `streamlit_economic_intelligence/app.py` or `streamlit_economic_intelligence/streamlit_app.py` consistently with Section 8.3.

## 9) Notes for ongoing refinement

- Prefer semantic-model improvements over adding new hardcoded routes.
- Add fallback routes only for mission-critical prompts with repeated failures.
- Track failures and convert repeated ones into semantic verified queries first.

## 10) Operational validation and NL test log

Use this checklist after creating views, uploading the semantic YAML, and deploying the Streamlit app.

### Data and stage

1. **Views exist:** `SHOW VIEWS IN SCHEMA HACKATHON.DATA;` — expect `V_UNEMPLOYMENT`, `V_RETAIL_SALES`, `V_INTEREST_RATES`, `V_INDUSTRIAL_PRODUCTION`, `V_CPI`, `V_GDP`, `V_COMPANY_RELATIONSHIPS`, and `ECONOMIC_INDICATORS_WIDE`.
2. **Row counts (non-zero):** run `SELECT COUNT(*) FROM HACKATHON.DATA.<view>;` for each view above.
3. **Semantic file on stage:** `LIST @HACKATHON.DATA.SEMANTIC_MODELS;` includes `semantic_model.yaml` (and matches the file you intend to test).

### Golden NL smoke tests

In Analyst or the app, run a short set before demos:

- Latest or trend **unemployment** (2019+).
- **Retail** USD trend since 2020 or top categories in 2023.
- **Interest** rates in 2022–2023 (Treasury bill monthly pattern).
- **Industrial production** sector or time trend.
- **Consumer Price Index / Gross Domestic Product:** e.g. “peak YoY headline **Consumer Price Index** inflation in 2022” or “**Gross Domestic Product** by quarter last five years” (`cpi` / `gdp` logical tables).
- **Macro wide:** e.g. “Compare unemployment and industrial production since 2020” or “unemployment vs **Consumer Price Index** on the same timeline” (`macro_wide` / `ECONOMIC_INDICATORS_WIDE`).
- **Company graph:** top parents by subsidiary count or one named parent’s subsidiaries.

### Full rubric log

Documented questions with expected **pass** / **partial** / **fail** and schema alignment: `hackathon/QUERY_LOG.md`. Re-run those prompts in your account after each YAML change and adjust the log if needed.

### Modeling narrative

See `hackathon/notebooks/Economic_Modeling_Decisions.ipynb` for why the model uses five logical tables, how joins are scoped, and how iteration (YAML vs Streamlit fallbacks) works.
