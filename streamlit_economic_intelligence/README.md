# Economic Intelligence - Streamlit + Cortex Analyst

This app provides a conversational BI interface on top of curated economic and company-relationship views in Snowflake.

It supports:
- natural-language questions through Cortex Analyst,
- SQL transparency for every response,
- narrative summaries via Cortex COMPLETE,
- resilient fallback routing for known high-value question patterns.

For deployment steps in Snowsight, see `DEPLOY_SIS.md`.

## 1) High-level architecture

User question -> Streamlit UI (`app.py`) -> Cortex Analyst REST (`/api/v2/cortex/analyst/message`) -> SQL -> Snowflake tables/views -> DataFrame/chart/table -> Cortex COMPLETE narrative.

Core data objects:
- `HACKATHON.DATA.V_UNEMPLOYMENT`
- `HACKATHON.DATA.V_RETAIL_SALES`
- `HACKATHON.DATA.V_INTEREST_RATES`
- `HACKATHON.DATA.V_INDUSTRIAL_PRODUCTION`
- `HACKATHON.DATA.V_CPI` — headline CPI-U all items SA (BLS)
- `HACKATHON.DATA.V_GDP` — quarterly real GDP SAAR (BEA)
- `HACKATHON.DATA.V_COMPANY_RELATIONSHIPS`
- `HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE` — curated macro panel (one row per `GEO_ID` + `OBSERVATION_DATE`) for multi-indicator comparisons; semantic logical table **`macro_wide`**
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
- table-level metadata for the 5 logical domains,
- dimensions/time/measures and synonyms,
- custom routing instructions,
- verified query examples for key ask patterns.

This file strongly influences how Analyst maps text to SQL. The **`macro_wide`** logical table maps to `ECONOMIC_INDICATORS_WIDE` for join-style questions across measures on the same timeline.

### `hackathon/economic_indicators_views.sql`

Creates granular source views used by Analyst and fallback SQL, including **`V_CPI`** (BLS headline CPI-U) and **`V_GDP`** (BEA quarterly real GDP).

### `hackathon/sql/02_economic_indicators_wide.sql`

Builds `ECONOMIC_INDICATORS_WIDE` by aggregating each `V_*` domain to a consistent grain, plus CPI/GDP from Public Data, for verified compare-over-time queries.

### `hackathon/sql/03_semantic_stage.sql`

Creates stage for semantic model YAML.

### `streamlit_economic_intelligence/streamlit_app.py`

Thin loader used when Snowsight defaults main entry to `streamlit_app.py`; it executes `app.py`.

## 3) End-to-end process flow

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

## 4) How iterative improvements are done

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

## 5) Common traps we found and fixes in place

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

### Trap 4: Plotly package availability differences in SiS
Symptom:
- `ModuleNotFoundError: plotly` in some runtime setups.
Fix:
- app currently uses native Streamlit charts in the clean baseline.

### Trap 5: Deprecated Snowflake connector warning
Symptom:
- warning around old env var names.
Fix:
- operational note only; does not block app behavior.

## 6) Current fallback intent coverage

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

## 7) Deployment checklist

1. Run setup SQL:
   - `hackathon/economic_indicators_views.sql`
   - `hackathon/sql/02_economic_indicators_wide.sql`
   - `hackathon/sql/03_semantic_stage.sql`
2. Upload semantic model:

```sql
PUT file://semantic_model.yaml @HACKATHON.DATA.SEMANTIC_MODELS
  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

3. In Streamlit app, deploy `app.py` (or `streamlit_app.py` + `app.py`).
4. Ensure `requirements.txt` packages are installed in app settings.
5. Retest core prompts after every semantic-model change.

## 8) Notes for ongoing refinement

- Prefer semantic-model improvements over adding new hardcoded routes.
- Add fallback routes only for mission-critical prompts with repeated failures.
- Track failures and convert repeated ones into semantic verified queries first.

## 9) Operational validation and NL test log

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
- **CPI / GDP:** e.g. “peak YoY CPI inflation in 2022” or “real GDP by quarter last five years” (`cpi` / `gdp` logical tables).
- **Macro wide:** e.g. “Compare unemployment and industrial production since 2020” or “unemployment vs CPI on the same timeline” (`macro_wide` / `ECONOMIC_INDICATORS_WIDE`).
- **Company graph:** top parents by subsidiary count or one named parent’s subsidiaries.

### Full rubric log

Documented questions with expected **pass** / **partial** / **fail** and schema alignment: `hackathon/QUERY_LOG.md`. Re-run those prompts in your account after each YAML change and adjust the log if needed.

### Modeling narrative

See `hackathon/notebooks/Economic_Modeling_Decisions.ipynb` for why the model uses five logical tables, how joins are scoped, and how iteration (YAML vs Streamlit fallbacks) works.

