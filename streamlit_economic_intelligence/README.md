# Economic Intelligence — Streamlit + Cortex Analyst

Hackathon **AI-02** app: Cortex Analyst semantic model on **`V_UNEMPLOYMENT`**, **`V_RETAIL_SALES`**, **`V_INTEREST_RATES`**, **`V_INDUSTRIAL_PRODUCTION`**, and **`V_COMPANY_RELATIONSHIPS`**. Optional **`ECONOMIC_INDICATORS_WIDE`** (run `02_economic_indicators_wide.sql`) is only for the Streamlit header KPI queries in `app.py`, not for the YAML.

**Step-by-step Streamlit in Snowflake (Option A):** see **`DEPLOY_SIS.md`** in this folder.

## Prereqs in Snowflake

1. **Marketplace:** install **Snowflake Data: Finance & Economics** so `SNOWFLAKE_PUBLIC_DATA_FREE` is available (macro tables + **`PUBLIC_DATA_FREE.COMPANY_RELATIONSHIPS`**).
2. Run **`hackathon/economic_indicators_views.sql`** (creates `HACKATHON.DATA` views, including `V_COMPANY_RELATIONSHIPS`).
3. Run **`hackathon/sql/03_semantic_stage.sql`** (semantic stage).
4. *(Optional)* Run **`hackathon/sql/02_economic_indicators_wide.sql`** if you want Streamlit **header** KPIs that read `ECONOMIC_INDICATORS_WIDE`.
5. Upload the semantic model (**re-PUT after any YAML change**):

   ```sql
   PUT file://semantic_model.yaml @HACKATHON.DATA.SEMANTIC_MODELS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   ```

   (Run from Snowflake CLI or SnowSQL with the file path adjusted; or use **Data » Stages** upload.)

6. **Streamlit in Snowflake:** create a new Streamlit app, point root file to `app.py`, include `requirements.txt` packages. Set secrets/env if needed:

   - `SEMANTIC_MODEL_FILE` — default `@HACKATHON.DATA.SEMANTIC_MODELS/semantic_model.yaml`
   - `CORTEX_COMPLETE_MODEL` — default `mistral-large2` (use an LLM enabled on your account, e.g. `llama3-8b`, `snowflake-arctic`)

7. **External access:** if the REST call to `/api/v2/cortex/analyst/message` is blocked, create a **network rule / Egress** as required by your org (training accounts often allow Snowflake REST to same account).

## Local UI skeleton (optional)

```bash
export STREAMLIT_MOCK_CORTEX=true
pip install -r requirements.txt
streamlit run app.py
```

Mock mode skips Analyst; full execution requires a Snowpark session (SiS).

## Query log

Fill in `hackathon/QUERY_LOG.md` with pass/fail for the 15+ test questions.
