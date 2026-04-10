# Economic Intelligence — Streamlit + Cortex Analyst

Hackathon **AI-02** app: natural language BI over `HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE` with Cortex Analyst, Cortex COMPLETE, Plotly, and the six innovation features from the spec (confidence meter, follow-up chips, ambiguity handler, executive brief, auto charts, live query accuracy).

## Prereqs in Snowflake

1. Run `hackathon/economic_indicators_views.sql`, then **`hackathon/sql/02_economic_indicators_wide.sql`** (adds `OBSERVATION_YEAR` / `OBSERVATION_MONTH` for the semantic model), then `hackathon/sql/03_semantic_stage.sql`.
2. Upload the semantic model (**re-PUT after any YAML change**):

   ```sql
   PUT file://economic_model.yaml @HACKATHON.DATA.SEMANTIC_MODELS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   ```

   (Run from Snowflake CLI or SnowSQL with the file path adjusted; or use **Data » Stages** upload.)

3. **Streamlit in Snowflake:** create a new Streamlit app, point root file to `app.py`, include `requirements.txt` packages. Set secrets/env if needed:

   - `SEMANTIC_MODEL_FILE` — default `@HACKATHON.DATA.SEMANTIC_MODELS/economic_model.yaml`
   - `CORTEX_COMPLETE_MODEL` — default `mistral-large2` (use an LLM enabled on your account, e.g. `llama3-8b`, `snowflake-arctic`)

4. **External access:** if the REST call to `/api/v2/cortex/analyst/message` is blocked, create a **network rule / Egress** as required by your org (training accounts often allow Snowflake REST to same account).

## Local UI skeleton (optional)

```bash
export STREAMLIT_MOCK_CORTEX=true
pip install -r requirements.txt
streamlit run app.py
```

Mock mode skips Analyst; full execution requires a Snowpark session (SiS).

## Query log

Fill in `hackathon/QUERY_LOG.md` with pass/fail for the 15+ test questions.
