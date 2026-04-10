# Deploy Economic Intelligence — Streamlit in Snowflake (Option A)

Follow this in order. Official reference: [Create your Streamlit app](https://docs.snowflake.com/en/developer-guide/streamlit/getting-started/create-streamlit-ui.html).

## 1. One-time data setup (SQL worksheet)

Run in a worksheet as a role that can create objects in `HACKATHON` and read `SNOWFLAKE_PUBLIC_DATA_FREE`:

1. `hackathon/economic_indicators_views.sql`
2. `hackathon/sql/02_economic_indicators_wide.sql` — creates `ECONOMIC_INDICATORS_WIDE` (shared `GEO_ID` + `OBSERVATION_DATE` panel). Required for the semantic **`macro_wide`** logical table and verified “compare X and Y over time” queries; optional for any Streamlit header KPIs you add against this view.
3. `hackathon/sql/03_semantic_stage.sql`

Upload the semantic model to the stage (adjust path to your local file or use **Data → Stages → Upload**):

```sql
PUT file://semantic_model.yaml @HACKATHON.DATA.SEMANTIC_MODELS
  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

Use the file from `hackathon/semantic_models/semantic_model.yaml`.

Confirm:

```sql
LIST @HACKATHON.DATA.SEMANTIC_MODELS;
SELECT COUNT(*) FROM HACKATHON.DATA.V_UNEMPLOYMENT;
SELECT COUNT(*) FROM HACKATHON.DATA.V_CPI;
SELECT COUNT(*) FROM HACKATHON.DATA.V_GDP;
SELECT COUNT(*) FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE;
```

## 2. Create the Streamlit app in Snowsight

1. Sign in to [Snowsight](https://app.snowflake.com) for your account.
2. Open **Projects** → **Streamlit** (left navigation).
3. Click **+ Streamlit** (or **+ Streamlit App**).
4. Set:
   - **App name:** e.g. `ECONOMIC_INTELLIGENCE`
   - **Database / schema:** where the app object should live (`HACKATHON` / `DATA` is fine if your role has **CREATE STREAMLIT** there).
   - **Warehouse:** a running warehouse (e.g. `COMPUTE_WH` or your training warehouse).
   - **Runtime:** **Warehouse** (simplest) unless your org requires a container runtime.
5. Click **Create**.

## 3. Add your code

1. Open the app → **Files** (or editor).
2. **Main entry — pick one:**
   - **`app.py`:** paste the full contents of `streamlit_economic_intelligence/app.py` and set it as the main file, **or**
   - **`streamlit_app.py`:** upload **`app.py`** and **`streamlit_app.py`** from `streamlit_economic_intelligence/` in the **same folder**, set main to **`streamlit_app.py`** (it loads `app.py`). Do **not** keep the default SiS template that imports Plotly.
3. **Save** before **Run**.

## 4. Python packages (required — or imports fail)

If you see **`ModuleNotFoundError`** (often **`requests`**), add packages in app **Settings** / **Packages**, one per line:

```
streamlit>=1.31.0
pandas>=2.0.0
requests>=2.31.0
snowflake-snowpark-python>=1.11.0
reportlab>=4.0.0
```

(Same as `requirements.txt` — **no Plotly**; charts use native `st.line_chart` / `st.bar_chart`.)

**Save**, then **Run** again. Add **`requests`** if you see `ModuleNotFoundError: No module named 'requests'`.

## 5. Optional environment variables

If your app settings expose **environment variables** or **secrets**:

| Name | Example value |
|------|----------------|
| `SEMANTIC_MODEL_FILE` | `@HACKATHON.DATA.SEMANTIC_MODELS/semantic_model.yaml` |
| `CORTEX_COMPLETE_MODEL` | `llama3-8b` or `snowflake-arctic` if `mistral-large2` is not enabled |

If unset, the app uses the defaults in `app.py`.

## 6. Run

Click **Run** / **Open** on the app. The app runs **inside** Snowflake; `get_active_session()` supplies your Snowpark session.

## 7. If something fails

| Symptom | What to check |
|--------|----------------|
| “No active session” / `get_active_session` fails | Run only from **Streamlit in Snowflake**, not locally. |
| Cortex Analyst HTTP errors | Role needs **USAGE** on DB/schema, **READ** on stage, and network access to the REST API (org policy). |
| `COMPLETE` errors | Set `CORTEX_COMPLETE_MODEL` to a model enabled in your account. |
| Permission denied on `HACKATHON` | Grant **USAGE** on database/schema, **SELECT** on views, **READ** on stage. |

## 8. Git (optional)

If your org enabled **Git for Streamlit**, use **Connect Git Repository** on the app, point at this repo, and set the root file to `streamlit_economic_intelligence/app.py` (or `streamlit_economic_intelligence/streamlit_app.py` if that is your workspace default — keep both files in the repo path).
