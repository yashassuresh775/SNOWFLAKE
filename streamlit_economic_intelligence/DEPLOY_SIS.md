# Deploy Economic Intelligence — Streamlit in Snowflake (Option A)

Follow this in order. Official reference: [Create your Streamlit app](https://docs.snowflake.com/en/developer-guide/streamlit/getting-started/create-streamlit-ui.html).

## 1. One-time data setup (SQL worksheet)

Run in a worksheet as a role that can create objects in `HACKATHON` and read `SNOWFLAKE_PUBLIC_DATA_FREE`:

1. `hackathon/economic_indicators_views.sql`
2. `hackathon/sql/03_semantic_stage.sql`
3. *(Optional)* `hackathon/sql/02_economic_indicators_wide.sql` — only for Streamlit **header** KPIs in `app.py` (`ECONOMIC_INDICATORS_WIDE`). The Cortex **YAML** uses granular `V_*` views only.

Upload the semantic model to the stage (adjust path to your local file or use **Data → Stages → Upload**):

```sql
PUT file://economic_model.yaml @HACKATHON.DATA.SEMANTIC_MODELS
  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

Use the file from `hackathon/semantic_models/economic_model.yaml`.

Confirm:

```sql
LIST @HACKATHON.DATA.SEMANTIC_MODELS;
SELECT COUNT(*) FROM HACKATHON.DATA.V_UNEMPLOYMENT;
-- If you created the wide view for Streamlit header cards:
-- SELECT COUNT(*) FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE;
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
2. Set the **main entry** file to **`app.py`** (if asked).
3. **Replace** the starter template with the full contents of this repo’s  
   `streamlit_economic_intelligence/app.py`  
   (copy from Cursor, paste into Snowsight).

## 4. Python packages (required — or imports fail)

Streamlit in Snowflake **does not** ship with Plotly. If you see a crash on **`import plotly.express as px`** (or `ModuleNotFoundError: No module named 'plotly'`), the app’s **Packages** are missing or not saved.

In the app **Settings** / **Packages** (wording varies), add **every** dependency, one per line, for example:

```
streamlit>=1.31.0
pandas>=2.0.0
plotly>=5.18.0
requests>=2.31.0
snowflake-snowpark-python>=1.11.0
```

(Same as `requirements.txt` in this folder.)

**Save**, then **Run** again. `streamlit` / `pandas` / `snowpark` may already be provided by the runtime — **`plotly` is usually the one you must add explicitly.**

## 5. Optional environment variables

If your app settings expose **environment variables** or **secrets**:

| Name | Example value |
|------|----------------|
| `SEMANTIC_MODEL_FILE` | `@HACKATHON.DATA.SEMANTIC_MODELS/economic_model.yaml` |
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

If your org enabled **Git for Streamlit**, use **Connect Git Repository** on the app, point at this repo, and set the root file to `streamlit_economic_intelligence/app.py`.
