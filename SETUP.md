# Quick setup — Economic Intelligence (Snowflake)

This file is the **short checklist** to get the app running. **Full documentation** (architecture, dashboard UI reference, innovation narrative, troubleshooting, operational validation) lives in **[streamlit_economic_intelligence/README.md](streamlit_economic_intelligence/README.md)** — start there if you need depth.

**Repository map**

| File | Purpose |
|------|--------|
| [README.md](README.md) | **Main page** — project overview and links |
| [SETUP.md](SETUP.md) | This checklist (SQL + SiS + packages) |
| [streamlit_economic_intelligence/README.md](streamlit_economic_intelligence/README.md) | Complete technical guide (includes **Section 8 — Deployment**) |

---

## 1. Prerequisites

- Snowflake role that can create objects in **`HACKATHON`** (or adjust names in SQL) and **read** **`SNOWFLAKE_PUBLIC_DATA_FREE`** (see `hackathon/economic_indicators_views.sql` for Cybersyn swap notes).
- Access to **Cortex Analyst** and **Cortex COMPLETE** per your account policy.

## 2. Run SQL (worksheet order)

Execute in order:

1. `hackathon/economic_indicators_views.sql`
2. `hackathon/sql/02_economic_indicators_wide.sql`
3. `hackathon/sql/03_semantic_stage.sql`

## 3. Upload semantic model

From a machine where the repo is cloned, set Snowflake **`LOCAL`** to the directory that **contains** the `hackathon` folder, then:

```sql
PUT file://hackathon/semantic_models/semantic_model.yaml @HACKATHON.DATA.SEMANTIC_MODELS
  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

Or upload `semantic_model.yaml` via **Data → Stages** in Snowsight.

**Verify:**

```sql
LIST @HACKATHON.DATA.SEMANTIC_MODELS;
SELECT COUNT(*) FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE;
```

## 4. Streamlit in Snowflake (Snowsight)

1. Create a **Streamlit** app; set database/schema (e.g. `HACKATHON.DATA`) and warehouse.
2. Add **`streamlit_economic_intelligence/app.py`** as main **or** main **`streamlit_app.py`** with **`app.py`** alongside (see [streamlit_economic_intelligence/README.md](streamlit_economic_intelligence/README.md) Section 8.3).
3. In app **Settings → Packages**, add lines from `streamlit_economic_intelligence/requirements.txt`.

Optional environment variables (if your app settings support them):

- `SEMANTIC_MODEL_FILE` — default in code is `@HACKATHON.DATA.SEMANTIC_MODELS/semantic_model.yaml`
- `CORTEX_COMPLETE_MODEL` — if the default model is not enabled
- `EI_DOCS_BASE_URL` — optional; see app sidebar “Repository setup & docs” for GitHub links

## 5. After deploy

Run the golden checks in **Section 10** of [streamlit_economic_intelligence/README.md](streamlit_economic_intelligence/README.md) and spot-test prompts from `hackathon/QUERY_LOG.md`.

---

**Next:** [streamlit_economic_intelligence/README.md](streamlit_economic_intelligence/README.md) for architecture, UI glossary, innovation summary, and detailed troubleshooting.
