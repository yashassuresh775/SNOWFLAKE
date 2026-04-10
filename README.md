# SNOWFLAKE — Economic Intelligence

**US Economic Intelligence** is a **Streamlit in Snowflake** app that combines **Cortex Analyst** (natural language → SQL with a staged semantic model), **Cortex COMPLETE** narratives, curated **macro** and **company-relationship** views, verified SQL fallbacks, and analyst-style UX (charts, transparency, PDF brief).

## Documentation map

| Document | What it is |
|----------|------------|
| **[SETUP.md](SETUP.md)** | **Quick setup** — SQL order, semantic upload, SiS deploy, packages |
| **[streamlit_economic_intelligence/README.md](streamlit_economic_intelligence/README.md)** | **Full guide** — architecture, innovation highlights, dashboard UI (full names), deployment (Section 8), validation, traps |

Start with **SETUP.md** to run the stack; use the Streamlit README for everything else.

## Repository layout (high level)

- `streamlit_economic_intelligence/` — `app.py`, `streamlit_app.py`, `requirements.txt`
- `hackathon/economic_indicators_views.sql` — `V_*` views including **Consumer Price Index** and **Gross Domestic Product**
- `hackathon/sql/02_economic_indicators_wide.sql` — **`ECONOMIC_INDICATORS_WIDE`** / **`macro_wide`**
- `hackathon/sql/03_semantic_stage.sql` — stage for YAML
- `hackathon/semantic_models/semantic_model.yaml` — Cortex Analyst semantic model
- `hackathon/QUERY_LOG.md` — natural-language test rubric

## Clone

```bash
git clone https://github.com/yashassuresh775/SNOWFLAKE.git
cd SNOWFLAKE
```

Then follow **[SETUP.md](SETUP.md)**.
