# NL query log — Economic Intelligence (AI-02)

Run each prompt in **Cortex Analyst** (semantic YAML on `@HACKATHON.DATA.SEMANTIC_MODELS/semantic_model.yaml`) or the **Streamlit** app in Snowsight. The **Result** and **Notes** columns below are **design-time expectations** from the curated views, `semantic_model.yaml`, and `streamlit_economic_intelligence/app.py` fallback routing. After you deploy, spot-check rows that matter for your demo and update Result if Analyst behavior differs.

**Curated scope:** granular views `V_UNEMPLOYMENT`, `V_RETAIL_SALES`, `V_INTEREST_RATES`, `V_INDUSTRIAL_PRODUCTION`, **`V_CPI`** (headline CPI-U), **`V_GDP`** (quarterly real GDP), `V_COMPANY_RELATIONSHIPS`, plus **`ECONOMIC_INDICATORS_WIDE`** / semantic **`macro_wide`**. Semantic logical tables **`cpi`** and **`gdp`** map 1:1 to `V_CPI` and `V_GDP`. Use **`macro_wide`** when the user wants multiple macro indicators on one timeline (see `hackathon/sql/02_economic_indicators_wide.sql`).

| # | Question | Type | Expected | Result | Notes |
|---|----------|------|----------|--------|-------|
| 1 | What is the latest unemployment rate in this dataset? | Basic | Latest month + rate | **pass** | Filter overall series (`VARIABLE_NAME`); use `MAX(DATE)` or order by date. |
| 2 | Show me the unemployment rate trend since 2019 | Time series | Line chart | **pass** | Matches verified-query pattern on `V_UNEMPLOYMENT`. |
| 3 | What were average monthly Treasury bill rates in 2022? | Basic | Series / scalar | **pass** | Replaces CPI-only prompt; `V_INTEREST_RATES` + `VARIABLE_NAME` Treasury bill filters. |
| 4 | When did the Fed funds or policy rate series start rising in 2022 in this data? | Basic | Date / series | **partial** | Depends on series naming in `V_INTEREST_RATES`; may need user to pick a specific `VARIABLE_NAME`. |
| 5 | What is the most recent federal funds rate observation? | Basic | Metric + date | **partial** | Weekly/monthly mix; correct instrument filter matters. |
| 6 | Show industrial production index trend over the last five years | Time series | Line | **pass** | Replaces GDP; `V_INDUSTRIAL_PRODUCTION` — may aggregate across series or filter one `VARIABLE_NAME`. |
| 7 | What is the 10-year Treasury yield trend since 2020? | Basic | Series | **pass** | `V_INTEREST_RATES` includes Treasury-named series; filter `VARIABLE_NAME` for 10-year if present. |
| 8 | Show retail sales trend since 2020 | Time series | Line | **pass** | `V_RETAIL_SALES`; prefer `UNIT = 'USD'` for levels. |
| 9 | Compare unemployment trend and total retail sales (USD) since 2020 | Comparative | Multi-series | **pass** | Use **`macro_wide`** / `ECONOMIC_INDICATORS_WIDE`: `UNEMPLOYMENT_RATE` and `RETAIL_SALES` share `OBSERVATION_DATE`. |
| 10 | How did interest rates change during 2022? | Comparative | Line/bar | **pass** | Verified query + app fallback `SQL_FALLBACK_INTEREST_2022_2023`. |
| 11 | Show one macro snapshot for 2020: average unemployment and average industrial production index | Comparative | Small table | **pass** | **`macro_wide`**: filter `OBSERVATION_YEAR = 2020`, `AVG(UNEMPLOYMENT_RATE)`, `AVG(INDUSTRIAL_PRODUCTION)`. |
| 12 | Which calendar year had the highest average unemployment in the sample? | Analytical | Year | **pass** | Aggregate `V_UNEMPLOYMENT` by year. |
| 13 | What was unemployment around the COVID shock (early 2020)? | Analytical | Peak window | **pass** | Expect elevated rates around Apr–May 2020 on overall series. |
| 14 | How did total US retail sales (USD) compare in 2021 vs 2022? | Analytical | YoY / delta | **pass** | Replaces CPI delta; `SUM`/`AVG` of `RETAIL_SALES` with `UNIT='USD'`. |
| 15 | Show recovery: unemployment trend and industrial production trend after January 2020 | Analytical | Two trends | **pass** | Verified pattern on **`macro_wide`**; columns `UNEMPLOYMENT_RATE`, `INDUSTRIAL_PRODUCTION` vs `OBSERVATION_DATE`. |
| 16 | Show federal funds and Treasury bill rate trends on the same timeline since 2020 | Analytical | Multi-series | **pass** | Single view `V_INTEREST_RATES`; multiple `VARIABLE_NAME` values. |
| 17 | Is the economy doing well? | Ambiguous | Clarifier / narrative | **partial** | Model may ask which indicator; COMPLETE narrative still subjective. |
| 18 | Show me rates | Ambiguous | Clarifier | **partial** | Should narrow to Treasury, fed funds, or prime. |
| 19 | Compare pre- and post-COVID unemployment by quarter | Complex | Bar by quarter | **pass** | `V_UNEMPLOYMENT` only; `DATE_TRUNC('quarter', DATE)`. |
| 20 | List parent companies and their subsidiaries | Company graph | Table | **pass** | `V_COMPANY_RELATIONSHIPS`; parent-only edges. |
| 21 | How many rows are in the parent–subsidiary view? | Company graph | Single count | **pass** | `SELECT COUNT(*) FROM V_COMPANY_RELATIONSHIPS`. |
| 22 | Which parents have the most subsidiaries? | Company graph | Top-N | **pass** | Verified query; app fallback `SQL_FALLBACK_MOST_SUBSIDIARIES` if Analyst fails. |

## Appendix — CPI / GDP (dedicated views)

Headline series live in **`V_CPI`** and **`V_GDP`** with semantic tables **`cpi`** and **`gdp`**. The same values roll into **`macro_wide`** as **`CPI`** / **`GDP`**. If a view returns no rows, run the validation hints in `hackathon/economic_indicators_views.sql` and relax `VARIABLE_NAME` / `MEASURE` filters to match your listing.

| Question | Expected Result |
|----------|-----------------|
| What was peak CPI inflation in 2022? | **pass** — use **`cpi`** / verified YoY query on `V_CPI`, or **`macro_wide`** for levels + manual YoY. |
| Show GDP over the last five years | **pass** — use **`gdp`** / `V_GDP` (quarterly); chart as time series on `DATE`. |

**Notes:** Refine `semantic_model.yaml` verified queries and synonyms for any **partial** or **fail** you observe in production. Ensure **Snowflake Data: Finance & Economics** is available so `SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE` objects resolve. Full validation checklist: `streamlit_economic_intelligence/README.md` section 9.
