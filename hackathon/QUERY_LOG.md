# NL query log — Economic Intelligence (AI-02)

Run each prompt in Cortex Analyst (semantic YAML covers `ECONOMIC_INDICATORS_WIDE` **and** `V_COMPANY_RELATIONSHIPS`) or through the Streamlit app. Mark **pass** / **partial** / **fail** after validating SQL and row counts.

| # | Question | Type | Expected | Result |
|---|----------|------|----------|--------|
| 1 | What is the current unemployment rate? | Basic | Single value | |
| 2 | Show me the unemployment rate trend since 2019 | Time series | Line chart | |
| 3 | What was peak inflation in 2022? | Basic | ~9.1% CPI context | |
| 4 | When did the Fed start raising interest rates in this dataset? | Basic | Date / series | |
| 5 | What is the current Fed Funds Rate? | Basic | Metric | |
| 6 | Show GDP over the last 5 years | Time series | Line | |
| 7 | What is the 10-year Treasury yield trend? | Basic | Series | |
| 8 | Show retail sales trend since 2020 | Time series | Line | |
| 9 | Compare unemployment and CPI since 2020 | Comparative | Multi-series | |
| 10 | How did interest rates change during 2022? | Comparative | Line/bar | |
| 11 | Show key indicators for 2020 | Comparative | Table | |
| 12 | Which year had the highest unemployment in the sample? | Analytical | Year | |
| 13 | What was unemployment around the COVID shock? | Analytical | Peak ~14.7% Apr 2020 | |
| 14 | How did CPI change from 2021 to 2022? | Analytical | Delta | |
| 15 | Show recovery — unemployment and industrial production after 2020 | Analytical | Dual series | |
| 16 | Relationship between Fed funds and CPI | Analytical | Scatter/line | |
| 17 | Is the economy doing well? | Ambiguous | Clarifier | |
| 18 | Show me rates | Ambiguous | Clarifier | |
| 19 | Compare pre- and post-COVID unemployment by quarter | Complex | Bar by quarter | |
| 20 | List parent companies and their subsidiaries | Company graph | Table from `V_COMPANY_RELATIONSHIPS` | |
| 21 | How many parent relationship rows exist? | Company graph | Single count | |
| 22 | Which parents have the most subsidiaries? | Company graph | Top-N by count | |

**Notes:** Refine `economic_model.yaml` verified queries and synonyms for any **fail** or **partial** rows. Ensure **Snowflake Data: Finance & Economics** is installed so `SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.COMPANY_RELATIONSHIPS` exists.
