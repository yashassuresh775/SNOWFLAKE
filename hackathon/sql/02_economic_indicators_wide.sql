-- Curated macro panel: one row per (GEO_ID, OBSERVATION_DATE) with multiple measures for join/compare story.
-- Run after hackathon/economic_indicators_views.sql.
-- Used by: Cortex semantic table `macro_wide` (ECONOMIC_INDICATORS_WIDE), optional Streamlit header KPIs.
--
-- Aggregations: headline-style unemployment (excludes age/gender splits), sum of USD retail, avg Fed funds /
-- 10-year Treasury from rate view, avg industrial production index; CPI and GDP from V_CPI / V_GDP (headline series).

CREATE OR REPLACE VIEW HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE AS
WITH
u AS (
    SELECT
        GEO_ID,
        "DATE" AS observation_date,
        AVG(unemployment_rate) AS unemployment_rate
    FROM HACKATHON.DATA.V_UNEMPLOYMENT
    WHERE VARIABLE_NAME LIKE '%Unemployment Rate%'
      AND VARIABLE_NAME NOT LIKE '%yrs%'
      AND VARIABLE_NAME NOT LIKE '%, Men%'
      AND VARIABLE_NAME NOT LIKE '%, Women%'
    GROUP BY GEO_ID, "DATE"
),
r AS (
    SELECT
        GEO_ID,
        "DATE" AS observation_date,
        SUM(retail_sales) AS retail_sales
    FROM HACKATHON.DATA.V_RETAIL_SALES
    WHERE UNIT = 'USD'
    GROUP BY GEO_ID, "DATE"
),
ff AS (
    SELECT
        GEO_ID,
        "DATE" AS observation_date,
        AVG(interest_rate) AS fed_funds_rate
    FROM HACKATHON.DATA.V_INTEREST_RATES
    WHERE LOWER(VARIABLE_NAME) LIKE '%federal funds%'
    GROUP BY GEO_ID, "DATE"
),
t10 AS (
    SELECT
        GEO_ID,
        "DATE" AS observation_date,
        AVG(interest_rate) AS treasury_10yr
    FROM HACKATHON.DATA.V_INTEREST_RATES
    WHERE LOWER(VARIABLE_NAME) LIKE '%10%year%'
       OR LOWER(VARIABLE_NAME) LIKE '%10-year%'
       OR LOWER(VARIABLE_NAME) LIKE '%10 yr%'
    GROUP BY GEO_ID, "DATE"
),
ip AS (
    SELECT
        GEO_ID,
        "DATE" AS observation_date,
        AVG(production_index) AS industrial_production
    FROM HACKATHON.DATA.V_INDUSTRIAL_PRODUCTION
    GROUP BY GEO_ID, "DATE"
),
cpi AS (
    SELECT
        GEO_ID,
        "DATE" AS observation_date,
        AVG(cpi_index) AS cpi
    FROM HACKATHON.DATA.V_CPI
    GROUP BY GEO_ID, "DATE"
),
gdp AS (
    SELECT
        GEO_ID,
        "DATE" AS observation_date,
        AVG(gdp_value) AS gdp
    FROM HACKATHON.DATA.V_GDP
    GROUP BY GEO_ID, "DATE"
),
spine AS (
    SELECT GEO_ID, observation_date FROM u
    UNION
    SELECT GEO_ID, observation_date FROM r
    UNION
    SELECT GEO_ID, observation_date FROM ff
    UNION
    SELECT GEO_ID, observation_date FROM t10
    UNION
    SELECT GEO_ID, observation_date FROM ip
    UNION
    SELECT GEO_ID, observation_date FROM cpi
    UNION
    SELECT GEO_ID, observation_date FROM gdp
)
SELECT
    s.GEO_ID,
    s.observation_date AS OBSERVATION_DATE,
    YEAR(s.observation_date) AS OBSERVATION_YEAR,
    DATE_TRUNC('month', s.observation_date) AS OBSERVATION_MONTH,
    u.unemployment_rate AS UNEMPLOYMENT_RATE,
    r.retail_sales AS RETAIL_SALES,
    ff.fed_funds_rate AS FED_FUNDS_RATE,
    t10.treasury_10yr AS TREASURY_10YR,
    ip.industrial_production AS INDUSTRIAL_PRODUCTION,
    cpi.cpi AS CPI,
    gdp.gdp AS GDP
FROM spine s
LEFT JOIN u
    ON s.GEO_ID = u.GEO_ID AND s.observation_date = u.observation_date
LEFT JOIN r
    ON s.GEO_ID = r.GEO_ID AND s.observation_date = r.observation_date
LEFT JOIN ff
    ON s.GEO_ID = ff.GEO_ID AND s.observation_date = ff.observation_date
LEFT JOIN t10
    ON s.GEO_ID = t10.GEO_ID AND s.observation_date = t10.observation_date
LEFT JOIN ip
    ON s.GEO_ID = ip.GEO_ID AND s.observation_date = ip.observation_date
LEFT JOIN cpi
    ON s.GEO_ID = cpi.GEO_ID AND s.observation_date = cpi.observation_date
LEFT JOIN gdp
    ON s.GEO_ID = gdp.GEO_ID AND s.observation_date = gdp.observation_date;
