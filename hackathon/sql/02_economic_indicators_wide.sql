-- Wide analytic table for Cortex Analyst semantic model (joins on GEO_ID + DATE).
-- Run after economic_indicators_views.sql. Adjust CPI/GDP filters if your listing uses different MEASURE labels.

CREATE OR REPLACE VIEW HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE AS
WITH
u AS (
    SELECT GEO_ID, "DATE" AS observation_date, unemployment_rate
    FROM HACKATHON.DATA.V_UNEMPLOYMENT
),
r AS (
    SELECT GEO_ID, "DATE" AS observation_date, retail_sales
    FROM HACKATHON.DATA.V_RETAIL_SALES
),
ff AS (
    SELECT GEO_ID, "DATE" AS observation_date, AVG(interest_rate) AS fed_funds_rate
    FROM HACKATHON.DATA.V_INTEREST_RATES
    WHERE LOWER(variable_name) LIKE '%federal funds%'
    GROUP BY GEO_ID, "DATE"
),
t10 AS (
    SELECT GEO_ID, "DATE" AS observation_date, AVG(interest_rate) AS treasury_10yr
    FROM HACKATHON.DATA.V_INTEREST_RATES
    WHERE LOWER(variable_name) LIKE '%10%year%'
       OR LOWER(variable_name) LIKE '%10-year%'
       OR LOWER(variable_name) LIKE '%10 yr%'
    GROUP BY GEO_ID, "DATE"
),
ip AS (
    SELECT GEO_ID, "DATE" AS observation_date, production_index AS industrial_production
    FROM HACKATHON.DATA.V_INDUSTRIAL_PRODUCTION
),
cpi AS (
    SELECT
        ts.geo_id,
        ts."DATE"::DATE AS observation_date,
        ts.value AS cpi
    FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
    JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
        ON ts.variable = att.variable
    WHERE att.measure ILIKE '%consumer price%'
      AND att.frequency = 'Monthly'
),
gdp AS (
    SELECT
        ts.geo_id,
        ts."DATE"::DATE AS observation_date,
        ts.value AS gdp
    FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
    JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
        ON ts.variable = att.variable
    WHERE att.measure ILIKE '%gross domestic product%'
      AND att.frequency IN ('Monthly', 'Quarterly', 'Annual')
),
spine AS (
    SELECT geo_id, observation_date FROM u
    UNION
    SELECT geo_id, observation_date FROM r
    UNION
    SELECT geo_id, observation_date FROM ff
    UNION
    SELECT geo_id, observation_date FROM t10
    UNION
    SELECT geo_id, observation_date FROM ip
    UNION
    SELECT geo_id, observation_date FROM cpi
    UNION
    SELECT geo_id, observation_date FROM gdp
)
SELECT
    s.geo_id,
    s.observation_date,
    YEAR(s.observation_date) AS observation_year,
    DATE_TRUNC('month', s.observation_date) AS observation_month,
    u.unemployment_rate,
    r.retail_sales,
    ff.fed_funds_rate,
    t10.treasury_10yr,
    ip.industrial_production,
    cpi.cpi,
    gdp.gdp
FROM spine s
LEFT JOIN u
    ON s.geo_id = u.geo_id AND s.observation_date = u.observation_date
LEFT JOIN r
    ON s.geo_id = r.geo_id AND s.observation_date = r.observation_date
LEFT JOIN ff
    ON s.geo_id = ff.geo_id AND s.observation_date = ff.observation_date
LEFT JOIN t10
    ON s.geo_id = t10.geo_id AND s.observation_date = t10.observation_date
LEFT JOIN ip
    ON s.geo_id = ip.geo_id AND s.observation_date = ip.observation_date
LEFT JOIN cpi
    ON s.geo_id = cpi.geo_id AND s.observation_date = cpi.observation_date
LEFT JOIN gdp
    ON s.geo_id = gdp.geo_id AND s.observation_date = gdp.observation_date;
