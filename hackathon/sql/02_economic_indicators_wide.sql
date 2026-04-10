-- Curated macro panel: one row per (GEO_ID, OBSERVATION_DATE) with multiple measures for join/compare story.
--
-- Prerequisites: HACKATHON.DATA.V_UNEMPLOYMENT, V_RETAIL_SALES, V_INTEREST_RATES, V_INDUSTRIAL_PRODUCTION
-- (run hackathon/economic_indicators_views.sql through those views at minimum).
-- CPI and GDP are inlined here from SNOWFLAKE_PUBLIC_DATA_FREE — you do NOT need V_CPI / V_GDP for this script.
-- Still create V_CPI and V_GDP in the full views script for Cortex granular logical tables.
--
-- Used by: semantic `macro_wide` (ECONOMIC_INDICATORS_WIDE).

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
        ts.GEO_ID,
        ts."DATE" AS observation_date,
        AVG(ts.VALUE) AS cpi
    FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
    JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
        ON ts.VARIABLE = att.VARIABLE
    WHERE (
            att.RELEASE_SOURCE ILIKE '%Labor Statistics%'
            OR att.RELEASE_SOURCE ILIKE '%BLS%'
            OR TRIM(att.RELEASE_SOURCE) = 'Bureau of Labor Statistics'
            OR att.RELEASE_SOURCE ILIKE '%Federal Reserve%'
            OR att.RELEASE_SOURCE ILIKE '%FRED%'
            OR att.RELEASE_SOURCE ILIKE '%Economic Data%'
            OR att.RELEASE_NAME ILIKE '%Bureau of Labor Statistics%'
            OR att.RELEASE_NAME ILIKE '%Consumer Price%'
            OR att.RELEASE_NAME ILIKE '%Labor Statistics%'
          )
      AND (
            TRIM(att.FREQUENCY) = 'Monthly'
            OR att.FREQUENCY ILIKE 'Month%'
          )
      AND (
            TRIM(att.SEASONALLY_ADJUSTED) = 'Seasonally adjusted'
            OR att.SEASONALLY_ADJUSTED ILIKE 'Seasonally adjusted%'
            OR att.SEASONALLY_ADJUSTED ILIKE '%Seasonally adjusted%'
            OR UPPER(TRIM(COALESCE(att.SEASONALLY_ADJUSTED, ''))) IN ('TRUE', 'YES', '1')
            OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%seasonally adjust%'
          )
      AND (
            att.MEASURE ILIKE '%consumer%price%'
            OR att.MEASURE ILIKE '%CPI%'
            OR att.MEASURE ILIKE '%price index%'
            OR TRIM(att.MEASURE) = 'Consumer Price Index'
            OR TRIM(att.MEASURE) ILIKE 'Consumer Price Index%'
            OR (
                TRIM(COALESCE(att.MEASURE, '')) ILIKE 'index'
                AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%CPI%'
            )
            OR (
                NULLIF(TRIM(COALESCE(att.MEASURE, '')), '') IS NULL
                AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%CPI%'
                AND (
                    COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%all items%'
                    OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%all urban%'
                    OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%U.S. city average%'
                    OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%city average%'
                )
            )
          )
      AND (
            COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%all items%'
            OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%all urban%'
            OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%urban consumers%'
            OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%cpi-u%'
            OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%consumer price index%'
            OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%cpiaucsl%'
            OR (
                COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%CPI%'
                AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%U.S. city average%'
            )
            OR (
                COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%CPI%'
                AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%city average%'
            )
          )
      AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) NOT ILIKE '%less food and energy%'
      AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) NOT ILIKE '%except food%'
      AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) NOT ILIKE '%core%'
    GROUP BY ts.GEO_ID, ts."DATE"
),
gdp AS (
    SELECT
        ts.GEO_ID,
        ts."DATE" AS observation_date,
        AVG(ts.VALUE) AS gdp
    FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
    JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
        ON ts.VARIABLE = att.VARIABLE
    WHERE (
            att.RELEASE_SOURCE ILIKE '%Economic Analysis%'
            OR att.RELEASE_SOURCE ILIKE '%BEA%'
            OR TRIM(att.RELEASE_SOURCE) = 'Bureau of Economic Analysis'
          )
      AND att.FREQUENCY IN ('Quarterly', 'Annual')
      AND (
            att.MEASURE ILIKE '%gross domestic product%'
            OR att.MEASURE ILIKE '%GDP%'
            OR TRIM(att.MEASURE) = 'Gross Domestic Product'
          )
      AND (
            ts.VARIABLE_NAME ILIKE '%gross domestic%'
            OR ts.VARIABLE_NAME ILIKE '%GDP%'
          )
      AND ts.VARIABLE_NAME NOT ILIKE '%per capita%'
      AND ts.VARIABLE_NAME NOT ILIKE '%growth%'
      AND ts.VARIABLE_NAME NOT ILIKE '%change%'
    GROUP BY ts.GEO_ID, ts."DATE"
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
