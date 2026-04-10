-- Safe to re-run: skip if database/schema already exist in your account.
CREATE DATABASE IF NOT EXISTS HACKATHON;

CREATE SCHEMA IF NOT EXISTS HACKATHON.DATA;

CREATE OR REPLACE VIEW HACKATHON.DATA.V_UNEMPLOYMENT AS
SELECT
    ts.GEO_ID,
    ts.DATE,
    ts.VALUE        AS unemployment_rate,
    ts.UNIT,
    ts.VARIABLE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
WHERE att.MEASURE         = 'Unemployment Rate'
AND   att.RELEASE_SOURCE  = 'Bureau of Labor Statistics'
AND   att.SEASONALLY_ADJUSTED = 'Seasonally adjusted'
AND   att.FREQUENCY       = 'Monthly';

-- ============================================================
-- V_RETAIL_SALES  (US Census Bureau, monthly retail figures)
-- ============================================================
CREATE OR REPLACE VIEW HACKATHON.DATA.V_RETAIL_SALES AS
SELECT
    ts.GEO_ID,
    ts.DATE,
    ts.VALUE        AS retail_sales,
    ts.UNIT,
    ts.VARIABLE_NAME,
    att.INDUSTRY
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
WHERE att.MEASURE         = 'Retail Sales'
AND   att.RELEASE_SOURCE  = 'US Census Bureau'
AND   att.SEASONALLY_ADJUSTED = 'Seasonally adjusted'
AND   att.FREQUENCY       = 'Monthly';

-- ============================================================
-- V_INTEREST_RATES  (Federal Reserve)
-- ============================================================
CREATE OR REPLACE VIEW HACKATHON.DATA.V_INTEREST_RATES AS
SELECT
    ts.GEO_ID,
    ts.DATE,
    ts.VALUE        AS interest_rate,
    ts.UNIT,
    ts.VARIABLE_NAME,
    att.RELEASE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
WHERE att.RELEASE_SOURCE  = 'Federal Reserve'
AND   att.FREQUENCY       IN ('Monthly', 'Weekly')
AND   att.SEASONALLY_ADJUSTED = 'Not seasonally adjusted'
AND   (
    LOWER(ts.VARIABLE_NAME) LIKE '%federal funds%'
    OR LOWER(ts.VARIABLE_NAME) LIKE '%interest rate%'
    OR LOWER(ts.VARIABLE_NAME) LIKE '%treasury%'
    OR LOWER(ts.VARIABLE_NAME) LIKE '%prime rate%'
);

-- ============================================================
-- V_INDUSTRIAL_PRODUCTION  (Federal Reserve)
-- ============================================================
CREATE OR REPLACE VIEW HACKATHON.DATA.V_INDUSTRIAL_PRODUCTION AS
SELECT
    ts.GEO_ID,
    ts.DATE,
    ts.VALUE        AS production_index,
    ts.UNIT,
    ts.VARIABLE_NAME,
    att.INDUSTRY
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
WHERE att.MEASURE         = 'Industrial Production'
AND   att.RELEASE_SOURCE  = 'Federal Reserve'
AND   att.SEASONALLY_ADJUSTED = 'Seasonally adjusted'
AND   att.FREQUENCY       = 'Monthly';

-- ============================================================
-- V_CPI  (BLS — headline CPI-U, all items, seasonally adjusted, monthly)
-- Validate if empty: SELECT DISTINCT MEASURE, VARIABLE_NAME FROM ...attributes a JOIN ...timeseries t ON a.VARIABLE=t.VARIABLE
--   WHERE a.RELEASE_SOURCE ILIKE '%Labor Statistics%' AND a.MEASURE ILIKE '%consumer%price%' LIMIT 50;
-- ============================================================
CREATE OR REPLACE VIEW HACKATHON.DATA.V_CPI AS
SELECT
    ts.GEO_ID,
    ts.DATE,
    ts.VALUE        AS cpi_index,
    ts.UNIT,
    ts.VARIABLE_NAME,
    att.FREQUENCY
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
WHERE att.RELEASE_SOURCE = 'Bureau of Labor Statistics'
  AND att.FREQUENCY = 'Monthly'
  AND att.SEASONALLY_ADJUSTED = 'Seasonally adjusted'
  AND (
        att.MEASURE = 'Consumer Price Index'
        OR TRIM(att.MEASURE) ILIKE 'Consumer Price Index%'
      )
  AND ts.VARIABLE_NAME ILIKE '%All Urban Consumers%'
  AND ts.VARIABLE_NAME ILIKE '%All Items%'
  AND ts.VARIABLE_NAME NOT ILIKE '%less food and energy%';

-- ============================================================
-- V_GDP  (BEA — real GDP, quarterly, seasonally adjusted annual rate)
-- Validate if empty: relax VARIABLE_NAME (remove chained filter) or check MEASURE / RELEASE_SOURCE strings in your listing.
-- ============================================================
CREATE OR REPLACE VIEW HACKATHON.DATA.V_GDP AS
SELECT
    ts.GEO_ID,
    ts.DATE,
    ts.VALUE        AS gdp_value,
    ts.UNIT,
    ts.VARIABLE_NAME,
    att.FREQUENCY
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
WHERE (att.RELEASE_SOURCE = 'Bureau of Economic Analysis'
    OR att.RELEASE_SOURCE ILIKE '%Bureau of Economic Analysis%')
  AND att.FREQUENCY = 'Quarterly'
  AND (
        att.MEASURE = 'Gross Domestic Product'
        OR TRIM(att.MEASURE) ILIKE '%Gross Domestic Product%'
      )
  AND ts.VARIABLE_NAME ILIKE '%gross domestic product%'
  AND ts.VARIABLE_NAME ILIKE '%seasonally adjusted annual rate%'
  AND ts.VARIABLE_NAME ILIKE '%chained%'
  AND ts.VARIABLE_NAME NOT ILIKE '%per capita%';

-- ============================================================
-- V_COMPANY_RELATIONSHIPS  (Snowflake Data: Finance & Economics — same listing as macro feeds)
-- ============================================================
-- Source: SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.COMPANY_RELATIONSHIPS
-- Validate: DESCRIBE TABLE SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.COMPANY_RELATIONSHIPS;
--           SELECT DISTINCT RELATIONSHIP_TYPE FROM ... LIMIT 100;
CREATE OR REPLACE VIEW HACKATHON.DATA.V_COMPANY_RELATIONSHIPS AS
SELECT
    cr.COMPANY_ID,
    cr.COMPANY_NAME,
    cr.RELATED_COMPANY_ID,
    cr.RELATED_COMPANY_NAME,
    cr.ENTITY_LEVEL,
    cr.RELATIONSHIP_TYPE,
    cr.RELATIONSHIP_START_DATE,
    cr.RELATIONSHIP_END_DATE
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.COMPANY_RELATIONSHIPS AS cr
WHERE UPPER(TRIM(cr.RELATIONSHIP_TYPE)) = 'PARENT';
-- Parent-only edges reduce duplicate / bidirectional rows for parent–subsidiary analysis.

SHOW VIEWS IN SCHEMA HACKATHON.DATA;

-- Multi-macro panel (shared GEO_ID + OBSERVATION_DATE) for Analyst `macro_wide`: run hackathon/sql/02_economic_indicators_wide.sql next.
