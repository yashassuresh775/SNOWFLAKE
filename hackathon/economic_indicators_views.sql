-- Safe to re-run: skip if database/schema already exist in your account.
--
-- Financial indicators source: most accounts use
--   SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_*
-- Some listings expose the same tables under
--   SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN.financial_economic_indicators_*
-- If V_CPI / V_GDP return 0 rows, run hackathon/sql/discover_cpi_gdp_filters.sql in your account,
-- then find/replace PUBLIC_DATA_FREE with CYBERSYN on the FROM/JOIN lines below (timeseries + attributes only).
--
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
-- V_CPI  (BLS — headline CPI / all-items style, monthly, seasonally adjusted when labeled)
-- Uses COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME): some listings populate the name only on attributes.
-- MEASURE may be NULL for CPI rows; NULL ILIKE ... fails, so CPI is also detected from the coalesced name.
-- ============================================================
CREATE OR REPLACE VIEW HACKATHON.DATA.V_CPI AS
SELECT
    ts.GEO_ID,
    ts.DATE,
    ts.VALUE        AS cpi_index,
    ts.UNIT,
    COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) AS VARIABLE_NAME,
    att.FREQUENCY
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
WHERE (
        att.RELEASE_SOURCE ILIKE '%Labor Statistics%'
        OR att.RELEASE_SOURCE ILIKE '%BLS%'
        OR TRIM(att.RELEASE_SOURCE) = 'Bureau of Labor Statistics'
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
        OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%cpi-u%'
        OR COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%consumer price index%'
        OR (
            COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%CPI%'
            AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) ILIKE '%U.S. city average%'
        )
      )
  AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) NOT ILIKE '%less food and energy%'
  AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) NOT ILIKE '%except food%'
  AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME) NOT ILIKE '%core%';

-- ============================================================
-- V_GDP  (BEA — GDP level series, quarterly preferred; broad MEASURE / VARIABLE_NAME match)
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
  AND ts.VARIABLE_NAME NOT ILIKE '%change%';

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
