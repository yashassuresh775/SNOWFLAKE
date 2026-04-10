CREATE DATABASE HACKATHON;

CREATE SCHEMA HACKATHON.DATA;

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
-- V_COMPANY_RELATIONSHIPS  (Cybersyn — Company Relationship Graph, Marketplace)
-- ============================================================
-- Before running: replace CYBERSYN_MARKETPLACE_DB with the database created when you
-- installed the listing (check Databases in Snowsight or the Marketplace "Open" link).
-- Confirm object path: DESCRIBE TABLE CYBERSYN_MARKETPLACE_DB.PUBLIC_DATA.COMPANY_RELATIONSHIPS;
-- Confirm filter values: SELECT DISTINCT RELATIONSHIP_TYPE FROM ... LIMIT 100;
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
FROM CYBERSYN_MARKETPLACE_DB.PUBLIC_DATA.COMPANY_RELATIONSHIPS AS cr
WHERE UPPER(TRIM(cr.RELATIONSHIP_TYPE)) = 'PARENT';
-- Parent-only edges reduce duplicate / bidirectional rows for parent–subsidiary analysis.

SHOW VIEWS IN SCHEMA HACKATHON.DATA;
