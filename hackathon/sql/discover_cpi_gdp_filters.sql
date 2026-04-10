-- Discovery queries when V_CPI or V_GDP return 0 rows.
-- Run each block in Snowsight; use DISTINCT strings to tighten economic_indicators_views.sql if needed.
--
-- 1) Which schema has attributes? (PUBLIC_DATA_FREE vs CYBERSYN)
-- SHOW SCHEMAS IN DATABASE SNOWFLAKE_PUBLIC_DATA_FREE;
--
-- 2) CPI-like variable names on attributes (try PUBLIC_DATA_FREE first; if empty, swap schema to CYBERSYN)
SELECT DISTINCT VARIABLE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes
WHERE VARIABLE_NAME ILIKE '%price%'
   OR VARIABLE_NAME ILIKE '%CPI%'
   OR VARIABLE_NAME ILIKE '%consumer%'
ORDER BY 1
LIMIT 50;

-- 2b) CPI often appears under Federal Reserve / FRED in the same share (check RELEASE_SOURCE / RELEASE_NAME)
SELECT DISTINCT att.RELEASE_SOURCE, att.RELEASE_NAME, att.MEASURE, att.FREQUENCY
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
WHERE att.MEASURE ILIKE '%CPI%'
   OR att.MEASURE ILIKE '%consumer%price%'
   OR att.VARIABLE_NAME ILIKE '%CPI%'
   OR att.VARIABLE_NAME ILIKE '%consumer price%'
ORDER BY 1, 2
LIMIT 40;

-- 3) CPI-like MEASURE + RELEASE_SOURCE (includes NULL MEASURE rows — V_CPI used to drop these)
SELECT DISTINCT att.MEASURE, att.RELEASE_SOURCE, att.FREQUENCY, att.SEASONALLY_ADJUSTED
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
WHERE att.MEASURE ILIKE '%consumer%'
   OR att.MEASURE ILIKE '%CPI%'
   OR att.MEASURE ILIKE '%price%'
   OR att.MEASURE IS NULL
ORDER BY 1 NULLS FIRST, 2
LIMIT 80;

-- 4) GDP-like strings
SELECT DISTINCT att.MEASURE, att.RELEASE_SOURCE, att.FREQUENCY
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
WHERE att.MEASURE ILIKE '%GDP%'
   OR att.MEASURE ILIKE '%gross domestic%'
ORDER BY 1, 2
LIMIT 50;

-- 5) Same as (2) but CYBERSYN (uncomment if your listing uses it)
-- SELECT DISTINCT VARIABLE_NAME
-- FROM SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN.financial_economic_indicators_attributes
-- WHERE VARIABLE_NAME ILIKE '%price%' OR VARIABLE_NAME ILIKE '%CPI%' OR VARIABLE_NAME ILIKE '%consumer%'
-- ORDER BY 1 LIMIT 50;
