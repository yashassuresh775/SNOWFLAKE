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
-- V_CPI  (headline CPI-U / all-items SA, monthly)
-- Tier A: exact FRED/BLS series ids. Tier B: strict metadata match (legacy).
-- Tier C: permissive monthly CPI-like rows + QUALIFY (one row per GEO_ID+DATE) when A/B return nothing in your account.
-- ============================================================
CREATE OR REPLACE VIEW HACKATHON.DATA.V_CPI AS
WITH cpi_base AS (
    SELECT
        ts.GEO_ID,
        ts.DATE,
        ts.VALUE,
        ts.UNIT,
        ts.VARIABLE,
        att.FREQUENCY,
        att.RELEASE_SOURCE,
        att.RELEASE_NAME,
        att.MEASURE,
        att.SEASONALLY_ADJUSTED,
        COALESCE(
            NULLIF(TRIM(ts.VARIABLE_NAME), ''),
            NULLIF(TRIM(att.VARIABLE_NAME), ''),
            NULLIF(TRIM(ts.VARIABLE), ''),
            ''
        ) AS series_txt
    FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
    JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
        ON ts.VARIABLE = att.VARIABLE
),
tier_ab AS (
    SELECT
        GEO_ID,
        DATE,
        VALUE           AS cpi_index,
        UNIT,
        COALESCE(NULLIF(series_txt, ''), VARIABLE) AS VARIABLE_NAME,
        FREQUENCY
    FROM cpi_base
    WHERE (
            TRIM(FREQUENCY) = 'Monthly'
            OR FREQUENCY ILIKE 'Month%'
            OR UPPER(VARIABLE) LIKE '%CPIAUCSL%'
          )
      AND (
            (
                (
                    UPPER(VARIABLE) LIKE '%CPIAUCSL%'
                    AND UPPER(VARIABLE) NOT LIKE '%CPIAUCNS%'
                )
                OR UPPER(VARIABLE) LIKE '%CUSR0000SA0%'
            )
            OR (
                (
                    RELEASE_SOURCE ILIKE '%Labor Statistics%'
                    OR RELEASE_SOURCE ILIKE '%BLS%'
                    OR TRIM(RELEASE_SOURCE) = 'Bureau of Labor Statistics'
                    OR RELEASE_SOURCE ILIKE '%Federal Reserve%'
                    OR RELEASE_SOURCE ILIKE '%FRED%'
                    OR RELEASE_SOURCE ILIKE '%Economic Data%'
                    OR RELEASE_NAME ILIKE '%Bureau of Labor Statistics%'
                    OR RELEASE_NAME ILIKE '%Consumer Price%'
                    OR RELEASE_NAME ILIKE '%Labor Statistics%'
                )
                AND (
                    TRIM(SEASONALLY_ADJUSTED) = 'Seasonally adjusted'
                    OR SEASONALLY_ADJUSTED ILIKE 'Seasonally adjusted%'
                    OR SEASONALLY_ADJUSTED ILIKE '%Seasonally adjusted%'
                    OR UPPER(TRIM(COALESCE(SEASONALLY_ADJUSTED, ''))) IN ('TRUE', 'YES', '1')
                    OR series_txt ILIKE '%seasonally adjust%'
                )
                AND (
                    MEASURE ILIKE '%consumer%price%'
                    OR MEASURE ILIKE '%CPI%'
                    OR MEASURE ILIKE '%price index%'
                    OR TRIM(MEASURE) = 'Consumer Price Index'
                    OR TRIM(MEASURE) ILIKE 'Consumer Price Index%'
                    OR (
                        TRIM(COALESCE(MEASURE, '')) ILIKE 'index'
                        AND series_txt ILIKE '%CPI%'
                    )
                    OR (
                        NULLIF(TRIM(COALESCE(MEASURE, '')), '') IS NULL
                        AND series_txt ILIKE '%CPI%'
                        AND (
                            series_txt ILIKE '%all items%'
                            OR series_txt ILIKE '%all urban%'
                            OR series_txt ILIKE '%U.S. city average%'
                            OR series_txt ILIKE '%city average%'
                        )
                    )
                )
                AND (
                    series_txt ILIKE '%all items%'
                    OR series_txt ILIKE '%all urban%'
                    OR series_txt ILIKE '%urban consumers%'
                    OR series_txt ILIKE '%cpi-u%'
                    OR series_txt ILIKE '%consumer price index%'
                    OR series_txt ILIKE '%cpiaucsl%'
                    OR (series_txt ILIKE '%CPI%' AND series_txt ILIKE '%U.S. city average%')
                    OR (series_txt ILIKE '%CPI%' AND series_txt ILIKE '%city average%')
                )
            )
          )
      AND series_txt NOT ILIKE '%less food and energy%'
      AND series_txt NOT ILIKE '%except food%'
      AND series_txt NOT ILIKE '%core%'
),
tier_c AS (
    SELECT
        GEO_ID,
        DATE,
        VALUE           AS cpi_index,
        UNIT,
        COALESCE(NULLIF(series_txt, ''), VARIABLE) AS VARIABLE_NAME,
        FREQUENCY
    FROM cpi_base
    WHERE (TRIM(FREQUENCY) = 'Monthly' OR FREQUENCY ILIKE 'Month%')
      AND VALUE BETWEEN 25 AND 900
      AND (
            UPPER(VARIABLE) LIKE '%CPIAUCSL%'
            OR UPPER(VARIABLE) LIKE '%CUSR0000SA0%'
            OR MEASURE ILIKE '%consumer%price%'
            OR MEASURE ILIKE '%CPI%'
            OR series_txt ILIKE '%consumer price%'
            OR series_txt ILIKE '%cpi%'
            OR UPPER(VARIABLE) LIKE '%CPI%'
          )
      AND UPPER(VARIABLE) NOT LIKE '%CPIAUCNS%'
      AND series_txt NOT ILIKE '%less food and energy%'
      AND series_txt NOT ILIKE '%except food%'
      AND series_txt NOT ILIKE '%core%'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY GEO_ID, DATE
        ORDER BY
            CASE
                WHEN UPPER(VARIABLE) LIKE '%CPIAUCSL%' AND UPPER(VARIABLE) NOT LIKE '%CPIAUCNS%' THEN 0
                WHEN UPPER(VARIABLE) LIKE '%CUSR0000SA0%' THEN 1
                WHEN series_txt ILIKE '%all items%' OR series_txt ILIKE '%all urban%' THEN 2
                ELSE 3
            END,
            VARIABLE
    ) = 1
)
SELECT * FROM tier_ab
UNION ALL
SELECT * FROM tier_c c
WHERE NOT EXISTS (SELECT 1 FROM tier_ab a WHERE a.GEO_ID = c.GEO_ID AND a.DATE = c.DATE);

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
  AND ts.VARIABLE_NAME NOT ILIKE '%change%'
  /* Exclude industry share / percentage-of-GDP rows (not headline real GDP level). */
  AND ts.VARIABLE_NAME NOT ILIKE '%percentage%'
  AND ts.VARIABLE_NAME NOT ILIKE '%value added by industry%'
  AND ts.VARIABLE_NAME NOT ILIKE '%contribution to percent change%'
  AND ts.VARIABLE_NAME NOT ILIKE '%share of gdp%'
  AND COALESCE(ts.UNIT, att.UNIT, '') NOT ILIKE '%percent%'
  /* Headline level (strict) OR permissive quarterly level rows (VALUE >> industry % shares). */
  AND (
        (
            ts.VARIABLE_NAME ILIKE '%real gross domestic product%'
            OR ts.VARIABLE_NAME ILIKE '%real gdp%'
            OR (
                ts.VARIABLE_NAME ILIKE '%gross domestic product%'
                AND ts.VARIABLE_NAME NOT ILIKE '%industry%'
                AND ts.VARIABLE_NAME NOT ILIKE '%sector%'
            )
            OR (
                ts.VARIABLE_NAME ILIKE '%gdp%'
                AND (
                    COALESCE(ts.UNIT, att.UNIT, '') ILIKE '%billion%'
                    OR COALESCE(ts.UNIT, att.UNIT, '') ILIKE '%dollar%'
                )
                AND ts.VARIABLE_NAME NOT ILIKE '%industry%'
                AND ts.VARIABLE_NAME NOT ILIKE '%sector%'
            )
        )
        OR (
            att.FREQUENCY = 'Quarterly'
            AND ts.VALUE IS NOT NULL
            AND TRY_TO_DOUBLE(ts.VALUE) > 500
            AND ts.VARIABLE_NAME NOT ILIKE '%industry%'
            AND ts.VARIABLE_NAME NOT ILIKE '%sector%'
            AND ts.VARIABLE_NAME NOT ILIKE '%percentage%'
            AND ts.VARIABLE_NAME NOT ILIKE '%value added%'
            AND ts.VARIABLE_NAME NOT ILIKE '%contribution%'
            AND (
                ts.VARIABLE_NAME ILIKE '%gross domestic product%'
                OR (
                    ts.VARIABLE_NAME ILIKE '%gdp%'
                    AND LENGTH(COALESCE(ts.VARIABLE_NAME, '')) < 120
                )
            )
        )
      );

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
