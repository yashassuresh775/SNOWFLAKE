"""
US Economic Intelligence — Streamlit in Snowflake
Cortex Analyst (semantic YAML) + Cortex COMPLETE narratives (persona modes), native charts,
z-score anomaly highlights, economic event context, wide-panel correlation line, PDF brief export.
"""

from __future__ import annotations

import html
import inspect
import io
import difflib
import json
import os
import re
from datetime import datetime
from typing import Any

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

# ── session ─────────────────────────────────────────────────────────────────
try:
    from snowflake.snowpark.context import get_active_session

    session = get_active_session()
except Exception:  # noqa: BLE001
    session = None

# ── config ────────────────────────────────────────────────────────────────
SEMANTIC_MODEL_FILE = os.environ.get(
    "SEMANTIC_MODEL_FILE",
    "@HACKATHON.DATA.SEMANTIC_MODELS/semantic_model.yaml",
)
CORTEX_COMPLETE_MODEL = os.environ.get("CORTEX_COMPLETE_MODEL", "mistral-large2")
CORTEX_ANALYST_PATH = "/api/v2/cortex/analyst/message"

PERSONAS: dict[str, str] = {
    "Executive": (
        "You are a Chief Economist briefing the board: be concise, plain language, "
        "business implications first, no jargon unless necessary."
    ),
    "Analyst": (
        "You are a quantitative economist: cite specific numbers and ranges, note trends "
        "and volatility, stay technical and precise."
    ),
    "Press": (
        "You are a financial journalist: open with a punchy lede; at most two short sentences; "
        "headline-worthy and readable."
    ),
}

# Short labels for the UI (aligned with PERSONAS prompts above).
PERSONA_UI_HINTS: dict[str, str] = {
    "Executive": "Board-style brief: concise, plain language, business implications first, minimal jargon.",
    "Analyst": "Quantitative: cite numbers and ranges, call out trends and volatility.",
    "Press": "Newsroom voice: punchy lede, at most two short sentences, headline-ready.",
}

# Notable dates overlapping typical macro series (YYYY-MM-DD).
ECONOMIC_EVENTS: tuple[tuple[str, str], ...] = (
    ("2020-03-15", "COVID shock — emergency policy / market stress"),
    ("2020-04-01", "Labor market collapse — unemployment spike"),
    ("2021-06-16", "Fed taper / tightening signal"),
    ("2022-03-16", "Fed hike cycle begins"),
    ("2022-06-01", "Headline CPI pressure — multi-decade highs"),
    ("2022-09-01", "Global tightening / dollar strength"),
    ("2023-03-10", "Regional bank stress (SVB)"),
    ("2023-07-26", "Policy rate held in restrictive band"),
    ("2024-09-18", "Easing expectations build"),
)

# Verified SQL when Analyst returns a broken CTE (parent column dropped from scope).
SQL_FALLBACK_MOST_SUBSIDIARIES = """
SELECT COMPANY_NAME AS parent_company,
       COUNT(RELATED_COMPANY_NAME) AS subsidiary_count
FROM HACKATHON.DATA.V_COMPANY_RELATIONSHIPS
GROUP BY COMPANY_NAME
ORDER BY subsidiary_count DESC
LIMIT 15
""".strip()

SQL_FALLBACK_TOP_RETAIL_2023 = """
SELECT VARIABLE_NAME AS category, SUM(RETAIL_SALES) AS total_sales
FROM HACKATHON.DATA.V_RETAIL_SALES
WHERE "DATE" BETWEEN '2023-01-01' AND '2023-12-31'
  AND UNIT = 'USD'
GROUP BY VARIABLE_NAME
ORDER BY total_sales DESC
LIMIT 10
""".strip()

SQL_FALLBACK_AEROSPACE_2019_2023 = """
SELECT "DATE", AVG(PRODUCTION_INDEX) AS production_index
FROM HACKATHON.DATA.V_INDUSTRIAL_PRODUCTION
WHERE (VARIABLE_NAME ILIKE '%Aerospace%' OR VARIABLE_NAME ILIKE '%Aircraft%')
  AND "DATE" BETWEEN '2019-01-01' AND '2023-12-31'
GROUP BY "DATE"
ORDER BY "DATE"
""".strip()

SQL_FALLBACK_KROGER_SUBSIDIARIES = """
SELECT RELATED_COMPANY_NAME AS subsidiary
FROM HACKATHON.DATA.V_COMPANY_RELATIONSHIPS
WHERE COMPANY_NAME = 'KROGER CO'
ORDER BY RELATED_COMPANY_NAME
""".strip()

SQL_FALLBACK_MARRIOTT_SUBSIDIARIES = """
SELECT RELATED_COMPANY_NAME AS subsidiary
FROM HACKATHON.DATA.V_COMPANY_RELATIONSHIPS
WHERE COMPANY_NAME = 'MARRIOTT INTERNATIONAL INC /MD/'
ORDER BY RELATED_COMPANY_NAME
""".strip()

SQL_FALLBACK_RETAIL_BEFORE_AFTER_HIKES_2022 = """
WITH monthly_sales AS (
  SELECT
    DATE_TRUNC('month', "DATE") AS month,
    SUM(RETAIL_SALES) AS total_sales
  FROM HACKATHON.DATA.V_RETAIL_SALES
  WHERE UNIT = 'USD'
    AND "DATE" BETWEEN '2019-01-01' AND '2024-12-31'
  GROUP BY 1
),
growth AS (
  SELECT
    month,
    total_sales,
    100 * (total_sales - LAG(total_sales) OVER (ORDER BY month))
      / NULLIF(LAG(total_sales) OVER (ORDER BY month), 0) AS mom_growth_pct
  FROM monthly_sales
)
SELECT
  CASE WHEN month < '2022-03-01' THEN 'before_hikes' ELSE 'after_hikes' END AS period,
  ROUND(AVG(mom_growth_pct), 2) AS avg_mom_growth_pct,
  ROUND(AVG(total_sales), 2) AS avg_monthly_sales,
  COUNT(*) AS months
FROM growth
WHERE mom_growth_pct IS NOT NULL
GROUP BY 1
ORDER BY 1
""".strip()

SQL_FALLBACK_UNEMPLOYMENT_MEN_WOMEN_2022 = """
SELECT
  "DATE",
  AVG(CASE WHEN VARIABLE_NAME LIKE '%, Men,%' THEN UNEMPLOYMENT_RATE END) AS male_rate,
  AVG(CASE WHEN VARIABLE_NAME LIKE '%, Women,%' THEN UNEMPLOYMENT_RATE END) AS female_rate
FROM HACKATHON.DATA.V_UNEMPLOYMENT
WHERE "DATE" BETWEEN '2022-01-01' AND '2022-12-31'
  AND VARIABLE_NAME LIKE '%Unemployment Rate%'
GROUP BY "DATE"
ORDER BY "DATE"
""".strip()

SQL_FALLBACK_AUTO_RETAIL_2019_2023 = """
SELECT "DATE", SUM(RETAIL_SALES) AS auto_sales
FROM HACKATHON.DATA.V_RETAIL_SALES
WHERE VARIABLE_NAME LIKE '%Auto and Other Motor Vehicles%'
  AND UNIT = 'USD'
  AND "DATE" BETWEEN '2019-01-01' AND '2023-12-31'
GROUP BY "DATE"
ORDER BY "DATE"
""".strip()

SQL_FALLBACK_TREASURY_SINCE_2020 = """
SELECT "DATE", AVG(INTEREST_RATE) AS avg_rate, VARIABLE_NAME AS rate_type
FROM HACKATHON.DATA.V_INTEREST_RATES
WHERE "DATE" >= '2020-01-01'
  AND VARIABLE_NAME ILIKE '%Treasury bill%'
  AND VARIABLE_NAME ILIKE '%Monthly%'
GROUP BY "DATE", VARIABLE_NAME
ORDER BY "DATE"
""".strip()

SQL_FALLBACK_INTEREST_2022_2023 = """
SELECT "DATE", AVG(INTEREST_RATE) AS avg_rate
FROM HACKATHON.DATA.V_INTEREST_RATES
WHERE "DATE" BETWEEN '2022-01-01' AND '2023-12-31'
  AND VARIABLE_NAME ILIKE '%Treasury bill%'
  AND VARIABLE_NAME ILIKE '%Monthly%'
GROUP BY "DATE"
ORDER BY "DATE"
""".strip()

SQL_FALLBACK_TOP_INDUSTRIAL_2023 = """
SELECT VARIABLE_NAME AS sector, AVG(PRODUCTION_INDEX) AS avg_production_index
FROM HACKATHON.DATA.V_INDUSTRIAL_PRODUCTION
WHERE "DATE" BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY VARIABLE_NAME
ORDER BY avg_production_index DESC
LIMIT 10
""".strip()

SQL_FALLBACK_COMPANIES_GT5_SUBSIDIARIES = """
SELECT COMPANY_NAME AS parent_company,
       COUNT(RELATED_COMPANY_NAME) AS subsidiary_count
FROM HACKATHON.DATA.V_COMPANY_RELATIONSHIPS
GROUP BY COMPANY_NAME
HAVING COUNT(RELATED_COMPANY_NAME) > 5
ORDER BY subsidiary_count DESC
""".strip()

SQL_FALLBACK_CPI_MONTHLY_2019_2024 = """
SELECT "DATE", CPI_INDEX, VARIABLE_NAME
FROM HACKATHON.DATA.V_CPI
WHERE "DATE" BETWEEN '2019-01-01' AND '2024-12-31'
ORDER BY "DATE"
""".strip()

# When V_CPI is empty (strict view filters vs listing metadata), pull headline CPI-U SA directly by series id.
SQL_FALLBACK_CPI_CPIAUCSL_PUBLIC = """
SELECT
  ts."DATE" AS "DATE",
  ts.VALUE AS CPI_INDEX,
  COALESCE(
    NULLIF(TRIM(ts.VARIABLE_NAME), ''),
    NULLIF(TRIM(att.VARIABLE_NAME), ''),
    ts.VARIABLE
  ) AS VARIABLE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
  ON ts.VARIABLE = att.VARIABLE
WHERE (
    (UPPER(ts.VARIABLE) LIKE '%CPIAUCSL%' AND UPPER(ts.VARIABLE) NOT LIKE '%CPIAUCNS%')
    OR UPPER(ts.VARIABLE) LIKE '%CUSR0000SA0%'
  )
  AND ts."DATE" BETWEEN '2019-01-01' AND '2024-12-31'
ORDER BY ts."DATE"
""".strip()

SQL_FALLBACK_CPI_CPIAUCSL_CYBERSYN = """
SELECT
  ts."DATE" AS "DATE",
  ts.VALUE AS CPI_INDEX,
  COALESCE(
    NULLIF(TRIM(ts.VARIABLE_NAME), ''),
    NULLIF(TRIM(att.VARIABLE_NAME), ''),
    ts.VARIABLE
  ) AS VARIABLE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN.financial_economic_indicators_attributes att
  ON ts.VARIABLE = att.VARIABLE
WHERE (
    (UPPER(ts.VARIABLE) LIKE '%CPIAUCSL%' AND UPPER(ts.VARIABLE) NOT LIKE '%CPIAUCNS%')
    OR UPPER(ts.VARIABLE) LIKE '%CUSR0000SA0%'
  )
  AND ts."DATE" BETWEEN '2019-01-01' AND '2024-12-31'
ORDER BY ts."DATE"
""".strip()

# CPI from macro panel (works when V_CPI is empty but wide view still has CPI column populated).
SQL_FALLBACK_CPI_FROM_MACRO_WIDE = """
SELECT
  OBSERVATION_DATE AS "DATE",
  CPI AS CPI_INDEX,
  'ECONOMIC_INDICATORS_WIDE.CPI' AS VARIABLE_NAME
FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
WHERE CPI IS NOT NULL
  AND OBSERVATION_DATE BETWEEN '2019-01-01' AND '2024-12-31'
ORDER BY OBSERVATION_DATE
""".strip()

# Pick the busiest monthly CPI-like series by row count (when VARIABLE id does not contain CPIAUCSL).
SQL_FALLBACK_CPI_LOOSE_PICK_PUBLIC = """
WITH candidates AS (
  SELECT ts.VARIABLE, COUNT(*) AS cnt
  FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
  INNER JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
  WHERE ts."DATE" BETWEEN '2019-01-01' AND '2024-12-31'
    AND ts.VALUE BETWEEN 25 AND 900
    AND (TRIM(att.FREQUENCY) = 'Monthly' OR att.FREQUENCY ILIKE 'Month%')
    AND (
      att.MEASURE ILIKE '%consumer price%'
      OR att.MEASURE ILIKE '%CPI%'
      OR ts.VARIABLE_NAME ILIKE '%consumer price%'
      OR att.VARIABLE_NAME ILIKE '%consumer price%'
    )
    AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME, '') NOT ILIKE '%core%'
    AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME, '') NOT ILIKE '%excluding food%'
  GROUP BY ts.VARIABLE
),
best AS (
  SELECT VARIABLE FROM candidates ORDER BY cnt DESC NULLS LAST LIMIT 1
)
SELECT
  ts."DATE" AS "DATE",
  ts.VALUE AS CPI_INDEX,
  COALESCE(
    NULLIF(TRIM(ts.VARIABLE_NAME), ''),
    NULLIF(TRIM(att.VARIABLE_NAME), ''),
    ts.VARIABLE
  ) AS VARIABLE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
  ON ts.VARIABLE = att.VARIABLE
JOIN best b ON ts.VARIABLE = b.VARIABLE
WHERE ts."DATE" BETWEEN '2019-01-01' AND '2024-12-31'
ORDER BY ts."DATE"
""".strip()

SQL_FALLBACK_CPI_LOOSE_PICK_PUBLIC_NO_BAND = """
WITH candidates AS (
  SELECT ts.VARIABLE, COUNT(*) AS cnt
  FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
  INNER JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
  WHERE ts."DATE" BETWEEN '2019-01-01' AND '2024-12-31'
    AND (TRIM(att.FREQUENCY) = 'Monthly' OR att.FREQUENCY ILIKE 'Month%')
    AND (
      att.MEASURE ILIKE '%consumer price%'
      OR att.MEASURE ILIKE '%CPI%'
      OR ts.VARIABLE_NAME ILIKE '%consumer price%'
      OR att.VARIABLE_NAME ILIKE '%consumer price%'
    )
    AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME, '') NOT ILIKE '%core%'
    AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME, '') NOT ILIKE '%excluding food%'
  GROUP BY ts.VARIABLE
),
best AS (
  SELECT VARIABLE FROM candidates ORDER BY cnt DESC NULLS LAST LIMIT 1
)
SELECT
  ts."DATE" AS "DATE",
  ts.VALUE AS CPI_INDEX,
  COALESCE(
    NULLIF(TRIM(ts.VARIABLE_NAME), ''),
    NULLIF(TRIM(att.VARIABLE_NAME), ''),
    ts.VARIABLE
  ) AS VARIABLE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.financial_economic_indicators_attributes att
  ON ts.VARIABLE = att.VARIABLE
JOIN best b ON ts.VARIABLE = b.VARIABLE
WHERE ts."DATE" BETWEEN '2019-01-01' AND '2024-12-31'
ORDER BY ts."DATE"
""".strip()

SQL_FALLBACK_CPI_LOOSE_PICK_CYBERSYN = """
WITH candidates AS (
  SELECT ts.VARIABLE, COUNT(*) AS cnt
  FROM SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN.financial_economic_indicators_timeseries ts
  INNER JOIN SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN.financial_economic_indicators_attributes att
    ON ts.VARIABLE = att.VARIABLE
  WHERE ts."DATE" BETWEEN '2019-01-01' AND '2024-12-31'
    AND ts.VALUE BETWEEN 25 AND 900
    AND (TRIM(att.FREQUENCY) = 'Monthly' OR att.FREQUENCY ILIKE 'Month%')
    AND (
      att.MEASURE ILIKE '%consumer price%'
      OR att.MEASURE ILIKE '%CPI%'
      OR ts.VARIABLE_NAME ILIKE '%consumer price%'
      OR att.VARIABLE_NAME ILIKE '%consumer price%'
    )
    AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME, '') NOT ILIKE '%core%'
    AND COALESCE(ts.VARIABLE_NAME, att.VARIABLE_NAME, '') NOT ILIKE '%excluding food%'
  GROUP BY ts.VARIABLE
),
best AS (
  SELECT VARIABLE FROM candidates ORDER BY cnt DESC NULLS LAST LIMIT 1
)
SELECT
  ts."DATE" AS "DATE",
  ts.VALUE AS CPI_INDEX,
  COALESCE(
    NULLIF(TRIM(ts.VARIABLE_NAME), ''),
    NULLIF(TRIM(att.VARIABLE_NAME), ''),
    ts.VARIABLE
  ) AS VARIABLE_NAME
FROM SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN.financial_economic_indicators_timeseries ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN.financial_economic_indicators_attributes att
  ON ts.VARIABLE = att.VARIABLE
JOIN best b ON ts.VARIABLE = b.VARIABLE
WHERE ts."DATE" BETWEEN '2019-01-01' AND '2024-12-31'
ORDER BY ts."DATE"
""".strip()

SQL_FALLBACK_GDP_QUARTERLY_5Y = """
SELECT "DATE", GDP_VALUE, UNIT, VARIABLE_NAME, FREQUENCY
FROM HACKATHON.DATA.V_GDP
WHERE "DATE" >= DATEADD(year, -5, CURRENT_DATE())
ORDER BY "DATE"
""".strip()

SQL_FALLBACK_MACRO_UNEMPLOYMENT_CPI_2020 = """
SELECT OBSERVATION_DATE, UNEMPLOYMENT_RATE, CPI
FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
WHERE OBSERVATION_DATE >= '2020-01-01'
ORDER BY OBSERVATION_DATE
""".strip()

SQL_FALLBACK_MACRO_UNEMPLOYMENT_IP_2020 = """
SELECT OBSERVATION_DATE, UNEMPLOYMENT_RATE, INDUSTRIAL_PRODUCTION
FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
WHERE OBSERVATION_DATE >= '2020-01-01'
ORDER BY OBSERVATION_DATE
""".strip()

SQL_FALLBACK_MACRO_UNEMPLOYMENT_RETAIL_2020 = """
SELECT OBSERVATION_DATE, UNEMPLOYMENT_RATE, RETAIL_SALES
FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
WHERE OBSERVATION_DATE >= '2020-01-01'
ORDER BY OBSERVATION_DATE
""".strip()


def _normalize_question(q: str) -> str:
    t = q.lower().strip()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t


def _analyst_text_is_question_echo(text: str, question: str) -> bool:
    """True when Analyst returns only a restatement of the user question (no real insight)."""
    if not text or not question:
        return False
    t_raw = text.strip()
    q_raw = question.strip()
    tl = t_raw.lower()

    tail = re.sub(
        r"^(this is\s+)?(our\s+)?interpretation of your question\s*[\n:—\-]+\s*",
        "",
        tl,
        flags=re.I,
    ).strip()
    if tail == tl:
        tail = tl
        for marker in (
            "interpretation of your question",
            "our interpretation of your question",
            "our interpretation",
        ):
            if marker in tl:
                idx = tl.find(marker)
                after = tl[idx + len(marker) :].lstrip(" \t:-—\n")
                if after:
                    tail = after
                break

    tnorm = _normalize_question(tail)
    qnorm = _normalize_question(q_raw)
    if len(tnorm) < 8:
        return False
    if tnorm == qnorm:
        return True
    if tnorm in qnorm or qnorm in tnorm:
        return True
    tw, qw = set(tnorm.split()), set(qnorm.split())
    if tw and qw and len(tw | qw) > 0:
        overlap = len(tw & qw) / max(len(tw), len(qw))
        if overlap >= 0.82:
            return True
    return _normalize_question(t_raw) == qnorm


def _has_any(text: str, words: tuple[str, ...]) -> bool:
    return any(w in text for w in words)


def _wants_subsidiary_leaderboard(t: str) -> bool:
    if "subsidiar" not in t:
        return False
    return _has_any(t, ("most", "top", "largest", "many", "number", "count", "biggest"))


def _company_name_from_subsidiary_question(q: str) -> str | None:
    t = _normalize_question(q)
    m = re.search(r"subsidiar(?:y|ies)\s+does\s+(.+?)\s+own", t)
    if not m:
        m = re.search(r"what\s+subsidiar(?:y|ies)\s+(.+?)\s+own", t)
    if not m:
        return None
    raw = m.group(1).strip(" ?.,")
    if not raw:
        return None
    return raw.upper().replace("'", "''")


def _build_subsidiary_by_company_sql(company_upper: str) -> str:
    return (
        "SELECT RELATED_COMPANY_NAME AS subsidiary\n"
        "FROM HACKATHON.DATA.V_COMPANY_RELATIONSHIPS\n"
        f"WHERE COMPANY_NAME ILIKE '%{company_upper}%'\n"
        "ORDER BY RELATED_COMPANY_NAME"
    )


def _fallback_sql_for_question(q: str) -> str | None:
    t = _normalize_question(q)

    # Company graph intents
    if _wants_subsidiary_leaderboard(t):
        return SQL_FALLBACK_MOST_SUBSIDIARIES
    if ("more than 5" in t or "more than five" in t) and "subsidiar" in t:
        return SQL_FALLBACK_COMPANIES_GT5_SUBSIDIARIES
    dyn_company = _company_name_from_subsidiary_question(q)
    if dyn_company:
        return _build_subsidiary_by_company_sql(dyn_company)

    # Unemployment intents
    if "unemployment" in t and "2022" in t and _has_any(t, ("men", "male")) and _has_any(t, ("women", "female")):
        return SQL_FALLBACK_UNEMPLOYMENT_MEN_WOMEN_2022

    # Retail intents
    if "retail" in t and _has_any(t, ("top", "biggest", "largest")) and _has_any(t, ("category", "categories", "sector", "sectors")) and "2023" in t:
        return SQL_FALLBACK_TOP_RETAIL_2023
    if "retail" in t and _has_any(t, ("auto", "motor vehicle", "motor vehicles")) and "2019" in t and "2023" in t:
        return SQL_FALLBACK_AUTO_RETAIL_2019_2023
    if (
        "retail" in t
        and ("growth" in t or "compare" in t)
        and ("before" in t and "after" in t)
        and ("interest" in t or "rate" in t)
        and "2022" in t
    ):
        return SQL_FALLBACK_RETAIL_BEFORE_AFTER_HIKES_2022

    # Interest-rate intents
    if ("treasury" in t and "2020" in t) or ("treasury bill" in t and "since 2020" in t):
        return SQL_FALLBACK_TREASURY_SINCE_2020
    if _has_any(t, ("interest", "rates", "rate")) and "2022" in t and "2023" in t:
        return SQL_FALLBACK_INTEREST_2022_2023

    # Industrial-production intents
    if _has_any(t, ("industrial", "industry")) and _has_any(t, ("highest", "top", "biggest", "largest")) and "2023" in t:
        return SQL_FALLBACK_TOP_INDUSTRIAL_2023
    if _has_any(t, ("aerospace", "aircraft", "aviation")) and _has_any(t, ("industrial production", "production")):
        return SQL_FALLBACK_AEROSPACE_2019_2023

    # CPI / GDP (headline series views)
    if _has_any(t, ("cpi", "consumer price", "inflation")) and "gdp" not in t and "unemployment" not in t:
        if _has_any(
            t,
            (
                "monthly",
                "trend",
                "2019",
                "2020",
                "2021",
                "2022",
                "2023",
                "2024",
                "show",
                "index",
                "headline",
                "inflation",
            ),
        ):
            return SQL_FALLBACK_CPI_MONTHLY_2019_2024
    if "gdp" in t or "gross domestic product" in t:
        if _has_any(t, ("quarter", "last 5", "five year", "5 year", "trend", "show")):
            return SQL_FALLBACK_GDP_QUARTERLY_5Y

    # Macro wide — same timeline comparisons (retail before CPI so “unemployment + retail” is unambiguous)
    if (
        "unemployment" in t
        and "retail" in t
        and _has_any(t, ("2020", "since", "compare", "trend", "timeline", "over time", "sales", "total"))
    ):
        return SQL_FALLBACK_MACRO_UNEMPLOYMENT_RETAIL_2020
    if "unemployment" in t and _has_any(t, ("cpi", "consumer price", "inflation")) and _has_any(t, ("2020", "since", "compare", "same")):
        return SQL_FALLBACK_MACRO_UNEMPLOYMENT_CPI_2020
    if "unemployment" in t and _has_any(t, ("industrial production", "production")) and _has_any(t, ("2020", "since", "compare")):
        return SQL_FALLBACK_MACRO_UNEMPLOYMENT_IP_2020

    return None


def _is_cpi_headline_series_intent(q: str) -> bool:
    """True for single-series headline CPI questions (not unemployment+CPI compare on macro_wide)."""
    t = _normalize_question(q)
    if not _has_any(t, ("cpi", "consumer price", "inflation")) or "gdp" in t:
        return False
    if "unemployment" in t:
        return False
    return _has_any(
        t,
        (
            "monthly",
            "trend",
            "2019",
            "2020",
            "2021",
            "2022",
            "2023",
            "2024",
            "show",
            "index",
            "headline",
            "inflation",
        ),
    )


def _recover_cpi_from_marketplace(
    df: pd.DataFrame,
    sql: str | None,
    question: str,
) -> tuple[pd.DataFrame, str | None]:
    """If V_CPI path failed, try macro_wide, CPIAUCSL, then loose monthly CPI-like series pickers."""
    if not _is_cpi_headline_series_intent(question):
        return df, sql
    if df is not None and not df.empty and "Error" not in df.columns:
        return df, sql
    prior = (sql or SQL_FALLBACK_CPI_MONTHLY_2019_2024).strip()
    attempts: list[tuple[str, str]] = [
        (SQL_FALLBACK_CPI_FROM_MACRO_WIDE, "HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE.CPI"),
        (SQL_FALLBACK_CPI_CPIAUCSL_PUBLIC, "SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE (CPIAUCSL/CUSR0000SA0)"),
        (SQL_FALLBACK_CPI_CPIAUCSL_CYBERSYN, "SNOWFLAKE_PUBLIC_DATA_FREE.CYBERSYN (CPIAUCSL/CUSR0000SA0)"),
        (SQL_FALLBACK_CPI_LOOSE_PICK_PUBLIC, "PUBLIC_DATA_FREE (auto-pick monthly CPI-like series)"),
        (SQL_FALLBACK_CPI_LOOSE_PICK_PUBLIC_NO_BAND, "PUBLIC_DATA_FREE (auto-pick, no value band)"),
        (SQL_FALLBACK_CPI_LOOSE_PICK_CYBERSYN, "CYBERSYN (auto-pick monthly CPI-like series)"),
    ]
    for alt_sql, tag in attempts:
        alt_df = run_sql(alt_sql)
        if alt_df is not None and not alt_df.empty and "Error" not in alt_df.columns:
            note = (
                f"\n\n-- Note: V_CPI / prior CPI SQL returned no rows or errored. "
                f"Using fallback CPI source: {tag}."
            )
            return alt_df, prior + note
    return df, sql


SPELLING_MAP = {
    "subsidaries": "subsidiaries",
    "subsdiaries": "subsidiaries",
    "subsidiery": "subsidiary",
    "unemploment": "unemployment",
    "unemployement": "unemployment",
    "interst": "interest",
    "retial": "retail",
    "aeropsace": "aerospace",
    "industrail": "industrial",
    "treasurry": "treasury",
    "inflaton": "inflation",
    "consmer": "consumer",
}

DOMAIN_TERMS = [
    "unemployment",
    "retail",
    "sales",
    "interest",
    "rates",
    "treasury",
    "industrial",
    "production",
    "aerospace",
    "aircraft",
    "subsidiaries",
    "company",
    "kroger",
    "marriott",
    "inflation",
    "cpi",
    "gdp",
]


def classify_query(question: str) -> tuple[str, str]:
    """Return (code, short description) for UI badge."""
    t = _normalize_question(question)
    if not t:
        return "UNKNOWN", "Empty or unclear question"
    if _wants_subsidiary_leaderboard(t) or "subsidiar" in t:
        return "COMPANY_GRAPH", "Corporate parent → subsidiary relationships"
    if "compare" in t and _has_any(
        t, ("unemployment", "cpi", "inflation", "retail", "industrial", "production", "gdp")
    ):
        return "MULTI_METRIC", "Compare multiple indicators over time"
    if re.search(r"\b(top|highest|largest|biggest|rank)\b", t) or re.search(r"\bmost\b", t):
        return "RANKING", "Ranking or top-N by a measure"
    if re.search(r"\b(peak|maximum|max|minimum|min|trough)\b", t):
        return "EXTREMA", "Peak, trough, or extreme value"
    if _has_any(t, ("trend", "over time", "since", "timeline", "monthly", "quarter")):
        return "TIME_SERIES", "Trend or level over time"
    if _has_any(t, ("cpi", "inflation", "gdp", "consumer price")):
        return "PRICES_OUTPUT", "Prices, inflation, or GDP"
    if "rate" in t or "treasury" in t or "interest" in t:
        return "RATES", "Interest or Treasury rates"
    if "retail" in t:
        return "RETAIL", "Retail sales"
    return "ANALYTICAL", "General analytical question"


def _assistant_reply_when_no_narrative(
    question: str,
    query_class: str,
    df: pd.DataFrame,
) -> str:
    """Sensible chat line when Cortex COMPLETE and Analyst text are empty but rows returned."""
    if df is None or df.empty or "Error" in df.columns:
        t0 = _normalize_question(question)
        if _has_any(t0, ("cpi", "consumer price", "headline cpi", "inflation")) and "gdp" not in t0:
            return (
                "**No CPI rows** after **V_CPI**, **ECONOMIC_INDICATORS_WIDE**, **CPIAUCSL/CUSR0000SA0**, and **auto-picked** monthly CPI-like series. "
                "Confirm this role can read `SNOWFLAKE_PUBLIC_DATA_FREE` (or `CYBERSYN`) Finance & Economics tables, deploy "
                "`HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE` (`hackathon/sql/02_economic_indicators_wide.sql`), and run "
                "`hackathon/sql/discover_cpi_gdp_filters.sql` in Snowsight to align `V_CPI` with your listing."
            )
        return (
            "**No data** for that query (empty result or SQL error), so there is nothing to chart yet. "
            "Check **Technical → SQL** for the statement, or rephrase with a clearer metric and date range."
        )
    t = _normalize_question(question)
    cols_joined = " ".join(str(c).lower() for c in df.columns)

    _ts_date, _ts_val = _time_series_cols(df)
    if _has_any(t, ("cpi", "consumer price", "headline cpi", "inflation")) and (
        "cpi" in cols_joined
        or "cpi_index" in cols_joined
        or (
            _ts_date
            and _ts_val
            and (
                "cpi" in t
                or "headline cpi" in t
                or "consumer price" in t
                or "cpi index" in t
            )
        )
    ):
        return (
            "Plotted above: **monthly headline CPI** (all-items index, seasonally adjusted) for your range, "
            "from the semantic **cpi** logical table (V_CPI). Use **Data** for rows and **Technical → SQL** for the exact query."
        )

    if "gdp" in t and "gdp" in cols_joined:
        return (
            "**Real GDP** (quarterly, SAAR) for your question is in the chart and **Data**; "
            "the SQL panel references **gdp** / V_GDP."
        )

    if "unemployment" in t or "unemployment_rate" in cols_joined:
        return (
            "**Unemployment rate** for your question is in the chart and table; see **Technical** for SQL."
        )

    if query_class == "MULTI_METRIC" or (
        _has_any(t, ("compare", "versus", " vs ", "same timeline", "together"))
        and sum(
            1
            for k in (
                "unemployment",
                "cpi",
                "inflation",
                "retail",
                "gdp",
                "industrial",
                "fed",
                "treasury",
            )
            if k in t
        )
        >= 2
    ):
        return (
            "**Multiple indicators** share one timeline in the chart; use **Data** for values and **Technical** for the join SQL."
        )

    if query_class == "COMPANY_GRAPH" or _wants_subsidiary_leaderboard(t) or "subsidiar" in t:
        return (
            "Corporate **parent → subsidiary** results for your question are below; refine or export from here."
        )

    if "retail" in t and ("retail" in cols_joined or "sales" in cols_joined):
        return "**Retail sales** (USD) for your filters are in the chart and **Data** panel."

    if _has_any(t, ("treasury", "fed funds", "federal funds")) or (
        query_class == "RATES" and "rate" in t
    ):
        return (
            "Interest-rate / Treasury series for your question are shown above; confirm series names in **Technical → SQL**."
        )

    if _has_any(t, ("industrial", "production")):
        return "**Industrial production** for your period is in the chart and table."

    if query_class == "RANKING":
        return "Ranking / top-N results are in the table (and chart when applicable)."

    if query_class == "EXTREMA":
        return "Peak or trough values for your question are reflected in the results above."

    if query_class == "PRICES_OUTPUT":
        return "Price or output series for your question are visualized above; see **Data** and **Technical** for detail."

    if query_class == "TIME_SERIES":
        return "Your **time series** is charted above; row-level values are in **Data** and the query under **Technical**."

    return (
        "Results are in the **Data** panel and chart when the shape supports it; use **Technical → SQL** for the exact query."
    )


def ambiguity_warnings(question: str) -> list[str]:
    """Heuristic ambiguity flags for NL analytics."""
    t = _normalize_question(question)
    warns: list[str] = []
    if len(t.split()) < 4 and len(t) > 0:
        warns.append("Very short question — consider naming the metric and time range (e.g. “US unemployment 2020–2024”).")
    if "rate" in t or "rates" in t:
        if "treasury" not in t and "interest" not in t and "fed" not in t and "unemployment" not in t:
            warns.append(
                "“Rates” could mean Treasury yields, policy rates, or something else — specify if results look off."
            )
    if "sales" in t and "retail" not in t and "industrial" not in t:
        warns.append("“Sales” may mean retail, wholesale, or sector-specific series — add context if needed.")
    if t.count(" and ") >= 2 and "compare" not in t:
        warns.append("Multiple “and” clauses can be interpreted as one combined query — use **Compare …** for two metrics on one timeline.")
    if "inflation" in t and "cpi" not in t and "price" not in t:
        warns.append("Headline inflation is usually CPI in this model — say **CPI** if you want the verified CPI view.")
    return warns


def heuristic_followups(question: str, df: pd.DataFrame, qclass: str) -> list[str]:
    """Rule-based follow-ups when LLM suggestions are thin or fail."""
    if df is None or df.empty or "Error" in df.columns:
        return []
    t = _normalize_question(question)
    out: list[str] = []
    if qclass != "MULTI_METRIC" and "unemployment" in t and "cpi" not in t:
        out.append("Compare unemployment and CPI on the same monthly timeline since 2020.")
    if qclass != "MULTI_METRIC" and "cpi" in t and "unemployment" not in t:
        out.append("Compare unemployment and headline CPI since 2020 on one chart.")
    if "2023" in t and "2024" not in t and "trend" not in t:
        out.append("How does the same metric look if we extend the window through 2024?")
    if qclass == "RANKING":
        out.append("Show the same ranking for a different year or limit to top 5.")
    if qclass == "COMPANY_GRAPH" and "kroger" not in t and "marriott" not in t:
        out.append("What subsidiaries does Kroger own?")
    out.append("Show monthly headline CPI index from 2019 through 2024.")
    seen: set[str] = set()
    deduped: list[str] = []
    for s in out:
        k = s.lower()
        if k not in seen:
            seen.add(k)
            deduped.append(s)
    return deduped[:5]


def merge_followup_lists(primary: list[str], extra: list[str], limit: int = 5) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for s in primary + extra:
        k = s.strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        merged.append(s.strip())
        if len(merged) >= limit:
            break
    return merged


def _infer_chart_plan(df: pd.DataFrame, question: str) -> tuple[str, str]:
    """Choose chart type and a one-line reason (auto selection)."""
    if df.empty or "Error" in df.columns or len(df.columns) < 2:
        return "table", "Table view — not enough structure for a chart."
    cols = df.columns.tolist()
    date_col = next(
        (
            c
            for c in cols
            if "date" in c.lower()
            or c.lower() in ("month", "observation_date", "observation_month")
        ),
        None,
    )
    num_cols = _chart_numeric_columns(df)
    qn = _normalize_question(question)
    if date_col and num_cols:
        n = len(df.dropna(subset=[date_col]))
        if len(num_cols) == 1 and n <= 36:
            return "area", "Area chart — compact time series (emphasizes level)."
        return (
            "line",
            "Line chart — time series"
            + (" with multiple metrics" if len(num_cols) > 1 else "."),
        )
    cat_cols = [c for c in cols if c not in num_cols]
    if cat_cols and len(num_cols) == 1:
        try:
            nu = int(df[cat_cols[0]].nunique(dropna=True))
        except Exception:  # noqa: BLE001
            nu = 0
        if nu > 1:
            prefer_bar = _has_any(qn, ("top", "highest", "largest", "rank", "sector", "category", "company"))
            label = f"Bar chart — {nu} categories vs one measure."
            if prefer_bar:
                label += " (fits ranking-style questions.)"
            return "bar", label
    if len(num_cols) >= 2 and not date_col:
        return "scatter", "Scatter chart — relationship between two numeric columns."
    return "table", "Table view — safest for this result shape."


def _autocorrect_question(question: str) -> str:
    parts = question.split()
    out: list[str] = []
    for token in parts:
        bare = re.sub(r"[^A-Za-z]", "", token).lower()
        prefix = token[: len(token) - len(token.lstrip("([{\"'"))]
        suffix = token[len(token.rstrip(")]}\"'.,!?;:")) :]
        core = token[len(prefix) : len(token) - len(suffix) if len(suffix) > 0 else len(token)]

        corrected_core = core
        lower_core = re.sub(r"[^A-Za-z]", "", core).lower()
        if lower_core in SPELLING_MAP:
            corrected_core = SPELLING_MAP[lower_core]
        elif lower_core and len(lower_core) >= 5:
            match = difflib.get_close_matches(lower_core, DOMAIN_TERMS, n=1, cutoff=0.86)
            if match:
                corrected_core = match[0]

        if core.istitle():
            corrected_core = corrected_core.title()
        out.append(f"{prefix}{corrected_core}{suffix}")
    return " ".join(out)


def _result_digest(df: pd.DataFrame) -> str:
    if df is None or df.empty or "Error" in df.columns:
        return ""
    cols = df.columns.tolist()
    numeric_cols = list(df.select_dtypes(include="number").columns)
    bits = [f"Rows: {len(df)}", f"Columns: {len(cols)}"]
    for c in numeric_cols[:4]:
        try:
            bits.append(f"{c}: min {df[c].min():,.2f}, max {df[c].max():,.2f}")
        except Exception:  # noqa: BLE001
            pass
    head_preview = df.head(3).to_string(index=False)
    return " | ".join(bits) + "\n\nTop rows:\n" + head_preview


def generate_followups(question: str, df: pd.DataFrame) -> list[str]:
    if session is None or df is None or df.empty or "Error" in df.columns:
        return []
    preview = df.head(4).to_string(index=False)
    prompt = f"""Given this user question and result:
Question: {question}
Result sample:
{preview}

Suggest exactly 5 concise follow-up questions the user may ask next (diverse, specific to the data).
Return only JSON array of strings."""
    try:
        raw = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE('"
            + CORTEX_COMPLETE_MODEL
            + "', $$"
            + prompt
            + "$$) AS followups"
        ).collect()[0]["FOLLOWUPS"]
        if not raw:
            return []
        parsed = json.loads(str(raw).strip())
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()][:5]
    except Exception:  # noqa: BLE001
        return []
    return []


LOADING_HINTS: tuple[str, ...] = (
    "Headline CPI uses V_CPI; GDP on ECONOMIC_INDICATORS_WIDE uses the classic BEA marketplace match (GDP fallback SQL reads V_GDP).",
    "The semantic model has eight logical tables: granular series plus macro_wide for multi-indicator joins.",
    "Verified SQL fallbacks kick in if Analyst SQL fails — check the transparency panel.",
    "Quarterly GDP rows only populate GDP on quarter-end dates in the wide panel.",
    "Try the Multi-metric tab for unemployment vs CPI or industrial production on one chart.",
)


def _loading_banner_html(step: str, hint: str) -> str:
    safe_step = html.escape(step)
    safe_hint = html.escape(hint)
    return f"""
<div class="analyst-loading-wrap">
  <div class="analyst-loading-orbs" aria-hidden="true">
    <span class="analyst-orb analyst-orb-1"></span>
    <span class="analyst-orb analyst-orb-2"></span>
    <span class="analyst-orb analyst-orb-3"></span>
  </div>
  <div class="analyst-loading-card">
    <div class="analyst-loading-kicker">Working on your question</div>
    <div class="analyst-loading-title">{safe_step}</div>
    <div class="analyst-dots" aria-hidden="true"><span></span><span></span><span></span></div>
    <div class="analyst-hint">{safe_hint}</div>
  </div>
</div>
"""


def _loading_hint_for_turn() -> str:
    n = len(st.session_state.get("messages", []))
    return LOADING_HINTS[n % len(LOADING_HINTS)]


def _time_greeting() -> str:
    h = datetime.now().hour
    if h < 12:
        return "Good morning."
    if h < 17:
        return "Good afternoon."
    return "Good evening."


def _render_welcome_hero() -> None:
    if st.session_state.messages or st.session_state.get("welcome_hero_dismissed"):
        return
    g = html.escape(_time_greeting())
    st.markdown(
        f"""
<div class="welcome-hero-shell">
  <p class="welcome-hero-greeting">{g} Nice to see you.</p>
  <p class="welcome-hero-title">Welcome to your Economic Intelligence workspace.</p>
  <p class="welcome-hero-body">
    Ask in plain English — unemployment, CPI, GDP, retail, rates, industrial production,
    or how metrics line up on one timeline. Cortex Analyst turns it into SQL; you always get transparency and a chart when it fits.
  </p>
  <div class="welcome-hero-chips">
    <span class="welcome-chip">8 semantic domains</span>
    <span class="welcome-chip">Macro wide joins</span>
    <span class="welcome-chip">Verified fallbacks</span>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )
    if st.button("Skip intro — show metrics", key="dismiss_welcome_hero"):
        st.session_state.welcome_hero_dismissed = True
        st.rerun()


def _render_welcome_back_strip() -> None:
    if not st.session_state.messages:
        return
    st.markdown(
        """
<div class="welcome-back-strip">
  <strong>Welcome back.</strong> Latest results are in the <strong>Analysis workspace</strong> (left) — ask again from the panel on the right.
</div>
""",
        unsafe_allow_html=True,
    )


# ── page ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="US Economic Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """


<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
/* Dark mode — deep slate + Snowflake sky (#29B5E8) / ice accents */
:root {
    --dm-bg: #070a0e;
    --dm-bg-mid: #0c1018;
    --dm-surface: #121a24;
    --dm-surface2: #182230;
    --dm-surface3: #1e2a3d;
    --dm-border: #2a3548;
    --dm-border2: #36445c;
    --dm-text: #e8f0fa;
    --dm-muted: #94a8bc;
    --dm-muted2: #6b7f95;
    --dm-sky: #29B5E8;
    --dm-sky-bright: #5dd4ff;
    --dm-sky-dim: #1e8eb8;
    --dm-ice: #a5d8ff;
    --dm-glow: rgba(41, 181, 232, 0.22);
    --dm-shadow: 0 4px 24px rgba(0, 0, 0, 0.45);
    --dm-shadow-inset: inset 0 1px 0 rgba(255,255,255,0.04);
}
html, body, [class*="css"] {
    font-family: "Inter", "Segoe UI", ui-sans-serif, system-ui, sans-serif !important;
    color-scheme: dark;
}
.stApp {
    background: var(--dm-bg) !important;
    background-image:
        radial-gradient(ellipse 120% 80% at 50% -30%, rgba(41, 181, 232, 0.14), transparent 55%),
        radial-gradient(ellipse 60% 40% at 100% 20%, rgba(30, 80, 120, 0.2), transparent 50%),
        linear-gradient(180deg, var(--dm-bg-mid) 0%, var(--dm-bg) 55%) !important;
    background-attachment: fixed !important;
    color: var(--dm-text) !important;
}
[data-testid="stAppViewContainer"], [data-testid="stHeader"], section.main {
    background-color: transparent !important;
}
section.main > div.block-container {
    max-width: min(1480px, 100%) !important;
    padding: 0.5rem 1rem 1rem 1rem !important;
}
/* Sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, var(--dm-surface) 0%, var(--dm-bg-mid) 100%) !important;
    border-right: 1px solid var(--dm-border) !important;
}
[data-testid="stSidebar"] [data-testid="stMarkdown"] p,
[data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] label {
    color: var(--dm-muted) !important;
}
[data-testid="stSidebar"] h3, [data-testid="stSidebar"] h2 {
    color: var(--dm-text) !important;
}
/* Header bar */
[data-testid="stHeader"] {
    background: rgba(7, 10, 14, 0.85) !important;
    backdrop-filter: blur(12px) !important;
    border-bottom: 1px solid var(--dm-border) !important;
}
/* Main chrome */
.ei-app-header {
    margin-bottom: 1.25rem;
    padding: 1.35rem 1.5rem 1.25rem 1.5rem;
    border-radius: 14px;
    background: linear-gradient(145deg, var(--dm-surface2) 0%, var(--dm-surface) 100%);
    border: 1px solid var(--dm-border);
    box-shadow: var(--dm-shadow), var(--dm-shadow-inset);
    position: relative;
    overflow: hidden;
}
.ei-app-header::before {
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--dm-sky), var(--dm-sky-bright), transparent);
    opacity: 0.95;
}
.ei-app-header-inner { position: relative; z-index: 1; }
.sf-marketing-strip {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--dm-sky);
    margin-bottom: 6px;
}
.main-header {
    font-size: 1.65rem;
    font-weight: 800;
    margin-bottom: 6px;
    letter-spacing: -0.03em;
    color: var(--dm-text);
    line-height: 1.2;
}
.sub-header {
    font-size: 0.95rem;
    color: var(--dm-muted);
    line-height: 1.55;
    max-width: 52rem;
    font-weight: 400;
}
.sub-header strong {
    color: var(--dm-ice);
    font-weight: 600;
}
/* Glass / bordered panels */
div[data-testid="stVerticalBlockBorderWrapper"] {
    background: var(--dm-surface) !important;
    border-radius: 12px !important;
    border: 1px solid var(--dm-border) !important;
    box-shadow: var(--dm-shadow), var(--dm-shadow-inset) !important;
    margin-bottom: 0.5rem !important;
    padding: 8px 10px 10px 10px !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
}
div[data-testid="stVerticalBlockBorderWrapper"]:hover {
    border-color: var(--dm-border2) !important;
    box-shadow: 0 8px 32px rgba(0,0,0,0.5), 0 0 0 1px var(--dm-glow) !important;
}
/* Metrics — compact so four-up row shows full text (no ellipsis) */
div[data-testid="stMetric"] {
    background: var(--dm-surface2) !important;
    border-radius: 10px !important;
    border: 1px solid var(--dm-border) !important;
    padding: 8px 10px 10px 10px !important;
    box-shadow: var(--dm-shadow-inset) !important;
    transition: transform 0.2s ease, border-color 0.2s ease !important;
    min-width: 0 !important;
}
div[data-testid="stMetric"]:hover {
    transform: translateY(-2px);
    border-color: rgba(41, 181, 232, 0.35) !important;
}
div[data-testid="stMetric"] label {
    color: var(--dm-muted2) !important;
    font-weight: 600 !important;
    font-size: 9px !important;
    letter-spacing: 0.05em !important;
    text-transform: uppercase !important;
    line-height: 1.25 !important;
    white-space: normal !important;
    word-break: break-word !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"],
div[data-testid="stMetric"] [data-testid="stMetricValue"] p,
div[data-testid="stMetric"] [data-testid="stMarkdownContainer"] p {
    color: var(--dm-sky-bright) !important;
    font-weight: 600 !important;
    font-size: 0.75rem !important;
    line-height: 1.35 !important;
    white-space: normal !important;
    word-break: break-word !important;
    overflow-wrap: anywhere !important;
    text-overflow: unset !important;
    max-width: 100% !important;
}
div[data-testid="column"] > div[data-testid="stVerticalBlock"] > div[data-testid="stMetric"] {
    max-width: 100% !important;
}
/* Buttons */
button[kind="primary"] {
    background: linear-gradient(180deg, var(--dm-sky) 0%, var(--dm-sky-dim) 100%) !important;
    color: #061018 !important;
    border: none !important;
    font-weight: 700 !important;
    border-radius: 10px !important;
    box-shadow: 0 0 20px var(--dm-glow) !important;
    transition: transform 0.15s ease, filter 0.2s ease !important;
}
button[kind="primary"]:hover {
    transform: translateY(-1px);
    filter: brightness(1.08);
}
div[data-testid="stButton"] button:not([kind="primary"]) {
    border-radius: 10px !important;
    border: 1px solid var(--dm-border2) !important;
    background: var(--dm-surface2) !important;
    color: var(--dm-text) !important;
    font-weight: 500 !important;
    transition: border-color 0.2s ease, background 0.2s ease !important;
}
div[data-testid="stButton"] button:not([kind="primary"]):hover {
    border-color: var(--dm-sky-dim) !important;
    background: var(--dm-surface3) !important;
}
/* Tabs */
div[data-testid="stTabs"] [data-baseweb="tab-list"] {
    background: var(--dm-surface2) !important;
    border-radius: 10px 10px 0 0 !important;
    gap: 4px !important;
    padding: 4px !important;
    border: 1px solid var(--dm-border) !important;
    border-bottom: none !important;
}
div[data-testid="stTabs"] [role="tablist"] button {
    color: var(--dm-muted) !important;
    font-weight: 500 !important;
    font-size: 13px !important;
    border-radius: 8px !important;
}
div[data-testid="stTabs"] [role="tablist"] button[aria-selected="true"] {
    color: var(--dm-text) !important;
    font-weight: 600 !important;
    background: var(--dm-surface3) !important;
}
/* Inputs */
.stTextInput input, .stTextArea textarea {
    border-radius: 10px !important;
    border: 1px solid var(--dm-border2) !important;
    background: var(--dm-bg-mid) !important;
    color: var(--dm-text) !important;
    caret-color: var(--dm-sky) !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: var(--dm-sky) !important;
    box-shadow: 0 0 0 2px var(--dm-glow) !important;
}
/* Main ask form — aligned row, no broken button label, single input chrome */
form[data-testid="stForm"] {
    border: 1px solid rgba(167, 139, 250, 0.28) !important;
    border-radius: 16px !important;
    padding: 18px 18px 18px 18px !important;
    margin: 0 0 14px 0 !important;
    background: linear-gradient(165deg, rgba(30, 27, 75, 0.28), rgba(15, 23, 42, 0.45)) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04) !important;
}
form[data-testid="stForm"] > div[data-testid="stVerticalBlockBorderWrapper"] {
    padding-bottom: 0 !important;
    gap: 0 !important;
}
form[data-testid="stForm"] div[data-testid="stHorizontalBlock"] {
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: nowrap !important;
    align-items: stretch !important;
    gap: 12px !important;
    width: 100% !important;
}
form[data-testid="stForm"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
    display: flex !important;
    flex-direction: column !important;
    justify-content: center !important;
    padding-top: 0 !important;
    padding-bottom: 0 !important;
}
form[data-testid="stForm"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {
    flex: 0 0 auto !important;
    min-width: 11.5rem !important;
    max-width: 15rem !important;
}
form[data-testid="stForm"] div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:first-child {
    flex: 1 1 auto !important;
    min-width: 0 !important;
}
form[data-testid="stForm"] div[data-testid="stTextInput"] {
    margin-bottom: 0 !important;
}
form[data-testid="stForm"] div[data-testid="stTextInput"] [data-baseweb="input"] {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
}
form[data-testid="stForm"] div[data-testid="stTextInput"] input {
    min-height: 68px !important;
    font-size: 19px !important;
    line-height: 1.4 !important;
    border: 1px solid var(--dm-border2) !important;
    border-radius: 11px !important;
    background: var(--dm-bg-mid) !important;
    box-shadow: none !important;
}
form[data-testid="stForm"] div[data-testid="stTextInput"] input:focus {
    border-color: var(--dm-sky) !important;
    box-shadow: 0 0 0 2px var(--dm-glow) !important;
}
form[data-testid="stForm"] [data-testid="stFormSubmitButton"] button,
form[data-testid="stForm"] button[kind="primary"] {
    white-space: nowrap !important;
    width: 100% !important;
    min-height: 68px !important;
    height: 100% !important;
    border-radius: 12px !important;
    font-weight: 700 !important;
    font-size: 1.12rem !important;
    letter-spacing: 0.03em !important;
    padding: 0 1.25rem !important;
    line-height: 1.2 !important;
}
/* Radio */
div[data-testid="stRadio"] label {
    color: var(--dm-muted) !important;
}
div[data-testid="stRadio"] [role="radiogroup"] label[data-baseweb="radio"] {
    color: var(--dm-text) !important;
}
/* Markdown in main */
.main .stMarkdown, section.main p, section.main li, section.main span {
    color: inherit;
}
hr {
    margin: 0.5rem 0 !important;
    border: none !important;
    height: 1px !important;
    background: linear-gradient(90deg, transparent, var(--dm-border), transparent) !important;
}
label[data-testid="stWidgetLabel"] {
    color: var(--dm-muted2) !important;
}
/* Alerts */
div[data-testid="stAlert"] {
    background: rgba(234, 179, 8, 0.08) !important;
    border: 1px solid rgba(234, 179, 8, 0.35) !important;
    border-radius: 10px !important;
}
div[data-testid="stAlert"] p, div[data-testid="stAlert"] div {
    color: #fde68a !important;
}
/* Expanders */
.streamlit-expanderHeader {
    color: var(--dm-sky) !important;
}
[data-testid="stExpander"] details {
    background: var(--dm-surface2) !important;
    border: 1px solid var(--dm-border) !important;
    border-radius: 10px !important;
}
/* Dataframe / tables */
div[data-testid="stDataFrame"] {
    border-radius: 10px !important;
    border: 1px solid var(--dm-border) !important;
    overflow: hidden !important;
}
/* Narrative card */
.ei-narrative-card {
    border-radius: 12px;
    padding: 16px 18px;
    margin-bottom: 14px;
    background: var(--dm-surface2);
    border: 1px solid var(--dm-border);
    position: relative;
    overflow: hidden;
    animation: ei-card-in 0.45s cubic-bezier(0.22, 1, 0.36, 1) both;
}
.ei-narrative-card::before {
    content: "";
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 3px;
    background: linear-gradient(180deg, var(--dm-sky-dim), var(--dm-sky));
    border-radius: 3px 0 0 3px;
}
.ei-narrative-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 10px;
    padding-left: 10px;
}
.ei-narrative-kicker {
    font-size: 13px;
    font-weight: 600;
    color: var(--dm-text);
}
.ei-narrative-badge {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: #061018;
    background: var(--dm-sky);
    padding: 4px 10px;
    border-radius: 999px;
}
.ei-narrative-body {
    font-size: 15px;
    line-height: 1.65;
    color: var(--dm-muted);
    padding-left: 10px;
}
.ei-narrative-card--compact {
    padding: 8px 10px !important;
    margin-bottom: 6px !important;
}
.ei-narrative-card--compact .ei-narrative-head {
    margin-bottom: 4px !important;
    padding-left: 6px !important;
}
.ei-narrative-card--compact .ei-narrative-kicker {
    font-size: 10px !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dm-sky) !important;
}
.ei-narrative-card--compact .ei-narrative-badge {
    font-size: 9px !important;
    padding: 2px 8px !important;
}
.ei-narrative-card--compact .ei-narrative-body {
    font-size: 12px !important;
    line-height: 1.5 !important;
    padding-left: 6px !important;
}
.section-label--tight {
    font-size: 10px !important;
    margin-bottom: 4px !important;
    margin-top: 2px !important;
}
@keyframes ei-card-in {
    from { opacity: 0; transform: translateY(8px); }
    to { opacity: 1; transform: translateY(0); }
}
/* Chat bubbles */
.user-bubble {
    background: linear-gradient(135deg, #1a4a66 0%, var(--dm-sky-dim) 100%);
    color: #f0f9ff;
    padding: 12px 16px;
    border-radius: 16px 16px 4px 16px;
    margin: 10px 0;
    max-width: 92%;
    float: right;
    clear: both;
    font-size: 14px;
    line-height: 1.5;
    border: 1px solid rgba(41, 181, 232, 0.35);
    box-shadow: 0 4px 20px rgba(41, 181, 232, 0.12);
}
.ai-bubble {
    background: var(--dm-surface2);
    color: var(--dm-text);
    padding: 12px 16px;
    border-radius: 16px 16px 16px 4px;
    margin: 10px 0;
    max-width: 95%;
    float: left;
    clear: both;
    font-size: 14px;
    line-height: 1.55;
    border: 1px solid var(--dm-border);
}
.clearfix { clear: both; }
/* SQL */
.sql-box {
    background: linear-gradient(165deg, #05080c 0%, #0d1520 50%, #0a121c 100%);
    color: #7dd3fc;
    padding: 14px 16px;
    border-radius: 10px;
    font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, monospace;
    font-size: 12px;
    line-height: 1.45;
    white-space: pre-wrap;
    overflow-x: auto;
    margin-top: 10px;
    border: 1px solid rgba(41, 181, 232, 0.25);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
}
.section-label {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--dm-sky);
    margin-bottom: 10px;
}
.section-label-lg {
    font-size: 13px;
    font-weight: 700;
    color: var(--dm-text);
    letter-spacing: -0.02em;
    margin-bottom: 12px;
    text-transform: none;
}
/* Loading */
@keyframes analyst-float-y {
    0%, 100% { transform: translateY(0); }
    50% { transform: translateY(-8px); }
}
@keyframes analyst-orbit {
    0% { transform: rotate(0deg) translateX(28px) rotate(0deg); }
    100% { transform: rotate(360deg) translateX(28px) rotate(-360deg); }
}
@keyframes analyst-pulse-ring {
    0%, 100% { box-shadow: 0 0 0 0 rgba(41, 181, 232, 0.35); }
    50% { box-shadow: 0 0 0 12px rgba(41, 181, 232, 0); }
}
@keyframes analyst-dot-bounce {
    0%, 80%, 100% { transform: translateY(0); opacity: 0.35; }
    40% { transform: translateY(-6px); opacity: 1; }
}
@keyframes analyst-shimmer {
    0% { background-position: -200% 0; }
    100% { background-position: 200% 0; }
}
.analyst-loading-wrap {
    position: relative;
    margin: 8px 0 20px 0;
    min-height: 120px;
    display: flex;
    align-items: center;
    justify-content: center;
}
.analyst-loading-orbs {
    position: absolute;
    width: 120px;
    height: 120px;
    left: 50%;
    top: 50%;
    margin-left: -60px;
    margin-top: -60px;
    pointer-events: none;
}
.analyst-orb {
    position: absolute;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    left: 50%;
    top: 50%;
    margin: -7px 0 0 -7px;
    animation: analyst-orbit 4.5s linear infinite;
}
.analyst-orb-1 { background: linear-gradient(135deg, var(--dm-sky-dim), var(--dm-sky)); animation-duration: 3.8s; }
.analyst-orb-2 { background: linear-gradient(135deg, var(--dm-sky), var(--dm-sky-bright)); animation-duration: 5.2s; animation-direction: reverse; }
.analyst-orb-3 { background: linear-gradient(135deg, #1e3a5f, var(--dm-sky-dim)); animation-duration: 4.2s; }
.analyst-loading-card {
    position: relative;
    z-index: 1;
    text-align: center;
    padding: 20px 28px 22px 28px;
    border-radius: 14px;
    background: var(--dm-surface2);
    border: 1px solid var(--dm-border);
    box-shadow: var(--dm-shadow);
    animation: analyst-float-y 3.2s ease-in-out infinite, analyst-pulse-ring 2.4s ease-out infinite;
    max-width: 520px;
    margin: 0 auto;
}
.analyst-loading-kicker {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--dm-sky);
    margin-bottom: 8px;
}
.analyst-loading-title {
    font-size: 17px;
    font-weight: 600;
    color: var(--dm-text);
    margin-bottom: 12px;
    line-height: 1.35;
}
.analyst-dots {
    display: flex;
    gap: 8px;
    justify-content: center;
    margin-bottom: 14px;
}
.analyst-dots span {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: linear-gradient(180deg, var(--dm-sky), var(--dm-sky-bright));
    animation: analyst-dot-bounce 1.05s ease-in-out infinite;
}
.analyst-dots span:nth-child(2) { animation-delay: 0.15s; }
.analyst-dots span:nth-child(3) { animation-delay: 0.3s; }
.analyst-hint {
    font-size: 13px;
    color: var(--dm-muted);
    line-height: 1.45;
    padding-top: 8px;
    border-top: 1px solid var(--dm-border);
    background: linear-gradient(90deg, transparent, rgba(41,181,232,0.06) 20%, rgba(41,181,232,0.06) 80%, transparent);
    background-size: 200% 100%;
    animation: analyst-shimmer 4s ease-in-out infinite;
}
/* Welcome */
@keyframes welcome-hero-in {
    from { opacity: 0; transform: translateY(14px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes welcome-line-in {
    from { opacity: 0; transform: translateX(-6px); }
    to { opacity: 1; transform: translateX(0); }
}
.welcome-hero-shell {
    border-radius: 12px;
    padding: 12px 16px 12px 16px;
    margin-bottom: 8px;
    background: linear-gradient(135deg, var(--dm-surface2) 0%, var(--dm-surface) 100%);
    border: 1px solid var(--dm-border);
    box-shadow: var(--dm-shadow);
    animation: welcome-hero-in 0.65s ease-out both;
}
.welcome-hero-greeting {
    font-size: 14px;
    font-weight: 600;
    color: var(--dm-sky);
    margin: 0 0 6px 0;
    animation: welcome-line-in 0.5s ease-out 0.08s both;
}
.welcome-hero-title {
    font-size: 16px;
    font-weight: 700;
    line-height: 1.25;
    margin: 0 0 6px 0;
    color: var(--dm-text);
    animation: welcome-line-in 0.55s ease-out 0.18s both;
}
.welcome-hero-body {
    font-size: 13px;
    color: var(--dm-muted);
    line-height: 1.45;
    margin: 0 0 14px 0;
    max-width: 640px;
    animation: welcome-line-in 0.55s ease-out 0.28s both;
}
.welcome-hero-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    animation: welcome-line-in 0.5s ease-out 0.38s both;
}
.welcome-chip {
    font-size: 12px;
    font-weight: 600;
    padding: 6px 12px;
    border-radius: 999px;
    background: var(--dm-surface3);
    border: 1px solid var(--dm-border);
    color: var(--dm-sky-bright);
}
.welcome-chip:hover {
    border-color: var(--dm-sky-dim);
}
@keyframes welcome-back-in {
    from { opacity: 0; transform: translateY(-6px); }
    to { opacity: 1; transform: translateY(0); }
}
.welcome-back-strip {
    animation: welcome-back-in 0.45s ease-out both;
    border-radius: 10px;
    padding: 8px 12px;
    margin-bottom: 6px;
    background: var(--dm-surface2);
    border: 1px solid var(--dm-border);
    font-size: 12px;
    color: var(--dm-muted);
    font-weight: 500;
    line-height: 1.35;
}
.welcome-back-strip strong { color: var(--dm-text); }
/* Charts */
@keyframes chart-reveal-pop {
    0% { opacity: 0; transform: translateY(10px) scale(0.99); }
    100% { opacity: 1; transform: translateY(0) scale(1); }
}
div[data-testid="stVegaLiteChart"],
div[data-testid="stArrowVegaLiteChart"],
div[data-testid*="VegaLiteChart"],
div[data-testid="stPlotlyChart"] {
    animation: chart-reveal-pop 0.55s cubic-bezier(0.22, 1, 0.36, 1) both !important;
    border-radius: 10px !important;
    border: 1px solid var(--dm-border) !important;
    background: var(--dm-surface2) !important;
    padding: 8px !important;
}
div[data-testid="stDataFrame"] {
    animation: chart-reveal-pop 0.45s ease-out both !important;
}
/* Persona */
.persona-hints {
    margin: 8px 0 4px 0;
    padding: 12px 14px;
    border-radius: 10px;
    background: var(--dm-bg-mid);
    border: 1px solid var(--dm-border);
    font-size: 13px;
    line-height: 1.5;
    color: var(--dm-muted);
}
/* Stacked layout: label on its own row so narrow sidebars don’t crush description text */
.persona-hint-row {
    display: block;
    margin: 10px 0 0 0;
    padding: 10px 12px;
    border-radius: 8px;
    border: 1px solid transparent;
}
.persona-hint-row:first-child {
    margin-top: 0;
}
.persona-hint-row:hover {
    background: var(--dm-surface2);
}
.persona-hint-active {
    background: rgba(41, 181, 232, 0.1) !important;
    border: 1px solid rgba(41, 181, 232, 0.35) !important;
}
.persona-hint-name {
    display: block;
    font-weight: 700;
    color: var(--dm-sky);
    font-size: 10px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin-bottom: 6px;
    line-height: 1.3;
}
.persona-hint-text {
    display: block;
    width: 100%;
    font-size: 13px;
    line-height: 1.55;
    overflow-wrap: break-word;
    word-wrap: break-word;
}
/* Executive BI Copilot chrome (violet + glass) */
.ei-topnav {
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 8px 12px;
    padding: 8px 14px;
    margin: 0 0 8px 0;
    background: linear-gradient(105deg, rgba(88, 28, 135, 0.22), rgba(30, 27, 75, 0.45));
    border: 1px solid rgba(167, 139, 250, 0.28);
    border-radius: 14px;
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.2) inset, 0 8px 32px rgba(0, 0, 0, 0.25);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
}
.ei-topnav-brand {
    display: flex;
    align-items: center;
    gap: 12px;
    font-weight: 800;
    font-size: 15px;
    letter-spacing: -0.02em;
    color: var(--dm-ice);
}
.ei-logo-rings {
    width: 28px;
    height: 28px;
    border-radius: 50%;
    background: radial-gradient(circle at 30% 30%, #a78bfa, #5b21b6 55%, #1e1b4b);
    box-shadow: 0 0 20px rgba(167, 139, 250, 0.45);
    position: relative;
}
.ei-logo-rings::after {
    content: "";
    position: absolute;
    inset: 5px;
    border-radius: 50%;
    border: 2px solid rgba(41, 181, 232, 0.65);
    opacity: 0.9;
}
.ei-topnav-links {
    display: flex;
    gap: 22px;
    font-size: 13px;
    font-weight: 600;
    color: var(--dm-muted);
}
.ei-topnav-link-active {
    color: var(--dm-sky);
    border-bottom: 2px solid var(--dm-sky);
    padding-bottom: 2px;
}
.ei-topnav-meta {
    display: flex;
    align-items: center;
    gap: 14px;
    font-size: 12px;
    color: var(--dm-muted);
}
.ei-topnav-time {
    font-variant-numeric: tabular-nums;
    color: var(--dm-ice);
    font-weight: 600;
}
.ei-hero-copilot {
    padding: 12px 16px 12px;
    margin: 0 0 8px 0;
    border-radius: 14px;
    background: linear-gradient(135deg, rgba(30, 27, 75, 0.55), rgba(15, 23, 42, 0.75));
    border: 1px solid rgba(167, 139, 250, 0.22);
    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.35);
    position: relative;
    overflow: hidden;
}
.ei-hero-copilot::before {
    content: "";
    position: absolute;
    top: -40%;
    right: -10%;
    width: 55%;
    height: 140%;
    background: radial-gradient(ellipse, rgba(41, 181, 232, 0.12), transparent 70%);
    pointer-events: none;
}
.ei-hero-title {
    font-size: clamp(17px, 2.4vw, 22px);
    font-weight: 800;
    letter-spacing: -0.03em;
    color: var(--dm-ice);
    margin: 0 0 4px 0;
    line-height: 1.2;
}
.ei-hero-sub {
    font-size: 13px;
    color: var(--dm-muted);
    margin: 0;
    line-height: 1.4;
}
.ei-hero-badge {
    position: absolute;
    top: 14px;
    right: 16px;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #061018;
    background: linear-gradient(90deg, #a78bfa, #29b5e8);
    padding: 5px 10px;
    border-radius: 8px;
}
/* Right insight rail — single compact identity block */
.ei-rail-identity {
    padding: 8px 10px 10px;
    border-radius: 10px;
    background: rgba(30, 27, 75, 0.35);
    border: 1px solid rgba(167, 139, 250, 0.22);
    margin-bottom: 8px;
}
.ei-rail-identity-top {
    font-size: 9px;
    font-weight: 800;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #a78bfa;
    margin: 0 0 2px 0;
}
.ei-rail-identity-name {
    font-size: 12px;
    font-weight: 700;
    color: var(--dm-ice);
    line-height: 1.25;
    margin: 0;
}
.ei-rail-query-inline {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-start;
    gap: 6px 8px;
    margin-top: 8px;
    padding-top: 8px;
    border-top: 1px solid rgba(167, 139, 250, 0.14);
    font-size: 11px;
    color: var(--dm-muted);
    line-height: 1.35;
}
.ei-ai-card {
    padding: 14px 16px;
    border-radius: 12px;
    background: rgba(30, 27, 75, 0.35);
    border: 1px solid rgba(167, 139, 250, 0.25);
    margin-bottom: 12px;
}
.ei-ai-card-title {
    font-size: 11px;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #a78bfa;
    margin: 0 0 4px 0;
}
.ei-ai-card-name {
    font-size: 15px;
    font-weight: 700;
    color: var(--dm-ice);
    margin: 0;
}
/* Empty state */
.ei-empty-state {
    text-align: center;
    padding: 3rem 1.5rem;
    color: var(--dm-muted);
    background: var(--dm-bg-mid);
    border-radius: 12px;
    border: 1px dashed var(--dm-border2);
    animation: ei-empty-in 0.5s cubic-bezier(0.22, 1, 0.36, 1) both;
}
.ei-empty-state .ei-empty-icon {
    font-size: 44px;
    margin-bottom: 12px;
    opacity: 0.9;
}
.ei-empty-state .ei-empty-title {
    font-size: 17px;
    font-weight: 700;
    margin-bottom: 8px;
    color: var(--dm-text);
}
.ei-empty-state .ei-empty-copy {
    font-size: 14px;
    line-height: 1.6;
    max-width: 380px;
    margin: 0 auto;
    color: var(--dm-muted);
}
.ei-empty-state .ei-empty-copy strong {
    color: var(--dm-ice);
}
.ei-empty-state--compact {
    padding: 1.1rem 0.85rem !important;
}
.ei-empty-state--compact .ei-empty-icon {
    font-size: 32px !important;
    margin-bottom: 6px !important;
}
.ei-empty-state--compact .ei-empty-title {
    font-size: 14px !important;
    margin-bottom: 4px !important;
}
.ei-empty-state--compact .ei-empty-copy {
    font-size: 12px !important;
    line-height: 1.45 !important;
}
.ei-ask-head {
    font-size: clamp(20px, 2.2vw, 26px);
    font-weight: 800;
    color: var(--dm-ice);
    margin: 0 0 6px 0;
    letter-spacing: -0.03em;
    line-height: 1.2;
}
.ei-ask-sub {
    font-size: 15px;
    font-weight: 500;
    color: var(--dm-muted);
    margin: 0 0 16px 0;
    line-height: 1.5;
    max-width: 52rem;
}
/* Dashboard conversation — always visible, large */
.ei-thread-anchor {
    scroll-margin-top: 88px;
}
.ei-results-anchor {
    scroll-margin-top: 88px;
}
.ei-thread-shell {
    border: 1px solid rgba(167, 139, 250, 0.22);
    border-radius: 16px;
    background: linear-gradient(180deg, rgba(30, 27, 75, 0.2), rgba(15, 23, 42, 0.35));
    padding: 14px 16px 14px 16px;
    margin: 0 0 10px 0;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
}
.ei-thread-head {
    font-size: clamp(17px, 1.6vw, 20px);
    font-weight: 800;
    color: var(--dm-ice);
    letter-spacing: -0.02em;
    margin: 0 0 4px 0;
}
.ei-thread-sub {
    font-size: 13px;
    color: var(--dm-muted);
    margin: 0 0 12px 0;
    line-height: 1.45;
}
.ei-bubble-enter {
    animation: ei-card-in 0.55s cubic-bezier(0.22, 1, 0.36, 1) both;
}
.ei-flash-once {
    animation: ei-anchor-pulse 0.9s ease-out 1;
}
@keyframes ei-anchor-pulse {
    0% { box-shadow: 0 0 0 0 rgba(41, 181, 232, 0.55); }
    100% { box-shadow: 0 0 0 12px rgba(41, 181, 232, 0); }
}
.user-bubble.ei-dash-bubble,
.ai-bubble.ei-dash-bubble {
    font-size: 16px !important;
    line-height: 1.55 !important;
    padding: 16px 18px !important;
    margin: 12px 0 !important;
    max-width: 96% !important;
}
.ei-followups-hero {
    margin: 14px 0 18px 0;
    padding: 14px 16px;
    border-radius: 14px;
    border: 1px solid rgba(41, 181, 232, 0.28);
    background: rgba(41, 181, 232, 0.06);
}
.ei-followups-hero-title {
    font-size: 12px;
    font-weight: 800;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--dm-sky);
    margin: 0 0 10px 0;
}
@media (prefers-reduced-motion: reduce) {
    .ei-bubble-enter { animation: none !important; }
    .ei-flash-once { animation: none !important; }
}
.ei-example-section-label {
    font-size: 12px;
    font-weight: 800;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--dm-sky);
    margin: 6px 0 12px 0;
    padding-bottom: 8px;
    border-bottom: 1px solid rgba(41, 181, 232, 0.25);
}
/* Main area: larger secondary buttons (example picks, follow-ups, starter chips) */
section.main div[data-testid="stButton"] button:not([kind="primary"]) {
    min-height: 56px !important;
    padding: 14px 16px !important;
    font-size: 16px !important;
    font-weight: 500 !important;
    line-height: 1.35 !important;
    white-space: normal !important;
    text-align: left !important;
    justify-content: flex-start !important;
}
[data-testid="stSidebar"] div[data-testid="stButton"] button:not([kind="primary"]) {
    min-height: unset !important;
    padding: inherit !important;
    font-size: inherit !important;
    font-weight: inherit !important;
    line-height: inherit !important;
    white-space: nowrap !important;
    text-align: center !important;
    justify-content: center !important;
}
/* Left dashboard column only: starter prompts inside tabs — full-width feel, taller targets */
section.main div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:first-child
  [data-testid="stTabs"] div[data-testid="stButton"] button:not([kind="primary"]) {
    min-height: 64px !important;
    padding: 16px 14px !important;
    font-size: 15px !important;
    font-weight: 600 !important;
    line-height: 1.4 !important;
    white-space: normal !important;
    text-align: left !important;
    justify-content: flex-start !important;
    width: 100% !important;
}
details summary {
    font-weight: 600 !important;
    color: var(--dm-sky) !important;
}
/* Query classification */
.ei-query-meta {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px 12px;
    margin-bottom: 12px;
    padding: 10px 12px;
    background: var(--dm-bg-mid);
    border: 1px solid var(--dm-border);
    border-radius: 10px;
    font-size: 13px;
    color: var(--dm-muted);
}
.ei-query-badge {
    display: inline-block;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: #061018;
    background: var(--dm-sky);
    padding: 4px 10px;
    border-radius: 6px;
}
.ei-query-desc {
    flex: 1;
    min-width: 140px;
    line-height: 1.4;
}
/* Spinner / caption */
.stCaption, [data-testid="stCaption"] {
    color: var(--dm-muted2) !important;
}
@media (prefers-reduced-motion: reduce) {
    .welcome-hero-shell, .welcome-chip, .welcome-back-strip,
    .analyst-loading-wrap .analyst-loading-card, .analyst-orb, .analyst-dots span, .analyst-hint,
    .ei-narrative-card, .ei-empty-state, .user-bubble, .ai-bubble,
    div[data-testid="stVegaLiteChart"], div[data-testid="stArrowVegaLiteChart"],
    div[data-testid*="VegaLiteChart"], div[data-testid="stDataFrame"] {
        animation: none !important;
    }
    .user-bubble, .ai-bubble, .welcome-chip, div[data-testid="stMetric"],
    button[kind="primary"], div[data-testid="stButton"] button, .sql-box, .persona-hint-row {
        transition: none !important;
    }
}
@keyframes ei-empty-in {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}
</style>


""",
    unsafe_allow_html=True,
)


def _render_persona_hints(selected: str) -> None:
    parts: list[str] = ['<div class="persona-hints">']
    for name in PERSONAS:
        hint = html.escape(PERSONA_UI_HINTS[name])
        cls = "persona-hint-row persona-hint-active" if name == selected else "persona-hint-row"
        parts.append(
            f'<div class="{cls}"><div class="persona-hint-name">{html.escape(name)}</div>'
            f'<div class="persona-hint-text">{hint}</div></div>'
        )
    parts.append("</div>")
    st.markdown("".join(parts), unsafe_allow_html=True)


def _render_narrative_card(text: str, *, compact: bool = False) -> None:
    """LLM summary with product-style framing (not default st.info)."""
    safe = html.escape(text.strip()).replace("\n", "<br/>")
    card_cls = "ei-narrative-card ei-narrative-card--compact" if compact else "ei-narrative-card"
    st.markdown(
        f'<div class="{card_cls}">'
        '<div class="ei-narrative-head">'
        '<span class="ei-narrative-kicker">Narrative</span>'
        '<span class="ei-narrative-badge">Cortex COMPLETE</span></div>'
        f'<div class="ei-narrative-body">{safe}</div></div>',
        unsafe_allow_html=True,
    )


def _connection_auth():
    if session is None:
        return None, None
    conn = session.connection
    rest = getattr(conn, "_rest", None) or getattr(conn, "rest", None)
    token = None
    if rest:
        token = getattr(rest, "_token_or_authenticator", None) or getattr(rest, "token", None)
    host = getattr(conn, "host", None)
    return host, token


def call_cortex_analyst(question: str, history: list[dict[str, str]]) -> dict[str, Any]:
    """POST /api/v2/cortex/analyst/message.

    Use a single user turn for maximum compatibility in SiS deployments that
    enforce strict message validation.
    """
    host, token = _connection_auth()
    if not host or not token:
        return {"error": "No Snowflake session or REST token. Run inside Streamlit in Snowflake."}

    _ = history  # Intentionally unused for stateless Analyst requests.
    api_messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "content": [{"type": "text", "text": question}],
        }
    ]

    payload = {
        "messages": api_messages,
        "semantic_model_file": SEMANTIC_MODEL_FILE,
    }
    url = f"https://{host}{CORTEX_ANALYST_PATH}"
    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=180)
        try:
            body = r.json()
        except Exception:  # noqa: BLE001
            return {"error": r.text or f"HTTP {r.status_code}"}
        if r.status_code != 200:
            return {"error": body.get("message", r.text), "raw": body}
        return body
    except Exception as ex:  # noqa: BLE001
        return {"error": str(ex)}


def run_sql(sql: str) -> pd.DataFrame:
    if session is None:
        return pd.DataFrame({"Error": ["No active session"]})
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:  # noqa: BLE001
        return pd.DataFrame({"Error": [str(e)]})


def _chart_numeric_columns(df: pd.DataFrame) -> list[str]:
    """Numeric dtypes plus object/string columns that are mostly parseable as numbers (e.g. Snowflake DECIMAL → object)."""
    if df.empty or "Error" in df.columns:
        return []
    out: list[str] = []
    for c in df.columns:
        if str(c) == "Error":
            continue
        s = df[c]
        if pd.api.types.is_bool_dtype(s):
            continue
        if pd.api.types.is_numeric_dtype(s):
            out.append(c)
            continue
        coerced = pd.to_numeric(s, errors="coerce")
        nn = int(coerced.notna().sum())
        if nn < 2:
            continue
        raw_nn = int(s.notna().sum())
        if raw_nn == 0:
            continue
        if nn / raw_nn >= 0.85:
            out.append(c)
    return out


def _time_series_cols(df: pd.DataFrame) -> tuple[str | None, str | None]:
    if df.empty or "Error" in df.columns:
        return None, None
    cols = df.columns.tolist()
    date_col = next(
        (
            c
            for c in cols
            if "date" in c.lower()
            or c.lower() in ("month", "observation_date", "observation_month")
        ),
        None,
    )
    num_cols = [c for c in _chart_numeric_columns(df) if c != date_col]
    if date_col and num_cols:
        return date_col, num_cols[0]
    return None, None


def _events_in_series_range(dmin: pd.Timestamp, dmax: pd.Timestamp) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for ds, label in ECONOMIC_EVENTS:
        t = pd.Timestamp(ds)
        if dmin <= t <= dmax:
            out.append((ds, label))
    return out


def _zscore_anomaly_rows(df: pd.DataFrame, date_col: str, value_col: str, z_thr: float = 3.0) -> pd.DataFrame:
    try:
        s = pd.to_numeric(df[value_col], errors="coerce").dropna()
        if len(s) < 5:
            return pd.DataFrame()
        mu, sig = float(s.mean()), float(s.std())
        if sig == 0 or pd.isna(sig):
            return pd.DataFrame()
        work = df[[date_col, value_col]].copy()
        work["_v"] = pd.to_numeric(work[value_col], errors="coerce")
        work["_z"] = (work["_v"] - mu) / sig
        work = work[work["_z"].abs() >= z_thr].dropna(subset=["_v"])
        work = work.rename(columns={"_z": "z_score"})
        return work[[date_col, value_col, "z_score"]].sort_values(date_col)
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


def _correlation_insight_line(df: pd.DataFrame, date_col: str, primary_numeric: str) -> str:
    if session is None or df.empty or "Error" in df.columns:
        return ""
    try:
        dts = pd.to_datetime(df[date_col], errors="coerce")
        d0, d1 = dts.min(), dts.max()
        if pd.isna(d0) or pd.isna(d1):
            return ""
        d0s = pd.Timestamp(d0).strftime("%Y-%m-%d")
        d1s = pd.Timestamp(d1).strftime("%Y-%m-%d")
        wide = run_sql(
            f"SELECT * FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE "
            f"WHERE OBSERVATION_DATE BETWEEN '{d0s}' AND '{d1s}'"
        )
        if wide.empty or "Error" in wide.columns:
            return ""
        num = wide.select_dtypes(include="number").columns.tolist()
        if not num or len(num) < 2:
            return ""
        # Match primary column case-insensitively to wide columns
        primary_upper = primary_numeric.upper()
        match = next((c for c in num if c.upper() == primary_upper), None)
        if match is None:
            match = num[0]
        if match not in wide.columns:
            return ""
        sub = wide[["OBSERVATION_DATE"] + num].copy()
        sub["OBSERVATION_DATE"] = pd.to_datetime(sub["OBSERVATION_DATE"], errors="coerce")
        sub = sub.dropna(subset=["OBSERVATION_DATE"]).sort_values("OBSERVATION_DATE")
        corr = sub[num].corr(numeric_only=True)
        if match not in corr.columns:
            return ""
        series = corr[match].drop(labels=[match], errors="ignore").abs().sort_values(ascending=False)
        if series.empty:
            return ""
        top_name = str(series.index[0])
        top_r = float(series.iloc[0])
        if pd.isna(top_r):
            return ""
        return (
            f"Cross-indicator note: **{match.replace('_', ' ')}** aligns most closely with "
            f"**{top_name.replace('_', ' ')}** on this date range (|r| ≈ {top_r:.2f}) using the macro wide panel."
        )
    except Exception:  # noqa: BLE001
        return ""


def build_brief_pdf_bytes(title: str, narrative: str, sql_text: str, df: pd.DataFrame) -> bytes | None:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except ImportError:
        return None
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, title=title[:40])
    styles = getSampleStyleSheet()
    story: list[Any] = []
    story.append(Paragraph(html.escape(title), styles["Title"]))
    story.append(
        Paragraph(
            f"<i>Powered by Snowflake Cortex Analyst — {datetime.now().strftime('%Y-%m-%d %H:%M')}</i>",
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 12))
    story.append(Paragraph("<b>Executive summary</b>", styles["Heading2"]))
    story.append(Paragraph(html.escape(narrative or "(none)"), styles["BodyText"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("<b>SQL (transparency)</b>", styles["Heading2"]))
    story.append(
        Paragraph(
            f"<font face='Courier'>{html.escape((sql_text or '—')[:8000])}</font>",
            styles["BodyText"],
        )
    )
    story.append(Spacer(1, 12))
    story.append(Paragraph("<b>Data preview (first rows)</b>", styles["Heading2"]))
    prev = df.head(12)
    if prev.empty:
        story.append(Paragraph("(empty)", styles["Normal"]))
    else:
        data = [list(prev.columns)] + [[html.escape(str(x))[:80] for x in row] for row in prev.values.tolist()]
        t = Table(data, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1565c0")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ]
            )
        )
        story.append(t)
    doc.build(story)
    return buf.getvalue()


def generate_narrative(question: str, df: pd.DataFrame, persona: str = "Executive") -> str:
    if df.empty or "Error" in df.columns or session is None:
        return ""
    persona_key = persona if persona in PERSONAS else "Executive"
    voice = PERSONAS[persona_key]
    summary = df.head(10).to_string(index=False)
    prompt = f"""{voice}

User question: "{question}"

Data (sample rows):
{summary}

Write the response for this persona. Be specific with numbers from the data.
Do not start with "The data shows" or "Based on the results".
Plain prose, no bullet points unless Press mode needs a very short second sentence."""

    try:
        result = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE('"
            + CORTEX_COMPLETE_MODEL
            + "', $$"
            + prompt
            + "$$) AS narrative"
        ).collect()[0]["NARRATIVE"]
        return result.strip() if result else ""
    except Exception as e:  # noqa: BLE001
        st.sidebar.write(f"Narrative error: {e}")
        return ""


def render_chart(df: pd.DataFrame, question: str = "") -> None:
    """Pick line / area / bar / scatter / table from data shape + question hints."""
    if df.empty or "Error" in df.columns:
        return
    qctx = question or str(st.session_state.get("last_user_question") or "")
    kind, reason = _infer_chart_plan(df, qctx)
    st.caption(f"**Auto chart:** {reason}")
    cols = df.columns.tolist()
    date_col = next(
        (
            c
            for c in cols
            if "date" in c.lower()
            or c.lower() in ("month", "observation_date", "observation_month")
        ),
        None,
    )
    num_cols = _chart_numeric_columns(df)
    if len(df.columns) < 2 or kind == "table":
        st.dataframe(df, use_container_width=True)
        return
    if not num_cols:
        st.dataframe(df, use_container_width=True)
        return

    if date_col and num_cols and kind in ("line", "area"):
        try:
            plot_df = df[[date_col] + num_cols].copy()
            plot_df[date_col] = pd.to_datetime(plot_df[date_col], errors="coerce")
            for _nc in num_cols:
                if not pd.api.types.is_numeric_dtype(plot_df[_nc]):
                    plot_df[_nc] = pd.to_numeric(plot_df[_nc], errors="coerce")
            plot_df = plot_df.sort_values(date_col).set_index(date_col)
            if kind == "area":
                st.area_chart(plot_df, use_container_width=True)
            else:
                st.line_chart(plot_df, use_container_width=True)
            dcol, vcol = date_col, num_cols[0]
            plot_df_reset = plot_df.reset_index()
            anomalies = _zscore_anomaly_rows(plot_df_reset, dcol, vcol)
            if not anomalies.empty:
                with st.expander("Statistical outliers (≈3σ on primary series)", expanded=False):
                    st.caption("Z-score screening on the first numeric series — highlights extreme points vs period mean.")
                    st.dataframe(anomalies, use_container_width=True)
                    try:
                        sc = anomalies.rename(columns={dcol: "date", vcol: "value"})
                        st.scatter_chart(sc, x="date", y="value", use_container_width=True)
                    except Exception:  # noqa: BLE001
                        pass
            dts = pd.to_datetime(df[date_col], errors="coerce")
            dmin, dmax = dts.min(), dts.max()
            if not pd.isna(dmin) and not pd.isna(dmax):
                evs = _events_in_series_range(pd.Timestamp(dmin), pd.Timestamp(dmax))
                if evs:
                    with st.expander("Economic context — events in this window", expanded=False):
                        for ds, lab in evs:
                            st.markdown(f"- **{ds}** — {lab}")
            return
        except Exception:  # noqa: BLE001
            pass

    cat_cols = [c for c in cols if c not in num_cols]
    if kind == "bar" and cat_cols and len(num_cols) == 1:
        try:
            plot_df = df[[cat_cols[0], num_cols[0]]].copy()
            plot_df = plot_df.set_index(cat_cols[0])
            st.bar_chart(plot_df, use_container_width=True)
            return
        except Exception:  # noqa: BLE001
            pass

    if kind == "scatter" and len(num_cols) >= 2:
        try:
            xn, yn = num_cols[0], num_cols[1]
            scat = df[[xn, yn]].copy()
            st.scatter_chart(scat, x=xn, y=yn, use_container_width=True)
            return
        except Exception:  # noqa: BLE001
            pass

    # Fallback: try bar then line then table
    if cat_cols and len(num_cols) == 1:
        try:
            plot_df = df[[cat_cols[0], num_cols[0]]].copy()
            plot_df = plot_df.set_index(cat_cols[0])
            st.bar_chart(plot_df, use_container_width=True)
            return
        except Exception:  # noqa: BLE001
            pass
    if date_col and num_cols:
        try:
            plot_df = df[[date_col] + num_cols].copy()
            plot_df[date_col] = pd.to_datetime(plot_df[date_col], errors="coerce")
            for _nc in num_cols:
                if not pd.api.types.is_numeric_dtype(plot_df[_nc]):
                    plot_df[_nc] = pd.to_numeric(plot_df[_nc], errors="coerce")
            plot_df = plot_df.sort_values(date_col).set_index(date_col)
            st.line_chart(plot_df, use_container_width=True)
            return
        except Exception:  # noqa: BLE001
            pass
    st.dataframe(df, use_container_width=True)


SUGGESTED_CORE = [
    "What is the US unemployment trend from 2020 to 2024?",
    "How have Treasury bill rates changed since 2020?",
    "What are the top retail sales categories in 2023?",
    "How did aerospace industrial production trend from 2019 to 2023?",
    "Which industrial sectors had the highest production in 2023?",
    "How did unemployment differ between men and women in 2022?",
]

SUGGESTED_PRICES_GDP = [
    "Show monthly headline CPI index from 2019 through 2024.",
    "What was the peak year-over-year headline CPI inflation rate in 2022?",
    "Show US real GDP by quarter for the last five years.",
]

SUGGESTED_MACRO_WIDE = [
    "Compare unemployment and CPI on the same monthly timeline since 2020.",
    "Compare unemployment and industrial production over time since 2020.",
    "Compare unemployment trend and total retail sales (USD) since 2020.",
]

SUGGESTED_COMPANIES = [
    "Which company owns the most subsidiaries?",
    "What subsidiaries does Kroger own?",
    "What subsidiaries does Marriott own?",
]

# Left-rail quick picks — ordered for time-series / multi-series charts (line & area)
EXAMPLE_QUICK: list[tuple[str, str]] = [
    ("📈", "Show monthly headline CPI index from 2019 through 2024."),
    ("📈", "Show US real GDP by quarter for the last five years."),
    ("📈", "What is the US unemployment trend from 2020 to 2024?"),
    ("📈", "How have Treasury bill rates changed since 2020?"),
    ("📈", "Compare unemployment and CPI on the same monthly timeline since 2020."),
    ("📈", "Compare unemployment and industrial production over time since 2020."),
    ("📈", "Compare unemployment trend and total retail sales (USD) since 2020."),
    ("📊", "How did interest rates change between 2022 and 2023?"),
]

# Left-rail when Workspace = Company relationships (tables / corporate graph)
EXAMPLE_QUICK_COMPANY: list[tuple[str, str]] = [
    ("🔗", "Which company owns the most subsidiaries?"),
    ("🔗", "Which companies have more than five subsidiaries?"),
    ("🏢", "What subsidiaries does Kroger own?"),
    ("🏢", "What subsidiaries does Marriott own?"),
    ("🔗", "Show parent companies ranked by subsidiary count."),
    ("📊", "What are the top retail sales categories in 2023?"),
    ("📈", "Show monthly headline CPI index from 2019 through 2024."),
]


def _workspace_quick_examples(ws: str) -> tuple[str, list[tuple[str, str]]]:
    """Section label + vertical example buttons for the selected workspace."""
    if (ws or "").strip() == "Company relationships":
        return "Company & relationship examples", EXAMPLE_QUICK_COMPANY
    return "Macro & chart-ready examples", EXAMPLE_QUICK


def _render_vertical_examples(items: list[tuple[str, str]], key_prefix: str) -> None:
    for i, (icon, q) in enumerate(items):
        # Full text on the button — CSS wraps; wide rail + large tap targets for demos
        label = f"{icon}  {q}"
        if st.button(label, key=f"{key_prefix}_{i}", use_container_width=True):
            st.session_state.pending_question = q
            st.rerun()


def _render_suggestion_chips(questions: list[str], key_prefix: str) -> None:
    # One button per row — full rail width so long prompts don’t squeeze into half-columns (mobile / narrow layout).
    for i, q in enumerate(questions):
        if st.button(q, key=f"{key_prefix}_{i}", use_container_width=True):
            st.session_state.pending_question = q
            st.rerun()

# ── session state ─────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_sql" not in st.session_state:
    st.session_state.last_sql = None
if "last_df" not in st.session_state:
    st.session_state.last_df = None
if "last_interpretation" not in st.session_state:
    st.session_state.last_interpretation = None
if "pending_question" not in st.session_state:
    st.session_state.pending_question = None
if "last_followups" not in st.session_state:
    st.session_state.last_followups = []
if "welcome_hero_dismissed" not in st.session_state:
    st.session_state.welcome_hero_dismissed = False
if "last_user_question" not in st.session_state:
    st.session_state.last_user_question = ""
if "last_query_class" not in st.session_state:
    st.session_state.last_query_class = ""
if "last_query_class_desc" not in st.session_state:
    st.session_state.last_query_class_desc = ""
if "last_ambiguity_warnings" not in st.session_state:
    st.session_state.last_ambiguity_warnings = []
if "last_debug_payload" not in st.session_state:
    st.session_state.last_debug_payload = None
if "ei_top_nav" not in st.session_state:
    st.session_state.ei_top_nav = "dashboard"

def _glass_panel():
    """Frosted panel wrapper; uses container(border=…) when the runtime supports it."""
    if "border" in inspect.signature(st.container).parameters:
        return st.container(border=True)
    return st.container()


def _emit_dashboard_scroll(target_id: str) -> None:
    """Scroll the host page to a dashboard anchor (Streamlit runs inside an iframe)."""
    sid = json.dumps(target_id)
    components.html(
        f"""
        <script>
        (function() {{
          try {{
            const doc = window.parent.document;
            const el = doc.getElementById({sid});
            if (!el) return;
            const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
            el.scrollIntoView({{ behavior: reduce ? "auto" : "smooth", block: "start" }});
            el.classList.add("ei-flash-once");
            setTimeout(function() {{
              try {{ el.classList.remove("ei-flash-once"); }} catch (e) {{}}
            }}, 1100);
          }} catch (e) {{}}
        }})();
        </script>
        """,
        height=0,
    )


def _consume_dashboard_scroll_if_any() -> None:
    tid = st.session_state.pop("ei_scroll_target", None)
    if tid in ("ei-thread-anchor", "ei-results-anchor"):
        _emit_dashboard_scroll(tid)


def _render_session_chat_bubbles(
    *,
    empty_hint: str,
    bubbles_off_intro: str,
    dashboard_thread: bool = False,
) -> None:
    msgs = st.session_state.get("messages") or []
    if not msgs:
        st.caption(empty_hint)
        return
    if st.session_state.get("ei_show_bubbles", True):
        n = len(msgs)
        dash = " ei-dash-bubble" if dashboard_thread else ""
        for i, msg in enumerate(msgs):
            anim = " ei-bubble-enter" if dashboard_thread and i >= n - 2 else ""
            if msg["role"] == "user":
                st.markdown(
                    f'<div class="user-bubble{dash}{anim}">{html.escape(msg["content"])}</div>'
                    '<div class="clearfix"></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="ai-bubble{dash}{anim}">{html.escape(msg["content"])}</div>'
                    '<div class="clearfix"></div>',
                    unsafe_allow_html=True,
                )
    else:
        st.caption(bubbles_off_intro)
        for msg in msgs:
            role = "You" if msg.get("role") == "user" else "Assistant"
            safe_txt = html.escape(str(msg.get("content", ""))).replace("\n", "<br/>")
            st.markdown(f"**{role}**  \n{safe_txt}", unsafe_allow_html=True)


def _render_chat_history_page() -> None:
    with _glass_panel():
        st.markdown(
            '<div class="section-label-lg">Chat history</div>',
            unsafe_allow_html=True,
        )
        st.caption(
            "Full session thread (same data as **Dashboard**). Download the transcript from the **sidebar**."
        )
        chat_container = st.container(height=620)
        with chat_container:
            _render_session_chat_bubbles(
                empty_hint="No messages yet — go to **Dashboard** and ask a question.",
                bubbles_off_intro="Bubbles off (sidebar) — plain text:",
                dashboard_thread=True,
            )


def _render_dashboard_layout() -> tuple[bool, str]:
    submitted = False
    user_input = ""
    _left_col, _center_col, _right_col = st.columns([5, 10, 6], gap="medium")

    with _left_col:
        with _glass_panel():
            st.selectbox(
                "Workspace",
                ["Economic analytics", "Company relationships"],
                key="ei_workspace",
                help="Switches the example rail and tab order to match your topic; same semantic model underneath.",
            )
            _ws = st.session_state.get("ei_workspace") or "Economic analytics"
            _ex_label, _ex_items = _workspace_quick_examples(_ws)
            st.markdown(
                f'<div class="ei-example-section-label">{html.escape(_ex_label)}</div>',
                unsafe_allow_html=True,
            )
            _ex_key = "ex_left_co" if _ws == "Company relationships" else "ex_left_econ"
            _render_vertical_examples(_ex_items, _ex_key)
            with st.expander("More starter prompts (tabs)", expanded=True):
                if _ws == "Company relationships":
                    tab_co, tab_core, tab_pg, tab_wide = st.tabs(
                        ["Cos.", "Macro", "CPI/GDP", "Wide"]
                    )
                    with tab_co:
                        _render_suggestion_chips(SUGGESTED_COMPANIES, "sug_co")
                    with tab_core:
                        _render_suggestion_chips(SUGGESTED_CORE, "sug_core")
                    with tab_pg:
                        _render_suggestion_chips(SUGGESTED_PRICES_GDP, "sug_pg")
                    with tab_wide:
                        _render_suggestion_chips(SUGGESTED_MACRO_WIDE, "sug_wide")
                else:
                    tab_core, tab_pg, tab_wide, tab_co = st.tabs(
                        ["Macro", "CPI/GDP", "Wide", "Cos."]
                    )
                    with tab_core:
                        _render_suggestion_chips(SUGGESTED_CORE, "sug_core")
                    with tab_pg:
                        _render_suggestion_chips(SUGGESTED_PRICES_GDP, "sug_pg")
                    with tab_wide:
                        _render_suggestion_chips(SUGGESTED_MACRO_WIDE, "sug_wide")
                    with tab_co:
                        _render_suggestion_chips(SUGGESTED_COMPANIES, "sug_co")
            st.selectbox(
                "Response voice",
                list(PERSONAS.keys()),
                key="persona_perspective",
                help="Tone for Cortex COMPLETE summaries.",
            )
            with st.expander("Voice details", expanded=False):
                _render_persona_hints(
                    st.session_state.get("persona_perspective") or list(PERSONAS.keys())[0]
                )

    with _center_col:
        with _glass_panel():
            _ws_center = st.session_state.get("ei_workspace") or "Economic analytics"
            if _ws_center == "Company relationships":
                _ask_sub = (
                    'Type below or tap a <strong>company</strong> example on the left — '
                    "parent → subsidiary questions return <strong>lists or tables</strong>; you can still run macro questions from "
                    "<strong>More starter prompts</strong>."
                )
            else:
                _ask_sub = (
                    'Type below or tap a <strong>chart-ready</strong> example on the left — '
                    "unemployment, CPI, GDP, rates, and multi-metric compares render as <strong>line charts</strong> when the result is a time series."
                )
            st.markdown(
                f'<div class="ei-ask-head">Ask &amp; analyze</div><div class="ei-ask-sub">{_ask_sub}</div>',
                unsafe_allow_html=True,
            )
            with st.form("chat_form", clear_on_submit=True):
                _qrow1, _qrow2 = st.columns([7, 3], gap="small")
                with _qrow1:
                    user_input = st.text_input(
                        "Your question",
                        placeholder="Try: monthly CPI 2019–2024 · unemployment vs CPI since 2020 · Treasury bills since 2020…",
                        label_visibility="collapsed",
                        key="ei_chat_input",
                    )
                with _qrow2:
                    submitted = st.form_submit_button(
                        "Run analysis",
                        use_container_width=True,
                        type="primary",
                    )

            st.markdown(
                '<div id="ei-thread-anchor" class="ei-thread-anchor ei-thread-shell">'
                '<div class="ei-thread-head">Conversation thread</div>'
                '<div class="ei-thread-sub">Your question and the analyst reply show up here after each run — '
                "scroll inside the box if the exchange is long.</div></div>",
                unsafe_allow_html=True,
            )
            thread_container = st.container(height=440)
            with thread_container:
                _render_session_chat_bubbles(
                    empty_hint="Run **Run analysis** above — the conversation appears here.",
                    bubbles_off_intro="Bubbles hidden (sidebar) — plain lines below:",
                    dashboard_thread=True,
                )

            if st.session_state.last_df is not None:
                st.markdown(
                    '<div id="ei-results-anchor" class="ei-results-anchor"></div>',
                    unsafe_allow_html=True,
                )
                _ldf = st.session_state.last_df
                _dc, _vc = _time_series_cols(_ldf)
                if _dc and _vc:
                    _ci = _correlation_insight_line(_ldf, _dc, _vc)
                    if _ci:
                        with st.expander("Cross-indicator note", expanded=False):
                            st.markdown(_ci)
                with st.container(height=220):
                    render_chart(_ldf, st.session_state.get("last_user_question") or "")
                if st.session_state.last_followups:
                    st.markdown(
                        '<div class="ei-followups-hero">'
                        '<div class="ei-followups-hero-title">Suggested next questions</div></div>',
                        unsafe_allow_html=True,
                    )
                    _fu_cols = st.columns(2)
                    for _fi, _fq in enumerate(st.session_state.last_followups):
                        with _fu_cols[_fi % 2]:
                            if st.button(_fq, key=f"fu_center_{_fi}", use_container_width=True):
                                st.session_state.pending_question = _fq
                                st.rerun()
                _tbl_col, _tab_col = st.columns([1, 1], gap="small")
                with _tbl_col:
                    st.markdown(
                        '<div class="section-label section-label--tight" style="margin-top:4px">Data</div>',
                        unsafe_allow_html=True,
                    )
                    st.dataframe(_ldf, use_container_width=True, height=220)
                with _tab_col:
                    st.markdown(
                        '<div class="section-label section-label--tight" style="margin-top:4px">Technical</div>',
                        unsafe_allow_html=True,
                    )
                    t_sql, t_dbg, t_fu = st.tabs(["SQL", "Debug", "Follow-ups"])
                    with t_sql:
                        if st.session_state.last_sql:
                            safe_sql = html.escape(st.session_state.last_sql)
                            st.markdown(f'<div class="sql-box">{safe_sql}</div>', unsafe_allow_html=True)
                        else:
                            st.caption("No SQL in this turn.")
                    with t_dbg:
                        _dbg = st.session_state.get("last_debug_payload")
                        if _dbg:
                            st.code(_dbg, language="json")
                        else:
                            st.caption("Run a query to capture the last Analyst response payload.")
                    with t_fu:
                        if st.session_state.last_followups:
                            for _i, _fq in enumerate(st.session_state.last_followups):
                                if st.button(_fq, key=f"fu_tab_{_i}", use_container_width=True):
                                    st.session_state.pending_question = _fq
                                    st.rerun()
                        else:
                            st.caption("Follow-ups appear after a successful analysis.")
                _pdf = build_brief_pdf_bytes(
                    st.session_state.last_user_question or "US Economic Intelligence — analyst brief",
                    st.session_state.last_interpretation or "",
                    st.session_state.last_sql or "",
                    _ldf,
                )
                if _pdf:
                    st.download_button(
                        label="Export brief (PDF)",
                        data=_pdf,
                        file_name=f"economic_brief_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                        mime="application/pdf",
                        key="download_brief_pdf",
                    )
            else:
                st.markdown(
                    """
    <div class="ei-empty-state ei-empty-state--compact">
      <div class="ei-empty-icon" aria-hidden="true">📈</div>
      <div class="ei-empty-title">No analysis yet</div>
      <div class="ei-empty-copy">
        Use the question bar or an <strong>Example question</strong> on the left.
      </div>
    </div>
    """,
                    unsafe_allow_html=True,
                )

            _consume_dashboard_scroll_if_any()

    with _right_col:
        with _glass_panel():
            _rail_bits: list[str] = [
                '<div class="ei-rail-identity">',
                '<div class="ei-rail-identity-top">Source</div>',
                '<div class="ei-rail-identity-name">AI Analyst · Cortex Analyst + COMPLETE</div>',
            ]
            if st.session_state.get("last_query_class"):
                _qc = html.escape(str(st.session_state.last_query_class))
                _qd = html.escape(str(st.session_state.last_query_class_desc))
                _rail_bits.append(
                    '<div class="ei-rail-query-inline">'
                    f'<span class="ei-query-badge">{_qc}</span>'
                    f'<span class="ei-query-desc">{_qd}</span></div>'
                )
            _rail_bits.append("</div>")
            st.markdown("".join(_rail_bits), unsafe_allow_html=True)
            for _warn in st.session_state.get("last_ambiguity_warnings") or []:
                st.warning(_warn)
            if st.session_state.last_df is not None and st.session_state.last_interpretation:
                st.markdown(
                    '<div class="section-label section-label--tight">Interpretation</div>',
                    unsafe_allow_html=True,
                )
                with st.container(height=172):
                    _render_narrative_card(st.session_state.last_interpretation, compact=True)
            elif st.session_state.last_df is not None:
                st.caption("No narrative this turn — see **Conversation thread**.")
            else:
                st.caption("Interpretation appears after you run an analysis.")
            with st.expander(
                "Suggested follow-ups",
                expanded=bool(st.session_state.last_followups),
            ):
                if st.session_state.last_followups:
                    for i, fq in enumerate(st.session_state.last_followups):
                        if st.button(fq, key=f"fu_rail_{i}", use_container_width=True):
                            st.session_state.pending_question = fq
                            st.rerun()
                else:
                    st.caption("Run a query for ideas.")

    # ══════════════════════════════════════════════════════════════════════════
    return submitted, user_input

# ══════════════════════════════════════════════════════════════════════════
#  LAYOUT
# ══════════════════════════════════════════════════════════════════════════

if session is None:
    st.error("Run this app inside **Streamlit in Snowflake** (active Snowpark session required).")
    st.stop()

with st.sidebar:
    st.markdown("### Chat & session")
    st.caption(
        "History is this browser session only. Use **Chat history** (top) for a full-page thread, or expand **Full chat transcript** below."
    )
    st.checkbox("Show conversation bubbles", value=True, key="ei_show_bubbles")
    hist_lines: list[str] = []
    for _m in st.session_state.get("messages", []):
        _role = "You" if _m.get("role") == "user" else "Assistant"
        hist_lines.append(f"**{_role}**\n{_m.get('content', '')}")
    _transcript = "\n\n---\n\n".join(hist_lines) if hist_lines else "(No messages yet.)"
    with st.expander("Full chat transcript", expanded=False):
        st.code(_transcript, language=None)
    st.download_button(
        label="Download transcript (.txt)",
        data=_transcript.encode("utf-8"),
        file_name=f"economic_intel_chat_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
        mime="text/plain",
        key="ei_dl_transcript",
    )
    if st.button("Clear chat & analysis", type="secondary", key="ei_clear_session"):
        st.session_state.messages = []
        st.session_state.last_df = None
        st.session_state.last_sql = None
        st.session_state.last_interpretation = None
        st.session_state.last_followups = []
        st.session_state.last_user_question = ""
        st.session_state.last_query_class = ""
        st.session_state.last_query_class_desc = ""
        st.session_state.last_ambiguity_warnings = []
        st.session_state.pending_question = None
        st.session_state.last_debug_payload = None
        st.session_state.ei_top_nav = "dashboard"
        st.rerun()

_now_nav = html.escape(datetime.now().strftime("%I:%M %p"))
st.markdown(
    f"""
<div class="ei-topnav">
  <div class="ei-topnav-brand"><div class="ei-logo-rings" aria-hidden="true"></div>
    <span>Executive BI Copilot</span></div>
  <div class="ei-topnav-meta"><span class="ei-topnav-time">{_now_nav}</span><span>Snowflake · Cortex</span></div>
</div>
""",
    unsafe_allow_html=True,
)
_nv1, _nv2, _nv3 = st.columns([1, 3, 1])
with _nv2:
    st.radio(
        "Primary view",
        ["dashboard", "history"],
        horizontal=True,
        label_visibility="collapsed",
        key="ei_top_nav",
        format_func=lambda x: "Dashboard" if x == "dashboard" else "Chat history",
    )
st.markdown(
    """
<div class="ei-hero-copilot">
  <span class="ei-hero-badge">Live</span>
  <p class="ei-hero-title">💡 US Economic Intelligence</p>
  <p class="ei-hero-sub">Answering macro and company-relationship questions in an instant — with SQL transparency and verified fallbacks.</p>
</div>
""",
    unsafe_allow_html=True,
)

_render_welcome_hero()
_render_welcome_back_strip()

st.divider()

query_progress = st.empty()

# Capture follow-up / chip questions before any widgets (esp. st.form) run — avoids
# intermittent loss of pending_question on alternating reruns in SiS.
_followup_pending: str | None = None
_raw_pq = st.session_state.get("pending_question")
if _raw_pq:
    _pq = str(_raw_pq).strip()
    st.session_state.pending_question = None
    _followup_pending = _pq if _pq else None

if st.session_state.get("ei_top_nav", "dashboard") == "dashboard":
    submitted, user_input = _render_dashboard_layout()
else:
    submitted, user_input = False, ""
    _render_chat_history_page()

# ══════════════════════════════════════════════════════════════════════════
#  PROCESS QUESTION
# ══════════════════════════════════════════════════════════════════════════

question: str | None = None
if _followup_pending:
    question = _followup_pending
elif submitted and user_input and user_input.strip():
    question = user_input.strip()

if question:
    question_for_model = _autocorrect_question(question)
    _qc, _qd = classify_query(question_for_model)
    st.session_state.last_query_class = _qc
    st.session_state.last_query_class_desc = _qd
    st.session_state.last_ambiguity_warnings = ambiguity_warnings(question_for_model)
    st.session_state.last_user_question = question
    st.session_state.messages.append({"role": "user", "content": question})

    hint = _loading_hint_for_turn()

    def _progress_step(label: str) -> None:
        query_progress.markdown(_loading_banner_html(label, hint), unsafe_allow_html=True)

    _progress_step("Sending your question to Cortex Analyst…")

    with st.spinner("Cortex Analyst is responding…"):
        try:
            response = call_cortex_analyst(
                question_for_model,
                st.session_state.messages[:-1],
            )
            try:
                _raw_dbg = json.dumps(response, default=str, ensure_ascii=True)
                if len(_raw_dbg) > 12000:
                    _raw_dbg = _raw_dbg[:12000] + "\n…(truncated)"
                st.session_state.last_debug_payload = _raw_dbg
            except Exception:  # noqa: BLE001
                st.session_state.last_debug_payload = repr(response)[:12000]

            if response.get("error"):
                err = str(response["error"])
                if response.get("raw"):
                    err = f"{err}\n\nDetails: {json.dumps(response['raw'], ensure_ascii=True)}"
                st.session_state.messages.append({"role": "assistant", "content": err})
                st.session_state.last_df = None
                st.session_state.last_sql = None
                st.session_state.last_interpretation = None
                st.session_state.last_followups = []
                query_progress.empty()
            else:
                sql = None
                interpretation = ""
                for block in response.get("message", {}).get("content", []):
                    if block.get("type") == "sql":
                        if not sql:
                            sql = (
                                block.get("statement")
                                or block.get("sql")
                                or block.get("query")
                            )
                    elif block.get("type") == "text":
                        tx = (block.get("text") or "").strip()
                        if tx:
                            interpretation = tx
                interpretation = interpretation.strip() if interpretation else None
                _clean_analyst_text = (
                    interpretation
                    if interpretation
                    and not _analyst_text_is_question_echo(interpretation, question_for_model)
                    else None
                )

                if sql:
                    _progress_step("Running generated SQL in your Snowflake warehouse…")
                    df = run_sql(sql)
                    fb_sql = _fallback_sql_for_question(question)
                    analyst_bad = "Error" in df.columns or df.empty
                    if analyst_bad and fb_sql:
                        if "Error" in df.columns:
                            _progress_step("Analyst SQL had an error — running a verified fallback query…")
                        else:
                            _progress_step("Analyst SQL returned no rows — running a verified fallback query…")
                        df_fb = run_sql(fb_sql)
                        if "Error" not in df_fb.columns and not df_fb.empty:
                            df = df_fb
                            sql = (
                                fb_sql
                                + "\n\n-- Note: Cortex Analyst SQL failed or returned no rows; ran verified fallback query."
                            )
                        elif "Error" in df.columns:
                            df = df_fb
                            sql = fb_sql + "\n\n-- Note: Cortex Analyst SQL failed; ran verified fallback query."
                    df, sql = _recover_cpi_from_marketplace(df, sql, question_for_model)
                    st.session_state.last_sql = sql
                    st.session_state.last_df = df
                    _progress_step("Drafting an executive summary with Cortex COMPLETE…")
                    narrative = generate_narrative(
                        question_for_model,
                        df,
                        st.session_state.get("persona_perspective") or "Executive",
                    )
                    digest = _result_digest(df)
                    if narrative:
                        st.session_state.last_interpretation = narrative
                    elif _clean_analyst_text:
                        st.session_state.last_interpretation = _clean_analyst_text
                    elif digest:
                        st.session_state.last_interpretation = f"**Data summary**  \n{digest}"
                    else:
                        st.session_state.last_interpretation = None
                    _progress_step("Generating smart follow-up suggestions…")
                    _llm_fu = generate_followups(question_for_model, df)
                    _heur_fu = heuristic_followups(question_for_model, df, _qc)
                    st.session_state.last_followups = merge_followup_lists(_llm_fu, _heur_fu, 6)
                    reply = (
                        narrative
                        or _clean_analyst_text
                        or _assistant_reply_when_no_narrative(question_for_model, _qc, df)
                    )
                    if digest:
                        reply = f"{reply}\n\n{digest}"
                    st.session_state.messages.append(
                        {"role": "assistant", "content": reply}
                    )
                    query_progress.empty()
                    try:
                        st.toast("Results ready — chart and narrative updated.", icon="✅")
                    except Exception:  # noqa: BLE001
                        pass
                else:
                    fallback_sql = _fallback_sql_for_question(question)
                    if fallback_sql:
                        _progress_step("No SQL from Analyst — using a verified fallback query…")
                        df = run_sql(fallback_sql)
                        _fsql = (
                            fallback_sql
                            + "\n\n-- Note: Cortex Analyst did not return SQL; ran verified fallback query."
                        )
                        df, _fsql = _recover_cpi_from_marketplace(df, _fsql, question_for_model)
                        st.session_state.last_sql = _fsql
                        st.session_state.last_df = df
                        _progress_step("Summarizing fallback results…")
                        narrative = generate_narrative(
                            question_for_model,
                            df,
                            st.session_state.get("persona_perspective") or "Executive",
                        )
                        digest = _result_digest(df)
                        if narrative:
                            st.session_state.last_interpretation = narrative
                        elif _clean_analyst_text:
                            st.session_state.last_interpretation = _clean_analyst_text
                        elif digest:
                            st.session_state.last_interpretation = f"**Data summary**  \n{digest}"
                        else:
                            st.session_state.last_interpretation = None
                        _progress_step("Generating smart follow-up suggestions…")
                        _llm_fu2 = generate_followups(question_for_model, df)
                        _heur_fu2 = heuristic_followups(question_for_model, df, _qc)
                        st.session_state.last_followups = merge_followup_lists(_llm_fu2, _heur_fu2, 6)
                        fallback_msg = (
                            narrative
                            or _clean_analyst_text
                            or _assistant_reply_when_no_narrative(question_for_model, _qc, df)
                        )
                        if digest:
                            fallback_msg = f"{fallback_msg}\n\n{digest}"
                        st.session_state.messages.append(
                            {"role": "assistant", "content": fallback_msg}
                        )
                        query_progress.empty()
                        try:
                            st.toast("Used verified fallback SQL — check transparency panel.", icon="ℹ️")
                        except Exception:  # noqa: BLE001
                            pass
                    else:
                        msg = (
                            _clean_analyst_text
                            or "I couldn't generate SQL for that. Try rephrasing or use a suggested question."
                        )
                        st.session_state.messages.append({"role": "assistant", "content": msg})
                        st.session_state.last_df = None
                        st.session_state.last_sql = None
                        st.session_state.last_interpretation = None
                        st.session_state.last_followups = []
                        query_progress.empty()
        except Exception as e:  # noqa: BLE001
            st.session_state.messages.append(
                {"role": "assistant", "content": f"Error: {e}"}
            )
            st.session_state.last_df = None
            st.session_state.last_sql = None
            st.session_state.last_interpretation = None
            st.session_state.last_followups = []
            query_progress.empty()

    if st.session_state.get("last_df") is not None:
        st.session_state.ei_scroll_target = "ei-results-anchor"
    else:
        st.session_state.ei_scroll_target = "ei-thread-anchor"
    st.rerun()
