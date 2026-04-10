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
    if "more than 5" in t and "subsidiar" in t:
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
    if _has_any(t, ("cpi", "consumer price", "inflation")) and _has_any(t, ("monthly", "trend", "2019", "2024", "show")):
        if "gdp" not in t and "unemployment" not in t:
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

Suggest exactly 3 concise follow-up questions the user may ask next.
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
            return [str(x).strip() for x in parsed if str(x).strip()][:3]
    except Exception:  # noqa: BLE001
        return []
    return []


LOADING_HINTS: tuple[str, ...] = (
    "Headline CPI and GDP use dedicated V_CPI and V_GDP views — compare timelines on ECONOMIC_INDICATORS_WIDE.",
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
  <strong>Welcome back.</strong> Your latest answer is on the right — keep asking; charts refresh as we go.
</div>
""",
        unsafe_allow_html=True,
    )


# ── page ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="US Economic Intelligence",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:ital,wght@0,400;0,500;0,600;0,700;0,800;1,400&display=swap');
:root {
    --ei-bg0: #e8ecf6;
    --ei-bg1: #eef1f8;
    --ei-surface: rgba(255, 255, 255, 0.78);
    --ei-border: rgba(15, 23, 42, 0.08);
    --ei-text: #0f172a;
    --ei-muted: #64748b;
    --ei-accent: #0d47a1;
    --ei-teal: #00838f;
    --ei-shadow: 0 4px 28px rgba(15, 23, 42, 0.07);
    --ei-shadow-hover: 0 14px 40px rgba(15, 23, 42, 0.11);
}
html, body, [class*="css"] {
    font-family: "Plus Jakarta Sans", "Segoe UI", system-ui, -apple-system, sans-serif !important;
}
.stApp {
    background: radial-gradient(1200px 600px at 10% -10%, rgba(21, 101, 192, 0.09), transparent 55%),
                radial-gradient(900px 500px at 100% 0%, rgba(0, 131, 143, 0.07), transparent 50%),
                linear-gradient(168deg, var(--ei-bg0) 0%, var(--ei-bg1) 45%, #e4e9f2 100%) !important;
    background-attachment: fixed !important;
}
section.main > div {
    max-width: 1280px;
    margin-left: auto;
    margin-right: auto;
    padding-left: 1.25rem;
    padding-right: 1.25rem;
}
.ei-app-header {
    margin-bottom: 18px;
    padding: 20px 22px 18px 22px;
    border-radius: 20px;
    background: linear-gradient(135deg, rgba(255,255,255,0.94) 0%, rgba(248,250,252,0.92) 100%);
    border: 1px solid rgba(255, 255, 255, 0.9);
    box-shadow: var(--ei-shadow);
    position: relative;
    overflow: hidden;
}
.ei-app-header::before {
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, #0d47a1, #1565c0, #00838f, #26a69a);
    opacity: 0.95;
}
.ei-app-header-inner { position: relative; z-index: 1; }
.main-header {
    font-size: 29px; font-weight: 800;
    margin-bottom: 6px;
    letter-spacing: -0.02em;
    background: linear-gradient(120deg, #0d47a1 0%, #1565c0 40%, #00838f 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}
.sub-header {
    font-size: 14px; color: var(--ei-muted);
    margin-bottom: 0;
    line-height: 1.55;
    max-width: 820px;
}
div[data-testid="stVerticalBlockBorderWrapper"] {
    background: var(--ei-surface) !important;
    backdrop-filter: blur(18px) saturate(140%) !important;
    -webkit-backdrop-filter: blur(18px) saturate(140%) !important;
    border-radius: 18px !important;
    border: 1px solid rgba(255, 255, 255, 0.85) !important;
    box-shadow: var(--ei-shadow) !important;
    margin-bottom: 1rem !important;
    padding: 6px 4px 10px 4px !important;
    transition: box-shadow 0.35s ease, border-color 0.35s ease !important;
}
div[data-testid="stVerticalBlockBorderWrapper"]:hover {
    box-shadow: var(--ei-shadow-hover) !important;
    border-color: rgba(21, 101, 192, 0.12) !important;
}
div[data-testid="stMetric"] {
    background: linear-gradient(145deg, rgba(255,255,255,0.98), rgba(248,250,252,0.95)) !important;
    border-radius: 14px !important;
    border: 1px solid var(--ei-border) !important;
    padding: 12px 14px !important;
    box-shadow: 0 2px 12px rgba(15, 23, 42, 0.04) !important;
    transition: transform 0.22s ease, box-shadow 0.22s ease, border-color 0.22s ease !important;
}
div[data-testid="stMetric"]:hover {
    transform: translateY(-3px);
    box-shadow: 0 10px 28px rgba(21, 101, 192, 0.1) !important;
    border-color: rgba(21, 101, 192, 0.14) !important;
}
div[data-testid="stMetric"] label { color: var(--ei-muted) !important; font-weight: 600 !important; font-size: 11px !important; letter-spacing: 0.06em !important; text-transform: uppercase !important; }
div[data-testid="stMetric"] [data-testid="stMetricValue"] { color: var(--ei-accent) !important; font-weight: 700 !important; }
button[kind="primary"] {
    background: linear-gradient(135deg, #1565c0 0%, #0d47a1 55%, #0a3d7a 100%) !important;
    border: none !important;
    font-weight: 700 !important;
    letter-spacing: 0.02em !important;
    border-radius: 12px !important;
    box-shadow: 0 4px 16px rgba(13, 71, 161, 0.28) !important;
    transition: transform 0.18s ease, box-shadow 0.22s ease, filter 0.2s ease !important;
}
button[kind="primary"]:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(13, 71, 161, 0.35) !important;
    filter: brightness(1.04);
}
div[data-testid="stButton"] button:not([kind="primary"]) {
    border-radius: 10px !important;
    border: 1px solid rgba(21, 101, 192, 0.18) !important;
    background: rgba(255,255,255,0.9) !important;
    font-weight: 600 !important;
    transition: transform 0.18s ease, box-shadow 0.2s ease, border-color 0.2s ease, background 0.2s ease !important;
}
div[data-testid="stButton"] button:not([kind="primary"]):hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 18px rgba(21, 101, 192, 0.12) !important;
    border-color: rgba(21, 101, 192, 0.35) !important;
    background: rgba(255,255,255,1) !important;
}
div[data-testid="stTabs"] [role="tablist"] button {
    font-weight: 600 !important;
    font-size: 13px !important;
    border-radius: 10px 10px 0 0 !important;
    transition: color 0.2s ease, background 0.2s ease !important;
}
div[data-testid="stTabs"] [role="tablist"] button[aria-selected="true"] {
    color: var(--ei-accent) !important;
}
.stTextInput input {
    border-radius: 12px !important;
    border: 1px solid rgba(15, 23, 42, 0.1) !important;
    padding: 12px 14px !important;
    font-size: 14px !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
}
.stTextInput input:focus {
    border-color: rgba(21, 101, 192, 0.45) !important;
    box-shadow: 0 0 0 3px rgba(21, 101, 192, 0.12) !important;
}
hr {
    margin: 1.1rem 0 !important;
    border: none !important;
    height: 1px !important;
    background: linear-gradient(90deg, transparent, rgba(15,23,42,0.1), transparent) !important;
}
.ei-narrative-card {
    border-radius: 16px;
    padding: 16px 18px 18px 18px;
    margin-bottom: 14px;
    background: linear-gradient(155deg, rgba(255,255,255,0.97) 0%, rgba(241, 248, 255, 0.92) 100%);
    border: 1px solid rgba(21, 101, 192, 0.12);
    box-shadow: 0 6px 28px rgba(13, 71, 161, 0.08);
    position: relative;
    overflow: hidden;
    animation: ei-card-in 0.55s cubic-bezier(0.22, 1, 0.36, 1) both;
}
.ei-narrative-card::before {
    content: "";
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 4px;
    background: linear-gradient(180deg, #1565c0, #00838f);
    border-radius: 4px 0 0 4px;
}
.ei-narrative-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 10px;
    padding-left: 8px;
}
.ei-narrative-kicker {
    font-size: 11px;
    font-weight: 800;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--ei-accent);
}
.ei-narrative-badge {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: #fff;
    background: linear-gradient(135deg, #1565c0, #00838f);
    padding: 4px 10px;
    border-radius: 999px;
    box-shadow: 0 2px 8px rgba(21, 101, 192, 0.25);
}
.ei-narrative-body {
    font-size: 14px;
    line-height: 1.65;
    color: #1e293b;
    padding-left: 8px;
}
@keyframes ei-card-in {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}
.user-bubble {
    background: linear-gradient(135deg, #1565c0 0%, #0d47a1 100%);
    color: white;
    padding: 12px 18px; border-radius: 18px 18px 5px 18px;
    margin: 10px 0; max-width: 80%; float: right; clear: both;
    font-size: 14px;
    line-height: 1.5;
    box-shadow: 0 4px 18px rgba(13, 71, 161, 0.28);
    transition: transform 0.2s ease, box-shadow 0.25s ease;
}
.user-bubble:hover {
    transform: translateY(-2px) scale(1.01);
    box-shadow: 0 8px 28px rgba(13, 71, 161, 0.32);
}
.ai-bubble {
    background: linear-gradient(145deg, #ffffff 0%, #f1f5f9 55%, #eef2ff 100%);
    color: #1a1a2e;
    padding: 12px 18px; border-radius: 18px 18px 18px 5px;
    margin: 10px 0; max-width: 85%; float: left; clear: both;
    font-size: 14px;
    line-height: 1.5;
    border: 1px solid rgba(21, 101, 192, 0.11);
    box-shadow: 0 4px 16px rgba(15, 23, 42, 0.06);
    transition: transform 0.2s ease, box-shadow 0.25s ease;
}
.ai-bubble:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 26px rgba(15, 23, 42, 0.09);
}
.sql-box {
    background: linear-gradient(165deg, #141a28 0%, #1c2436 45%, #252d42 100%);
    color: #a8d4ff;
    padding: 14px 16px; border-radius: 14px;
    font-family: ui-monospace, "Cascadia Code", "Source Code Pro", Menlo, monospace;
    font-size: 12px;
    line-height: 1.45;
    white-space: pre-wrap; overflow-x: auto; margin-top: 10px;
    border: 1px solid rgba(130, 196, 255, 0.2);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.06), 0 8px 32px rgba(0,0,0,0.2);
    transition: box-shadow 0.25s ease, border-color 0.25s ease;
}
.sql-box:hover {
    border-color: rgba(130, 196, 255, 0.32);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.08), 0 12px 36px rgba(0,0,0,0.25);
}
.clearfix { clear: both; }
.section-label {
    font-size: 10px; font-weight: 800;
    text-transform: uppercase; letter-spacing: 0.14em;
    color: var(--ei-accent);
    margin-bottom: 10px;
    padding-left: 12px;
    border-left: 3px solid var(--ei-teal);
}
@keyframes analyst-float-y {
    0%, 100% { transform: translateY(0); }
    50% { transform: translateY(-10px); }
}
@keyframes analyst-orbit {
    0% { transform: rotate(0deg) translateX(28px) rotate(0deg); }
    100% { transform: rotate(360deg) translateX(28px) rotate(-360deg); }
}
@keyframes analyst-pulse-ring {
    0%, 100% { box-shadow: 0 0 0 0 rgba(21, 101, 192, 0.35); }
    50% { box-shadow: 0 0 0 12px rgba(21, 101, 192, 0); }
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
.analyst-orb-1 { background: linear-gradient(135deg, #1565c0, #42a5f5); animation-duration: 3.8s; }
.analyst-orb-2 { background: linear-gradient(135deg, #00838f, #4dd0e1); animation-duration: 5.2s; animation-direction: reverse; }
.analyst-orb-3 { background: linear-gradient(135deg, #5c6bc0, #9fa8da); animation-duration: 4.2s; }
.analyst-loading-card {
    position: relative;
    z-index: 1;
    text-align: center;
    padding: 20px 28px 22px 28px;
    border-radius: 16px;
    background: linear-gradient(145deg, #ffffff 0%, #f0f7ff 50%, #e8f4fc 100%);
    border: 1px solid rgba(21, 101, 192, 0.18);
    box-shadow: 0 8px 32px rgba(13, 71, 161, 0.12);
    animation: analyst-float-y 3.2s ease-in-out infinite, analyst-pulse-ring 2.4s ease-out infinite;
    max-width: 520px;
    margin: 0 auto;
}
.analyst-loading-kicker {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #00838f;
    margin-bottom: 8px;
}
.analyst-loading-title {
    font-size: 17px;
    font-weight: 600;
    color: #0d47a1;
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
    background: linear-gradient(180deg, #1565c0, #00838f);
    animation: analyst-dot-bounce 1.05s ease-in-out infinite;
}
.analyst-dots span:nth-child(2) { animation-delay: 0.15s; }
.analyst-dots span:nth-child(3) { animation-delay: 0.3s; }
.analyst-hint {
    font-size: 13px;
    color: #5c6b7a;
    line-height: 1.45;
    padding-top: 4px;
    border-top: 1px solid rgba(0, 131, 143, 0.12);
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.9) 20%, rgba(255,255,255,0.9) 80%, transparent);
    background-size: 200% 100%;
    animation: analyst-shimmer 4s ease-in-out infinite;
}
@keyframes welcome-hero-in {
    from { opacity: 0; transform: translateY(18px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes welcome-line-in {
    from { opacity: 0; transform: translateX(-8px); }
    to { opacity: 1; transform: translateX(0); }
}
@keyframes welcome-gradient-shift {
    0%, 100% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
}
.welcome-hero-shell {
    border-radius: 18px;
    padding: 22px 26px 20px 26px;
    margin-bottom: 18px;
    background: linear-gradient(135deg, #f8fbff 0%, #eef6ff 45%, #e8f8fa 100%);
    background-size: 200% 200%;
    animation: welcome-hero-in 0.75s ease-out both, welcome-gradient-shift 10s ease-in-out infinite;
    border: 1px solid rgba(21, 101, 192, 0.14);
    box-shadow: 0 10px 40px rgba(13, 71, 161, 0.08);
}
.welcome-hero-greeting {
    font-size: 15px;
    font-weight: 600;
    color: #00838f;
    margin: 0 0 6px 0;
    animation: welcome-line-in 0.55s ease-out 0.1s both;
}
.welcome-hero-title {
    font-size: 22px;
    font-weight: 800;
    line-height: 1.3;
    margin: 0 0 10px 0;
    background: linear-gradient(110deg, #0d47a1 0%, #1565c0 40%, #006064 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    animation: welcome-line-in 0.6s ease-out 0.22s both;
}
.welcome-hero-body {
    font-size: 14px;
    color: #4a5568;
    line-height: 1.55;
    margin: 0 0 14px 0;
    max-width: 720px;
    animation: welcome-line-in 0.6s ease-out 0.35s both;
}
.welcome-hero-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    animation: welcome-line-in 0.55s ease-out 0.48s both;
}
.welcome-chip {
    font-size: 12px;
    font-weight: 600;
    padding: 7px 14px;
    border-radius: 999px;
    background: rgba(255,255,255,0.92);
    border: 1px solid rgba(21, 101, 192, 0.16);
    color: #1565c0;
    transition: transform 0.2s ease, box-shadow 0.22s ease, border-color 0.2s ease;
    display: inline-block;
}
.welcome-chip:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 18px rgba(21, 101, 192, 0.14);
    border-color: rgba(21, 101, 192, 0.32);
}
@keyframes welcome-back-in {
    from { opacity: 0; transform: translateY(-6px); }
    to { opacity: 1; transform: translateY(0); }
}
.welcome-back-strip {
    animation: welcome-back-in 0.5s ease-out both;
    border-radius: 14px;
    padding: 12px 18px;
    margin-bottom: 14px;
    background: linear-gradient(95deg, rgba(0,131,143,0.1), rgba(21,101,192,0.09), rgba(255,255,255,0.5));
    border: 1px solid rgba(0, 131, 143, 0.2);
    font-size: 14px;
    color: #1a365d;
    font-weight: 500;
    box-shadow: 0 4px 20px rgba(13, 71, 161, 0.06);
}
@keyframes chart-reveal-pop {
    0% { opacity: 0; transform: translateY(14px) scale(0.985); filter: blur(2px); }
    100% { opacity: 1; transform: translateY(0) scale(1); filter: blur(0); }
}
div[data-testid="stVegaLiteChart"],
div[data-testid="stArrowVegaLiteChart"],
div[data-testid*="VegaLiteChart"],
div[data-testid="stPlotlyChart"] {
    animation: chart-reveal-pop 0.75s cubic-bezier(0.22, 1, 0.36, 1) both !important;
}
div[data-testid="stDataFrame"] {
    animation: chart-reveal-pop 0.55s ease-out both !important;
}
.persona-hints {
    margin: 6px 0 4px 0;
    padding: 14px 16px;
    border-radius: 14px;
    background: linear-gradient(145deg, rgba(255,255,255,0.95) 0%, #f1f5f9 100%);
    border: 1px solid rgba(21, 101, 192, 0.1);
    font-size: 13px;
    line-height: 1.5;
    color: #475569;
    box-shadow: 0 2px 14px rgba(15, 23, 42, 0.04);
}
.persona-hint-row {
    display: flex;
    gap: 10px;
    align-items: baseline;
    margin: 2px 0;
    padding: 8px 10px;
    border-radius: 10px;
    transition: background 0.2s ease, border-color 0.2s ease, transform 0.18s ease;
    border: 1px solid transparent;
}
.persona-hint-row:hover {
    background: rgba(255,255,255,0.75);
    border-color: rgba(21, 101, 192, 0.1);
}
.persona-hint-row + .persona-hint-row {
    margin-top: 2px;
}
.persona-hint-active {
    background: linear-gradient(90deg, rgba(21, 101, 192, 0.1), rgba(0, 131, 143, 0.06)) !important;
    border: 1px solid rgba(21, 101, 192, 0.2) !important;
    box-shadow: 0 2px 12px rgba(21, 101, 192, 0.08);
}
.persona-hint-name {
    flex: 0 0 92px;
    font-weight: 800;
    color: #0d47a1;
    font-size: 11px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
}
.persona-hint-text { flex: 1; min-width: 0; }
.ei-empty-state {
    text-align: center;
    padding: 48px 24px 52px 24px;
    color: #64748b;
    background: linear-gradient(165deg, rgba(255,255,255,0.92) 0%, #f1f5f9 55%, #e8eef7 100%);
    border-radius: 18px;
    border: 1px solid rgba(255,255,255,0.95);
    box-shadow: 0 8px 36px rgba(15, 23, 42, 0.07);
    animation: ei-empty-in 0.65s cubic-bezier(0.22, 1, 0.36, 1) both;
}
.ei-empty-state .ei-empty-icon {
    font-size: 48px;
    margin-bottom: 14px;
    line-height: 1;
    filter: drop-shadow(0 4px 12px rgba(21, 101, 192, 0.15));
}
.ei-empty-state .ei-empty-title {
    font-size: 18px;
    font-weight: 800;
    margin-bottom: 10px;
    color: #0f172a;
    letter-spacing: -0.02em;
}
.ei-empty-state .ei-empty-copy {
    font-size: 13px;
    line-height: 1.6;
    max-width: 360px;
    margin: 0 auto;
}
details summary {
    font-weight: 600 !important;
    color: var(--ei-accent) !important;
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
    from { opacity: 0; transform: translateY(12px); }
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
            f'<div class="{cls}"><span class="persona-hint-name">{html.escape(name)}</span>'
            f'<span class="persona-hint-text">{hint}</span></div>'
        )
    parts.append("</div>")
    st.markdown("".join(parts), unsafe_allow_html=True)


def _render_narrative_card(text: str) -> None:
    """LLM summary with product-style framing (not default st.info)."""
    safe = html.escape(text.strip()).replace("\n", "<br/>")
    st.markdown(
        '<div class="ei-narrative-card">'
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
    num_cols = [c for c in df.select_dtypes(include="number").columns if c != date_col]
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


def render_chart(df: pd.DataFrame) -> None:
    if df.empty or "Error" in df.columns or len(df.columns) < 2:
        return
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
    num_cols = list(df.select_dtypes(include="number").columns)
    if not num_cols:
        st.dataframe(df, use_container_width=True)
        return
    if date_col and num_cols:
        try:
            plot_df = df[[date_col] + num_cols].copy()
            plot_df[date_col] = pd.to_datetime(plot_df[date_col], errors="coerce")
            plot_df = plot_df.sort_values(date_col).set_index(date_col)
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
    if cat_cols and len(num_cols) == 1:
        try:
            plot_df = df[[cat_cols[0], num_cols[0]]].copy()
            plot_df = plot_df.set_index(cat_cols[0])
            st.bar_chart(plot_df, use_container_width=True)
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


def _render_suggestion_chips(questions: list[str], key_prefix: str) -> None:
    cols = st.columns(2)
    for i, q in enumerate(questions):
        label = q[:78] + ("…" if len(q) > 78 else "")
        with cols[i % 2]:
            if st.button(label, key=f"{key_prefix}_{i}", use_container_width=True):
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

def _glass_panel():
    """Frosted panel wrapper; uses container(border=…) when the runtime supports it."""
    if "border" in inspect.signature(st.container).parameters:
        return st.container(border=True)
    return st.container()


# ══════════════════════════════════════════════════════════════════════════
#  LAYOUT
# ══════════════════════════════════════════════════════════════════════════

if session is None:
    st.error("Run this app inside **Streamlit in Snowflake** (active Snowpark session required).")
    st.stop()

st.markdown(
    """
<div class="ei-app-header">
  <div class="ei-app-header-inner">
    <div class="main-header">US Economic Intelligence</div>
    <div class="sub-header">Natural language over unemployment, retail, rates, industrial production,
    <strong>CPI</strong>, <strong>GDP</strong>, the <strong>macro wide</strong> join panel, and corporate
    ownership — Cortex Analyst + verified SQL fallbacks.</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

_render_welcome_hero()
_render_welcome_back_strip()

with _glass_panel():
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Logical tables", "8 domains", help="Granular V_* + macro_wide + company graph")
    with m2:
        st.metric("Join panel", "ECONOMIC_INDICATORS_WIDE", help="Shared timeline for multi-metric compares")
    with m3:
        st.metric("Prices & output", "CPI + GDP views", help="V_CPI monthly, V_GDP quarterly")
    with m4:
        st.metric("Company graph", "Parent → subsidiary", help="V_COMPANY_RELATIONSHIPS")

with _glass_panel():
    st.radio(
        "Narrative perspective",
        list(PERSONAS.keys()),
        horizontal=True,
        key="persona_perspective",
        help="Cortex COMPLETE uses a different voice per persona. Descriptions are shown in the panel directly below.",
    )
    _render_persona_hints(st.session_state.get("persona_perspective") or list(PERSONAS.keys())[0])

st.divider()

query_progress = st.empty()

with _glass_panel():
    left, right = st.columns([1, 1], gap="large")

    with left:
        st.markdown(
            '<div class="section-label">Ask a question</div>',
            unsafe_allow_html=True,
        )
        chat_container = st.container(height=380)
        with chat_container:
            for msg in st.session_state.messages:
                if msg["role"] == "user":
                    st.markdown(
                        f'<div class="user-bubble">{html.escape(msg["content"])}</div>'
                        '<div class="clearfix"></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="ai-bubble">{html.escape(msg["content"])}</div>'
                        '<div class="clearfix"></div>',
                        unsafe_allow_html=True,
                    )

        with st.form("chat_form", clear_on_submit=True):
            user_input = st.text_input(
                "Your question",
                placeholder="e.g. How did unemployment change in 2020?",
                label_visibility="collapsed",
            )
            submitted = st.form_submit_button("Ask →", use_container_width=True)

        st.markdown(
            '<div class="section-label" style="margin-top:12px">Suggested questions</div>',
            unsafe_allow_html=True,
        )
        tab_core, tab_pg, tab_wide, tab_co = st.tabs(
            ["Core macro", "CPI & GDP", "Multi-metric (wide)", "Companies"]
        )
        with tab_core:
            _render_suggestion_chips(SUGGESTED_CORE, "sug_core")
        with tab_pg:
            _render_suggestion_chips(SUGGESTED_PRICES_GDP, "sug_pg")
        with tab_wide:
            _render_suggestion_chips(SUGGESTED_MACRO_WIDE, "sug_wide")
        with tab_co:
            _render_suggestion_chips(SUGGESTED_COMPANIES, "sug_co")

        if st.session_state.last_followups:
            st.markdown(
                '<div class="section-label" style="margin-top:12px">Follow-up questions</div>',
                unsafe_allow_html=True,
            )
            fcols = st.columns(1)
            for i, fq in enumerate(st.session_state.last_followups):
                if fcols[0].button(fq, key=f"fu_{i}", use_container_width=True):
                    st.session_state.pending_question = fq
                    st.rerun()

    with right:
        st.markdown(
            '<div class="section-label">Results</div>',
            unsafe_allow_html=True,
        )
        if st.session_state.last_df is not None:
            with st.container():
                if st.session_state.last_interpretation:
                    _render_narrative_card(st.session_state.last_interpretation)
                _ldf = st.session_state.last_df
                _dc, _vc = _time_series_cols(_ldf)
                if _dc and _vc:
                    _ci = _correlation_insight_line(_ldf, _dc, _vc)
                    if _ci:
                        st.markdown(_ci)
                render_chart(_ldf)
                with st.expander("View raw data table"):
                    st.dataframe(_ldf, use_container_width=True)
                if st.session_state.last_sql:
                    st.markdown(
                        '<div class="section-label" style="margin-top:12px">'
                        "Generated SQL — transparency panel</div>",
                        unsafe_allow_html=True,
                    )
                    safe_sql = html.escape(st.session_state.last_sql)
                    st.markdown(
                        f'<div class="sql-box">{safe_sql}</div>',
                        unsafe_allow_html=True,
                    )
                _pdf = build_brief_pdf_bytes(
                    st.session_state.last_user_question or "US Economic Intelligence — analyst brief",
                    st.session_state.last_interpretation or "",
                    st.session_state.last_sql or "",
                    _ldf,
                )
                if _pdf:
                    st.download_button(
                        label="Export boardroom brief (PDF)",
                        data=_pdf,
                        file_name=f"economic_brief_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                        mime="application/pdf",
                        key="download_brief_pdf",
                    )
        else:
            st.markdown(
                """
<div class="ei-empty-state">
  <div class="ei-empty-icon" aria-hidden="true">📈</div>
  <div class="ei-empty-title">Ask a question to get started</div>
  <div class="ei-empty-copy">
    Try <strong>CPI &amp; GDP</strong> or <strong>Multi-metric (wide)</strong> for multi-series joins,
    or type your own. Results, charts, and SQL transparency appear here.
  </div>
</div>
""",
                unsafe_allow_html=True,
            )

# ══════════════════════════════════════════════════════════════════════════
#  PROCESS QUESTION
# ══════════════════════════════════════════════════════════════════════════

question: str | None = None
if st.session_state.pending_question:
    question = st.session_state.pending_question
    st.session_state.pending_question = None
elif submitted and user_input and user_input.strip():
    question = user_input.strip()

if question:
    question_for_model = _autocorrect_question(question)
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
                interpretation = None
                for block in response.get("message", {}).get("content", []):
                    if block.get("type") == "sql":
                        sql = block.get("statement") or block.get("sql")
                    elif block.get("type") == "text":
                        interpretation = block.get("text", "")

                if sql:
                    _progress_step("Running generated SQL in your Snowflake warehouse…")
                    df = run_sql(sql)
                    if "Error" in df.columns:
                        fallback_sql = _fallback_sql_for_question(question)
                        if fallback_sql:
                            _progress_step("Analyst SQL had an error — running a verified fallback query…")
                            df = run_sql(fallback_sql)
                            sql = fallback_sql + "\n\n-- Note: Cortex Analyst SQL failed; ran verified fallback query."
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
                    elif interpretation:
                        st.session_state.last_interpretation = interpretation
                    else:
                        st.session_state.last_interpretation = None
                    _progress_step("Generating smart follow-up suggestions…")
                    st.session_state.last_followups = generate_followups(question_for_model, df)
                    reply = (
                        narrative
                        or interpretation
                        or "Here are the results — see the chart and data."
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
                        st.session_state.last_sql = (
                            fallback_sql
                            + "\n\n-- Note: Cortex Analyst did not return SQL; ran verified fallback query."
                        )
                        st.session_state.last_df = df
                        _progress_step("Summarizing fallback results…")
                        narrative = generate_narrative(
                            question,
                            df,
                            st.session_state.get("persona_perspective") or "Executive",
                        )
                        digest = _result_digest(df)
                        st.session_state.last_interpretation = narrative or interpretation
                        _progress_step("Generating smart follow-up suggestions…")
                        st.session_state.last_followups = generate_followups(question_for_model, df)
                        fallback_msg = st.session_state.last_interpretation or "Here are the fallback results."
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
                            interpretation
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
            st.session_state.last_followups = []
            query_progress.empty()

    st.rerun()
