"""
US Economic Intelligence — Streamlit in Snowflake
Cortex Analyst (semantic YAML) + Cortex COMPLETE narratives. No Plotly (native Streamlit charts).
"""

from __future__ import annotations

import html
import difflib
import json
import os
import re
import uuid
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
    if numeric_cols:
        c = numeric_cols[0]
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


# ── page ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="US Economic Intelligence",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
<style>
.main-header {
    font-size: 28px; font-weight: 700;
    color: #1a1a2e; margin-bottom: 4px;
}
.sub-header {
    font-size: 14px; color: #666;
    margin-bottom: 24px;
}
.user-bubble {
    background: #0066cc; color: white;
    padding: 10px 16px; border-radius: 18px 18px 4px 18px;
    margin: 8px 0; max-width: 80%; float: right; clear: both;
    font-size: 14px;
}
.ai-bubble {
    background: #f0f4ff; color: #1a1a2e;
    padding: 10px 16px; border-radius: 18px 18px 18px 4px;
    margin: 8px 0; max-width: 85%; float: left; clear: both;
    font-size: 14px;
}
.sql-box {
    background: #1e1e2e; color: #79c0ff;
    padding: 12px 16px; border-radius: 8px;
    font-family: monospace; font-size: 12px;
    white-space: pre-wrap; overflow-x: auto; margin-top: 8px;
}
.clearfix { clear: both; }
.section-label {
    font-size: 11px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.08em;
    color: #888; margin-bottom: 6px;
}
</style>
""",
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


def generate_narrative(question: str, df: pd.DataFrame) -> str:
    if df.empty or "Error" in df.columns or session is None:
        return ""
    # Format top results for the prompt
    summary = df.head(8).to_string(index=False)
    prompt = f"""You are a senior business intelligence analyst presenting findings to an executive.

A user asked: "{question}"

The data shows:
{summary}

Write 2-3 sentences summarizing the key insight from this data.
Be specific — mention the top result and actual numbers.
Start directly with the insight, not with "The data shows" or "Based on the results".
Plain prose only, no bullet points."""

    try:
        # Use dollar-quoted string to avoid escaping issues
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
        (c for c in cols if "date" in c.lower() or c.lower() in ("month", "observation_date")),
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


SUGGESTED = [
    "What is the US unemployment trend from 2020 to 2024?",
    "Which company owns the most subsidiaries?",
    "How have Treasury bill rates changed since 2020?",
    "What are the top retail sales categories in 2023?",
    "How did aerospace industrial production trend from 2019 to 2023?",
    "What subsidiaries does Kroger own?",
    "How did unemployment differ between men and women in 2022?",
    "Which industrial sectors had the highest production in 2023?",
]

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

# ══════════════════════════════════════════════════════════════════════════
#  LAYOUT
# ══════════════════════════════════════════════════════════════════════════

if session is None:
    st.error("Run this app inside **Streamlit in Snowflake** (active Snowpark session required).")
    st.stop()

st.markdown(
    '<div class="main-header">US Economic Intelligence</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="sub-header">Ask about US unemployment, retail sales, interest rates, '
    "industrial production, or corporate ownership — powered by Cortex Analyst</div>",
    unsafe_allow_html=True,
)

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.metric("Data sources", "Finance & Economics + company graph")
with m2:
    st.metric("Semantic tables", "5 logical models")
with m3:
    st.metric("Typical range", "Multi-decade series")
with m4:
    st.metric("Company edges", "Parent → subsidiary")

st.divider()

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
    cols = st.columns(2)
    for i, q in enumerate(SUGGESTED[:6]):
        with cols[i % 2]:
            if st.button(q[:80] + ("…" if len(q) > 80 else ""), key=f"sug_{i}", use_container_width=True):
                st.session_state.pending_question = q
                st.rerun()

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
        if st.session_state.last_interpretation:
            st.info(st.session_state.last_interpretation)
        render_chart(st.session_state.last_df)
        with st.expander("View raw data table"):
            st.dataframe(st.session_state.last_df, use_container_width=True)
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
    else:
        st.markdown(
            """
<div style="text-align:center; padding:60px 20px; color:#888;">
<div style="font-size:40px; margin-bottom:16px">📊</div>
<div style="font-size:16px; font-weight:500; margin-bottom:8px">
                    Ask a question to get started
</div>
<div style="font-size:13px">
                    Use suggested questions or type your own.
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
    st.session_state.messages.append({"role": "user", "content": question})

    with st.spinner("Thinking..."):
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
            else:
                sql = None
                interpretation = None
                for block in response.get("message", {}).get("content", []):
                    if block.get("type") == "sql":
                        sql = block.get("statement") or block.get("sql")
                    elif block.get("type") == "text":
                        interpretation = block.get("text", "")

                if sql:
                    df = run_sql(sql)
                    if "Error" in df.columns:
                        fallback_sql = _fallback_sql_for_question(question)
                        if fallback_sql:
                            df = run_sql(fallback_sql)
                            sql = fallback_sql + "\n\n-- Note: Cortex Analyst SQL failed; ran verified fallback query."
                    st.session_state.last_sql = sql
                    st.session_state.last_df = df
                    narrative = generate_narrative(question_for_model, df)
                    digest = _result_digest(df)
                    if narrative:
                        st.session_state.last_interpretation = narrative
                    elif interpretation:
                        st.session_state.last_interpretation = interpretation
                    else:
                        st.session_state.last_interpretation = None
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
                else:
                    fallback_sql = _fallback_sql_for_question(question)
                    if fallback_sql:
                        df = run_sql(fallback_sql)
                        st.session_state.last_sql = (
                            fallback_sql
                            + "\n\n-- Note: Cortex Analyst did not return SQL; ran verified fallback query."
                        )
                        st.session_state.last_df = df
                        narrative = generate_narrative(question, df)
                        digest = _result_digest(df)
                        st.session_state.last_interpretation = narrative or interpretation
                        st.session_state.last_followups = generate_followups(question_for_model, df)
                        fallback_msg = st.session_state.last_interpretation or "Here are the fallback results."
                        if digest:
                            fallback_msg = f"{fallback_msg}\n\n{digest}"
                        st.session_state.messages.append(
                            {"role": "assistant", "content": fallback_msg}
                        )
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
        except Exception as e:  # noqa: BLE001
            st.session_state.messages.append(
                {"role": "assistant", "content": f"Error: {e}"}
            )
            st.session_state.last_followups = []

    st.rerun()
