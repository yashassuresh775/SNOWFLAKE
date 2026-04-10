"""
US Economic Intelligence — Streamlit in Snowflake
Cortex Analyst (semantic YAML) + Cortex COMPLETE narratives. No Plotly (native Streamlit charts).
"""

from __future__ import annotations

import html
import json
import os
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


def _wants_subsidiary_leaderboard(q: str) -> bool:
    t = q.lower()
    if "subsidiar" not in t:
        return False
    return any(k in t for k in ("most", "top", "largest", "many", "number", "count", "biggest"))


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
    st.session_state.messages.append({"role": "user", "content": question})

    with st.spinner("Thinking..."):
        try:
            response = call_cortex_analyst(
                question,
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
                    if "Error" in df.columns and _wants_subsidiary_leaderboard(question):
                        df = run_sql(SQL_FALLBACK_MOST_SUBSIDIARIES)
                        sql = (
                            SQL_FALLBACK_MOST_SUBSIDIARIES
                            + "\n\n-- Note: Cortex Analyst SQL failed; ran verified subsidiary leaderboard query."
                        )
                    st.session_state.last_sql = sql
                    st.session_state.last_df = df
                    narrative = generate_narrative(question, df)
                    if narrative:
                        st.session_state.last_interpretation = narrative
                    elif interpretation:
                        st.session_state.last_interpretation = interpretation
                    else:
                        st.session_state.last_interpretation = None
                    reply = (
                        narrative
                        or interpretation
                        or "Here are the results — see the chart and data."
                    )
                    st.session_state.messages.append(
                        {"role": "assistant", "content": reply}
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
        except Exception as e:  # noqa: BLE001
            st.session_state.messages.append(
                {"role": "assistant", "content": f"Error: {e}"}
            )

    st.rerun()
