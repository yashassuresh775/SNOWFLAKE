"""
Economic Intelligence — Streamlit in Snowflake (Cortex Analyst + Cortex COMPLETE).
Hackathon AI-02: Conversational BI with semantic YAML on HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
SEMANTIC_MODEL_FILE = os.environ.get(
    "SEMANTIC_MODEL_FILE",
    "@HACKATHON.DATA.SEMANTIC_MODELS/economic_model.yaml",
)
CORTEX_COMPLETE_MODEL = os.environ.get("CORTEX_COMPLETE_MODEL", "mistral-large2")
MOCK_MODE = os.environ.get("STREAMLIT_MOCK_CORTEX", "").lower() in ("1", "true", "yes")

COLORS = {
    "crisis": "#E24B4A",
    "recovery": "#EF9F27",
    "normal": "#639922",
    "default": "#378ADD",
    "forecast": "#7F77DD",
}

# -----------------------------------------------------------------------------
# Session (Streamlit in Snowflake)
# -----------------------------------------------------------------------------
def get_snowpark_session():
    try:
        from snowflake.snowpark.context import get_active_session

        return get_active_session()
    except Exception:
        return None


def get_connection_auth(session):
    """Return (host, token) for Cortex Analyst REST API."""
    conn = session.connection
    rest = getattr(conn, "_rest", None) or getattr(conn, "rest", None)
    token = None
    if rest:
        token = getattr(rest, "_token_or_authenticator", None) or getattr(rest, "token", None)
    host = getattr(conn, "host", None)
    return host, token


# -----------------------------------------------------------------------------
# Cortex COMPLETE (narratives, follow-ups, clarifiers, executive brief)
# -----------------------------------------------------------------------------
def cortex_complete(session, prompt: str) -> str:
    tag = f"BQ_{uuid.uuid4().hex[:12]}"
    sql_text = (
        f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{CORTEX_COMPLETE_MODEL}', ${tag}${prompt}${tag}$)"
    )
    try:
        rows = session.sql(sql_text).collect()
        if rows and rows[0][0] is not None:
            return str(rows[0][0]).strip()
    except Exception as ex:  # noqa: BLE001
        return f"[Cortex COMPLETE error: {ex}]"
    return ""


# -----------------------------------------------------------------------------
# Cortex Analyst REST
# -----------------------------------------------------------------------------
def call_cortex_analyst(session, question: str) -> dict[str, Any]:
    if MOCK_MODE:
        return _mock_analyst_response(question)
    host, token = get_connection_auth(session)
    if not host or not token:
        return {"error": "Could not read Snowflake REST host/token from session."}
    url = f"https://{host}/api/v2/cortex/analyst/message"
    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
    }
    body = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": question}]}
        ],
        "semantic_model_file": SEMANTIC_MODEL_FILE,
    }
    try:
        r = requests.post(url, headers=headers, json=body, timeout=180)
        try:
            payload = r.json()
        except Exception:  # noqa: BLE001
            return {"error": r.text or f"HTTP {r.status_code}"}
        if r.status_code != 200:
            return {
                "error": payload.get("message", r.text),
                "raw": payload,
            }
        return payload
    except Exception as ex:  # noqa: BLE001
        return {"error": str(ex)}


def _mock_analyst_response(question: str) -> dict[str, Any]:
    """Offline UI test — returns canned SQL against HACKATHON wide table."""
    q = question.lower()
    sql = """SELECT OBSERVATION_DATE, UNEMPLOYMENT_RATE
    FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
    WHERE OBSERVATION_DATE >= '2020-01-01' AND UNEMPLOYMENT_RATE IS NOT NULL
    ORDER BY 1"""
    if "cpi" in q or "inflation" in q:
        sql = """SELECT OBSERVATION_DATE, CPI FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
        WHERE OBSERVATION_DATE >= '2018-01-01' AND CPI IS NOT NULL ORDER BY 1"""
    if "fed" in q or "interest" in q:
        sql = """SELECT OBSERVATION_DATE, FED_FUNDS_RATE FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
        WHERE FED_FUNDS_RATE IS NOT NULL ORDER BY 1 LIMIT 200"""
    return {
        "message": {
            "role": "analyst",
            "content": [
                {
                    "type": "text",
                    "text": "Interpreted your question and generated SQL (mock mode).",
                },
                {
                    "type": "sql",
                    "statement": sql,
                    "confidence": {"verified_query_used": None},
                },
            ],
        },
        "warnings": [],
    }


def parse_analyst_payload(data: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "text": "",
        "statement": None,
        "suggestions": None,
        "confidence": {},
        "warnings": data.get("warnings") or [],
        "error": data.get("error") or data.get("message") if isinstance(data.get("message"), str) else None,
    }
    if data.get("error") and isinstance(data.get("error"), str):
        out["error"] = data["error"]
        return out
    msg = data.get("message") or {}
    for block in msg.get("content") or []:
        btype = block.get("type")
        if btype == "text":
            out["text"] += block.get("text") or ""
        elif btype == "sql":
            out["statement"] = block.get("statement")
            out["confidence"] = block.get("confidence") or {}
        elif btype == "suggestion":
            out["suggestions"] = block.get("suggestions") or block.get("text")
    if isinstance(data.get("message"), str):
        out["error"] = data["message"]
    return out


def confidence_score(conf: dict[str, Any], warnings: list) -> float:
    base = 0.78
    if conf.get("verified_query_used"):
        base = 0.94
    elif conf:
        base = 0.82
    base -= 0.04 * min(len(warnings or []), 3)
    return max(0.35, min(0.99, base))


def run_sql(session, sql: str) -> pd.DataFrame:
    try:
        return session.sql(sql).to_pandas()
    except Exception as ex:  # noqa: BLE001
        st.session_state.last_sql_error = str(ex)
        return pd.DataFrame()


# -----------------------------------------------------------------------------
# Charts (FEATURE 05)
# -----------------------------------------------------------------------------
def _detect_date_col(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        cl = c.lower()
        if "date" in cl or df[c].dtype == "datetime64[ns]":
            return c
    return None


def auto_chart(df: pd.DataFrame, headline: str) -> None:
    st.caption(headline)
    if df is None or df.empty:
        st.info("No rows returned for visualization.")
        return
    num_cols = df.select_dtypes(include=["number", "float", "int"]).columns.tolist()
    date_col = _detect_date_col(df)

    if len(df) == 1 and len(num_cols) == 1:
        st.metric(
            label=num_cols[0].replace("_", " ").title(),
            value=f"{df[num_cols[0]].iloc[0]:,.4f}",
        )
        return

    if date_col and len(df) > 2 and num_cols:
        dfp = df.copy()
        dfp[date_col] = pd.to_datetime(dfp[date_col], errors="coerce")
        ycols = num_cols[:3]
        fig = px.line(
            dfp,
            x=date_col,
            y=ycols,
            color_discrete_sequence=[COLORS["default"], COLORS["crisis"], COLORS["recovery"]],
        )
        fig.add_vrect(
            x0="2008-01-01",
            x1="2009-12-31",
            fillcolor=COLORS["crisis"],
            opacity=0.08,
            layer="below",
            line_width=0,
        )
        fig.add_vrect(
            x0="2020-02-01",
            x1="2021-06-30",
            fillcolor=COLORS["crisis"],
            opacity=0.08,
            layer="below",
            line_width=0,
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=28, b=0),
            height=320,
            paper_bgcolor="#1a1d24",
            plot_bgcolor="#0e1117",
            font=dict(color="#fafafa"),
        )
        st.plotly_chart(fig, use_container_width=True)
        return

    if len(df) <= 16 and not date_col and len(num_cols) >= 1:
        xcol = df.columns[0]
        fig = px.bar(
            df,
            x=xcol,
            y=num_cols[0],
            color_discrete_sequence=[COLORS["default"]],
        )
        fig.update_layout(height=300, paper_bgcolor="#1a1d24", plot_bgcolor="#0e1117")
        st.plotly_chart(fig, use_container_width=True)
        return

    st.dataframe(df, use_container_width=True)


def chart_headline(df: pd.DataFrame, question: str) -> str:
    dc = _detect_date_col(df) or "date"
    try:
        if not df.empty and dc in df.columns:
            dmin = pd.to_datetime(df[dc], errors="coerce").min()
            dmax = pd.to_datetime(df[dc], errors="coerce").max()
            span = f"{dmin.date()}–{dmax.date()}" if pd.notna(dmin) else ""
        else:
            span = ""
    except Exception:  # noqa: BLE001
        span = ""
    topic = question[:48] + ("…" if len(question) > 48 else "")
    return f"{topic} · {span}"


def followup_questions(session, question: str, df: pd.DataFrame) -> list[str]:
    sample = df.head(5).to_string() if not df.empty else "(empty result)"
    prompt = f"""Given this result for '{question}':
{sample}
Suggest 3 short follow-up questions a finance analyst would ask next.
Return ONLY a JSON array of 3 strings. No explanation or markdown."""
    raw = cortex_complete(session, prompt)
    try:
        m = re.search(r"\[.*\]", raw, re.S)
        if m:
            arr = json.loads(m.group(0))
            if isinstance(arr, list) and len(arr) >= 3:
                return [str(x) for x in arr[:3]]
    except Exception:  # noqa: BLE001
        pass
    return [
        "How does this compare to 2008?",
        "Which period was highest?",
        "Show year-over-year change?",
    ]


def insight_narrative(session, question: str, df: pd.DataFrame) -> str:
    sample = df.head(8).to_string() if not df.empty else "(no rows)"
    prompt = f"""You are a Bloomberg economic analyst. In at most 2 sentences, interpret this data in context of: '{question}'.
Data sample:
{sample}
No preamble. Be specific with numbers if visible."""
    return cortex_complete(session, prompt)


def clarifying_question(session, question: str, reason: str) -> str:
    prompt = f"""A finance BI tool could not run a query for: '{question}'
Reason: {reason}
Write ONE specific clarifying question the user should answer to get a chart. Return only the question."""
    return cortex_complete(session, prompt)


def executive_brief(session) -> str:
    lines = []
    for m in st.session_state.messages:
        if (
            m.get("role") == "assistant"
            and m.get("narrative")
            and not m.get("needs_clarification")
        ):
            lines.append(f"Q: {m.get('q', '')}\nInsight: {m.get('narrative', '')}")
    history = "\n".join(lines)
    prompt = f"""You are a CFO's senior analyst. Synthesize these economic findings into a professional 3-paragraph executive memo:
{history}
Structure: Current economic situation | Key trends identified | Strategic recommendations"""
    return cortex_complete(session, prompt)


def load_header_metrics(session) -> tuple[str, str, str]:
    """Latest unemployment, CPI, Fed funds — best-effort from wide table."""
    try:
        u = session.sql(
            """
            SELECT unemployment_rate AS v FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
            WHERE unemployment_rate IS NOT NULL ORDER BY observation_date DESC LIMIT 1
            """
        ).to_pandas()
        ur = f"{float(u.iloc[0, 0]):.1f}%" if not u.empty else "—"
    except Exception:  # noqa: BLE001
        ur = "—"
    try:
        c = session.sql(
            """
            SELECT cpi AS v FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
            WHERE cpi IS NOT NULL ORDER BY observation_date DESC LIMIT 1
            """
        ).to_pandas()
        # CPI index — show as index not %
        cpi = f"{float(c.iloc[0, 0]):.1f}" if not c.empty else "—"
    except Exception:  # noqa: BLE001
        cpi = "—"
    try:
        f = session.sql(
            """
            SELECT fed_funds_rate AS v FROM HACKATHON.DATA.ECONOMIC_INDICATORS_WIDE
            WHERE fed_funds_rate IS NOT NULL ORDER BY observation_date DESC LIMIT 1
            """
        ).to_pandas()
        fed = f"{float(f.iloc[0, 0]):.2f}%" if not f.empty else "—"
    except Exception:  # noqa: BLE001
        fed = "—"
    return ur, cpi, fed


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------
def inject_css():
    st.markdown(
        """
        <style>
        .ei-badge { background:#1e3a2f;color:#86efac;padding:4px 12px;border-radius:999px;font-size:13px;display:inline-block;}
        .ei-title { font-size: 1.75rem; font-weight: 700; letter-spacing: -0.02em; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main():
    st.set_page_config(
        page_title="Economic Intelligence",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_css()

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "query_log" not in st.session_state:
        st.session_state.query_log = []
    if "auto_query" not in st.session_state:
        st.session_state.auto_query = None
    if "time_filter" not in st.session_state:
        st.session_state.time_filter = "2020 – present"

    session = get_snowpark_session()
    if session is None and not MOCK_MODE:
        st.error("Run this app inside **Streamlit in Snowflake** (active Snowpark session), or set env `STREAMLIT_MOCK_CORTEX=true` for UI-only demo.")
        st.stop()

    if session is None and MOCK_MODE:
        st.warning("MOCK MODE — Cortex Analyst API not called; SQL may still fail without a session.")

    # --- Sidebar ---
    st.sidebar.markdown("### TOPICS")
    topics = [
        "Unemployment",
        "Inflation (CPI)",
        "Interest rates",
        "GDP",
        "Retail sales",
    ]
    cols = st.sidebar.columns(2)
    for i, t in enumerate(topics):
        with cols[i % 2]:
            if st.button(t, key=f"topic_{t}"):
                st.session_state.auto_query = f"Tell me about {t.lower()} in the US economy."

    st.sidebar.markdown("### TIME RANGE")
    st.session_state.time_filter = st.sidebar.radio(
        "Range",
        ["2020 – present", "5 years", "10 years", "Custom…"],
        label_visibility="collapsed",
    )

    st.sidebar.markdown("### SESSION HISTORY")
    hist = [m for m in st.session_state.messages if m.get("role") == "user"][-5:]
    for h in reversed(hist):
        q = h.get("q", "")[:42]
        if st.sidebar.button(q + ("…" if len(h.get("q", "")) > 42 else ""), key=f"h_{hash(q)}"):
            st.session_state.auto_query = h.get("q")

    passing = sum(1 for q in st.session_state.query_log if q.get("status") == "pass")
    total = len(st.session_state.query_log)
    acc_label = f"{passing}/{total} queries passing" if total else "0/0 queries passing"
    st.sidebar.metric("Query accuracy", acc_label.split(" passing")[0], delta=None)

    st.sidebar.download_button(
        label="Download query log (CSV)",
        data=_query_log_csv(),
        file_name="query_log.csv",
        mime="text/csv",
    )

    if len(st.session_state.messages) >= 3:
        if st.sidebar.button("Summarize session as executive brief"):
            if session:
                st.session_state.exec_brief = executive_brief(session)
            else:
                st.session_state.exec_brief = "(Connect to Snowflake for executive brief.)"
    if st.session_state.get("exec_brief"):
        st.sidebar.markdown("---")
        st.sidebar.markdown(st.session_state.exec_brief)

    # --- Header row ---
    h1, h2, h3 = st.columns([2, 2, 2])
    with h1:
        st.markdown('<span class="ei-title">Economic Intelligence</span>', unsafe_allow_html=True)
    with h2:
        st.radio(
            "Session",
            ["Today", "Yesterday", "Archive"],
            horizontal=True,
            label_visibility="collapsed",
        )
    with h3:
        st.markdown(
            f'<div style="text-align:right"><span class="ei-badge">{acc_label} ✓</span></div>',
            unsafe_allow_html=True,
        )

    # --- Metric cards ---
    m1, m2, m3 = st.columns(3)
    if session:
        ur, cpi, fed = load_header_metrics(session)
    else:
        ur, cpi, fed = "3.8%", "—", "—"
    m1.metric("Unemployment", ur)
    m2.metric("Inflation (CPI index)", cpi)
    m3.metric("Fed funds (latest)", fed)

    st.markdown("---")

    # --- Chat thread (render history) ---
    for turn in st.session_state.messages:
        if turn.get("role") == "user":
            with st.chat_message("user"):
                st.write(turn.get("q", ""))
        elif turn.get("role") == "assistant":
            with st.chat_message("assistant"):
                hl = turn.get("headline") or ""
                if turn.get("needs_clarification"):
                    st.warning(turn.get("narrative") or "I need a bit more detail to answer.")
                    cid = turn.get("_id", "x")
                    refined = st.text_input(
                        "Your clarification:",
                        key=f"clarify_{cid}",
                    )
                    if refined and st.button("Try again", key=f"try_{cid}"):
                        st.session_state.auto_query = f"{turn.get('q', '')} — {refined}"
                        st.rerun()
                else:
                    auto_chart(turn.get("df"), hl)
                    conf = turn.get("confidence", 0.8)
                    st.progress(
                        float(conf),
                        text=f"Confidence: {float(conf) * 100:.0f}%",
                    )
                    if conf < 0.6 and turn.get("clarifier"):
                        st.warning(f"Low confidence — {turn['clarifier']}")
                    if turn.get("narrative"):
                        st.info(f"✦ Insight: {turn['narrative']}")
                    if turn.get("sql"):
                        with st.expander("View generated SQL", expanded=False):
                            st.code(turn["sql"], language="sql")
                    chips = turn.get("followups") or []
                    if chips:
                        chip_cols = st.columns(3)
                        for idx, fq in enumerate(chips[:3]):
                            with chip_cols[idx]:
                                if st.button(
                                    fq,
                                    key=f"chip_{turn.get('_id', '')}_{idx}",
                                ):
                                    st.session_state.auto_query = fq

    # --- Input ---
    prompt = st.chat_input("Ask about unemployment, CPI, GDP, interest rates…")
    q = prompt or st.session_state.auto_query
    st.session_state.auto_query = None

    if q and session:
        time_note = ""
        tf = st.session_state.time_filter
        if tf == "2020 – present":
            time_note = " Focus on dates from 2020 onward."
        elif tf == "5 years":
            time_note = " Focus on roughly the last 5 years."
        elif tf == "10 years":
            time_note = " Focus on roughly the last 10 years."
        full_q = q + time_note

        st.session_state.messages.append({"role": "user", "q": full_q})

        raw = call_cortex_analyst(session, full_q)
        parsed = parse_analyst_payload(raw)
        conf_val = confidence_score(parsed.get("confidence") or {}, parsed.get("warnings"))

        if parsed.get("error") and not parsed.get("statement"):
            st.error(str(parsed.get("error")))
            st.session_state.query_log.append(
                {"question": full_q, "status": "fail", "rows": 0, "confidence": conf_val}
            )
            st.stop()

        if parsed.get("suggestions") and not parsed.get("statement"):
            reason = "Cortex Analyst returned suggestions instead of SQL (ambiguous question)."
            cq = clarifying_question(session, full_q, reason)
            st.session_state.query_log.append(
                {"question": full_q, "status": "partial", "rows": 0, "confidence": conf_val}
            )
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "q": full_q,
                    "df": pd.DataFrame(),
                    "sql": "",
                    "narrative": cq,
                    "confidence": conf_val,
                    "headline": "",
                    "followups": [],
                    "needs_clarification": True,
                    "_id": str(uuid.uuid4()),
                }
            )
            st.rerun()

        sql = parsed.get("statement")
        if not sql:
            cq = clarifying_question(session, full_q, "No SQL statement in Analyst response.")
            st.session_state.query_log.append(
                {"question": full_q, "status": "fail", "rows": 0, "confidence": conf_val}
            )
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "q": full_q,
                    "df": pd.DataFrame(),
                    "sql": "",
                    "narrative": cq,
                    "confidence": conf_val,
                    "headline": "",
                    "followups": [],
                    "needs_clarification": True,
                    "_id": str(uuid.uuid4()),
                }
            )
            st.rerun()

        df = run_sql(session, sql)
        status = "pass" if not df.empty else "fail"
        st.session_state.query_log.append(
            {
                "question": full_q,
                "status": status,
                "rows": len(df),
                "confidence": conf_val,
            }
        )

        headline = chart_headline(df, full_q)
        narrative = insight_narrative(session, full_q, df) if not df.empty else ""
        followups = followup_questions(session, full_q, df) if not df.empty else []

        clarifier_extra = ""
        if conf_val < 0.6:
            clarifier_extra = clarifying_question(
                session, full_q, "Model confidence is low; ask user to narrow scope."
            )

        st.session_state.messages.append(
            {
                "role": "assistant",
                "q": full_q,
                "df": df,
                "sql": sql,
                "narrative": narrative,
                "confidence": conf_val,
                "headline": headline,
                "followups": followups,
                "clarifier": clarifier_extra,
                "_id": str(uuid.uuid4()),
            }
        )
        st.rerun()

    elif q and not session and MOCK_MODE:
        st.warning("MOCK: enable Snowpark session to execute queries.")


def _query_log_csv() -> str:
    import io

    buf = io.StringIO()
    buf.write("question,status,rows,confidence\n")
    for row in st.session_state.query_log:
        buf.write(
            f"\"{row.get('question', '').replace(chr(34), chr(39))}\",{row.get('status')},{row.get('rows')},{row.get('confidence')}\n"
        )
    return buf.getvalue()


if __name__ == "__main__":
    main()
