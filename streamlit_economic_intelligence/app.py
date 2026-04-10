"""
US Economic Intelligence - Streamlit in Snowflake
Cortex Analyst (semantic YAML) + Cortex COMPLETE narratives and innovation UX.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import pandas as pd
import requests
import streamlit as st
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    HAS_PLOTLY = True
except Exception:  # noqa: BLE001
    HAS_PLOTLY = False

try:
    from snowflake.snowpark.context import get_active_session

    session = get_active_session()
except Exception:  # noqa: BLE001
    session = None

SEMANTIC_MODEL_FILE = os.environ.get(
    "SEMANTIC_MODEL_FILE",
    "@HACKATHON.DATA.SEMANTIC_MODELS/semantic_model.yaml",
)
CORTEX_COMPLETE_MODEL = os.environ.get("CORTEX_COMPLETE_MODEL", "mistral-large2")
CORTEX_ANALYST_PATH = "/api/v2/cortex/analyst/message"

SQL_FALLBACK_MOST_SUBSIDIARIES = """
SELECT COMPANY_NAME AS parent_company,
       COUNT(RELATED_COMPANY_NAME) AS subsidiary_count
FROM HACKATHON.DATA.V_COMPANY_RELATIONSHIPS
GROUP BY COMPANY_NAME
ORDER BY subsidiary_count DESC
LIMIT 15
""".strip()

TOPIC_PROMPTS = {
    "Unemployment": "What is the US unemployment trend from 2020 to 2024?",
    "Retail Sales": "What are the top retail sales categories by total value in 2023?",
    "Fed Rates": "How have Treasury bill rates changed since 2020?",
    "Industrial Production": "How did aerospace industrial production trend from 2019 to 2023?",
    "Company Graph": "Which company owns the most subsidiaries?",
}

TIME_RANGE_SUFFIX = {
    "2020-present": "Use data from 2020 onward.",
    "5 years": "Use the last 5 years of data.",
    "10 years": "Use the last 10 years of data.",
    "All available": "Use the full available history.",
}

SUGGESTED = [
    "What is the US unemployment trend from 2020 to 2024?",
    "Which company owns the most subsidiaries?",
    "How have Treasury bill rates changed since 2020?",
    "What are the top retail sales categories in 2023?",
    "How did aerospace industrial production trend from 2019 to 2023?",
    "What subsidiaries does Kroger own?",
]

COLOR_NORMAL = "#378ADD"
COLOR_RECOVERY = "#EF9F27"
COLOR_CRISIS = "#E24B4A"
COLOR_HEALTHY = "#639922"

st.set_page_config(page_title="US Economic Intelligence", page_icon="📈", layout="wide")


def _connection_auth() -> tuple[str | None, str | None]:
    if session is None:
        return None, None
    conn = session.connection
    rest = getattr(conn, "_rest", None) or getattr(conn, "rest", None)
    token = None
    if rest:
        token = getattr(rest, "_token_or_authenticator", None) or getattr(rest, "token", None)
    host = getattr(conn, "host", None)
    return host, token


def _complete_text(prompt: str) -> str:
    if session is None:
        return ""
    tag = f"P_{uuid.uuid4().hex[:8]}"
    sql = (
        f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{CORTEX_COMPLETE_MODEL}', ${tag}${prompt}${tag}$) AS text"
    )
    try:
        row = session.sql(sql).collect()[0]
        return str(row[0]).strip() if row[0] is not None else ""
    except Exception:
        return ""


def call_cortex_analyst(question: str) -> dict[str, Any]:
    host, token = _connection_auth()
    if not host or not token:
        return {"error": "No Snowflake session or REST token. Run inside Streamlit in Snowflake."}

    payload = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
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
        except Exception:
            return {"error": r.text or f"HTTP {r.status_code}"}
        if r.status_code != 200:
            return {"error": body.get("message", r.text), "raw": body}
        return body
    except Exception as ex:
        return {"error": str(ex)}


def run_sql(sql: str) -> pd.DataFrame:
    if session is None:
        return pd.DataFrame({"Error": ["No active session"]})
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})


def _wants_subsidiary_leaderboard(q: str) -> bool:
    t = q.lower()
    return "subsidiar" in t and any(
        k in t for k in ("most", "top", "largest", "many", "number", "count", "biggest")
    )


def _extract_confidence(response: dict[str, Any]) -> float:
    raw = response.get("confidence", {}) if isinstance(response, dict) else {}
    score = raw.get("score", 0.85) if isinstance(raw, dict) else 0.85
    try:
        val = float(score)
    except Exception:
        val = 0.85
    return val / 100.0 if val > 1 else max(0.0, min(1.0, val))


def _get_followups(question: str, df: pd.DataFrame) -> list[str]:
    preview = "No rows" if df.empty else df.head(3).to_string(index=False)
    prompt = (
        f"Given this result for '{question}':\n{preview}\n"
        "Suggest 3 short follow-up questions an analyst would ask next. "
        "Return ONLY a JSON array of 3 strings."
    )
    raw = _complete_text(prompt)
    if not raw:
        return [
            "Show year-over-year change.",
            "Compare to pre-COVID levels.",
            "Which period was highest?",
        ]
    try:
        parsed = json.loads(raw.strip())
        if isinstance(parsed, list):
            out = [str(x).strip() for x in parsed if str(x).strip()][:3]
            if len(out) == 3:
                return out
    except Exception:
        pass
    return [
        "Show year-over-year change.",
        "Compare to pre-COVID levels.",
        "Which period was highest?",
    ]


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
    except Exception as e:
        st.sidebar.write(f"Narrative error: {e}")
        return ""


def _headline(df: pd.DataFrame, question: str) -> str:
    cols = df.columns.tolist()
    date_col = next((c for c in cols if "date" in c.lower() or c.lower() in ("month", "period")), None)
    freq = "tabular"
    if date_col:
        freq = "time series"
        try:
            d = pd.to_datetime(df[date_col], errors="coerce")
            start = d.min()
            end = d.max()
            if pd.notna(start) and pd.notna(end):
                return f"{question} | {freq} | {start.date()} to {end.date()}"
        except Exception:
            pass
    return f"{question} | {freq}"


def _auto_chart(df: pd.DataFrame) -> None:
    if df.empty or "Error" in df.columns:
        st.dataframe(df, use_container_width=True)
        return

    cols = df.columns.tolist()
    num_cols = list(df.select_dtypes(include="number").columns)
    date_col = next((c for c in cols if "date" in c.lower() or c.lower() in ("month", "period")), None)

    if not HAS_PLOTLY:
        if len(df) == 1 and len(num_cols) == 1:
            value_col = num_cols[0]
            value = float(df[value_col].iloc[0])
            st.metric(value_col.replace("_", " ").title(), f"{value:,.2f}")
            return
        if date_col and num_cols:
            plot_df = df[[date_col] + num_cols].copy()
            plot_df[date_col] = pd.to_datetime(plot_df[date_col], errors="coerce")
            plot_df = plot_df.sort_values(date_col).set_index(date_col)
            st.line_chart(plot_df, use_container_width=True)
            return
        cat_cols = [c for c in cols if c not in num_cols]
        if cat_cols and len(num_cols) >= 1:
            plot_df = df[[cat_cols[0]] + num_cols[:1]].copy().set_index(cat_cols[0])
            st.bar_chart(plot_df, use_container_width=True)
            return
        st.dataframe(df, use_container_width=True)
        st.info("Plotly is not installed in this runtime; using native Streamlit charts.")
        return

    if len(df) == 1 and len(num_cols) == 1:
        value_col = num_cols[0]
        value = float(df[value_col].iloc[0])
        st.metric(value_col.replace("_", " ").title(), f"{value:,.2f}")
        return

    if date_col and len(num_cols) >= 2 and len(df) > 3:
        plot_df = df.copy()
        plot_df[date_col] = pd.to_datetime(plot_df[date_col], errors="coerce")
        plot_df = plot_df.sort_values(date_col)
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(
                x=plot_df[date_col],
                y=plot_df[num_cols[0]],
                name=num_cols[0],
                line={"color": COLOR_NORMAL, "width": 2},
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=plot_df[date_col],
                y=plot_df[num_cols[1]],
                name=num_cols[1],
                line={"color": COLOR_RECOVERY, "width": 2},
            ),
            secondary_y=True,
        )
        fig.update_layout(height=320, margin={"l": 10, "r": 10, "t": 20, "b": 10})
        st.plotly_chart(fig, use_container_width=True)
        return

    if date_col and len(num_cols) >= 1 and len(df) > 3:
        plot_df = df[[date_col] + num_cols].copy()
        plot_df[date_col] = pd.to_datetime(plot_df[date_col], errors="coerce")
        plot_df = plot_df.sort_values(date_col)
        fig = px.line(plot_df, x=date_col, y=num_cols, color_discrete_sequence=[COLOR_NORMAL, COLOR_RECOVERY, COLOR_HEALTHY])
        fig.update_layout(height=320, margin={"l": 10, "r": 10, "t": 20, "b": 10})
        st.plotly_chart(fig, use_container_width=True)
        return

    cat_cols = [c for c in cols if c not in num_cols]
    if cat_cols and 1 <= len(num_cols) <= 2 and len(df) <= 12:
        fig = px.bar(df, x=cat_cols[0], y=num_cols, color_discrete_sequence=[COLOR_NORMAL, COLOR_RECOVERY])
        fig.update_layout(height=320, margin={"l": 10, "r": 10, "t": 20, "b": 10})
        st.plotly_chart(fig, use_container_width=True)
        return

    st.dataframe(df, use_container_width=True)


def _confidence_ui(score: float) -> None:
    score = max(0.0, min(1.0, score))
    pct = int(round(score * 100))
    color = "normal"
    if score < 0.6:
        color = "inverse"
    st.progress(score, text=f"Confidence: {pct}%")
    st.caption(f"Confidence band: {'high' if score >= 0.8 else 'medium' if score >= 0.6 else 'low'}")
    _ = color


def _export_query_log_csv() -> bytes:
    rows = []
    for item in st.session_state.query_log:
        rows.append(
            {
                "question": item.get("question", ""),
                "status": item.get("status", ""),
                "rows": item.get("rows", 0),
                "confidence": item.get("confidence", 0.0),
            }
        )
    return pd.DataFrame(rows).to_csv(index=False).encode("utf-8")


def _generate_exec_brief() -> str:
    lines = []
    for rec in st.session_state.records:
        if rec.get("narrative"):
            lines.append(f"Q: {rec['question']}\nInsight: {rec['narrative']}")
    if not lines:
        return "No completed insights yet."
    history = "\n\n".join(lines)
    prompt = (
        "You are a CFO analyst. Write a professional 3-paragraph executive memo from these findings:\n"
        f"{history}\n"
        "Format: Current situation | Key trends | Recommendations"
    )
    return _complete_text(prompt) or "Could not generate executive memo right now."


if session is None:
    st.error("Run this app inside Streamlit in Snowflake (active Snowpark session required).")
    st.stop()

# Session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "records" not in st.session_state:
    st.session_state.records = []
if "query_log" not in st.session_state:
    st.session_state.query_log = []
if "last_result" not in st.session_state:
    st.session_state.last_result = None
if "pending_question" not in st.session_state:
    st.session_state.pending_question = None
if "exec_summary" not in st.session_state:
    st.session_state.exec_summary = ""

# Header
st.title("US Economic Intelligence")
st.caption("Conversational BI powered by Cortex Analyst with confidence, follow-ups, and live quality tracking.")

# Sidebar innovations
with st.sidebar:
    st.subheader("Navigation")

    st.markdown("**Topic chips**")
    tcols = st.columns(2)
    for i, topic in enumerate(TOPIC_PROMPTS):
        if tcols[i % 2].button(topic, key=f"topic_{topic}", use_container_width=True):
            st.session_state.pending_question = TOPIC_PROMPTS[topic]
            st.rerun()

    range_choice = st.radio("Time range", list(TIME_RANGE_SUFFIX.keys()), horizontal=False)

    st.markdown("**Session history**")
    for i, rec in enumerate(st.session_state.records[-5:][::-1]):
        label = rec["question"][:55] + ("..." if len(rec["question"]) > 55 else "")
        if st.button(label, key=f"hist_{i}", use_container_width=True):
            st.session_state.pending_question = rec["question"]
            st.rerun()

    passing = sum(1 for q in st.session_state.query_log if q.get("status") == "pass")
    total = len(st.session_state.query_log)
    rate = (passing / total * 100.0) if total else 0.0
    st.metric("Query accuracy", f"{passing}/{total}", f"{rate:.0f}%")

    if len(st.session_state.records) >= 3 and st.button("Summarize session as executive brief", use_container_width=True):
        st.session_state.exec_summary = _generate_exec_brief()

    if st.session_state.exec_summary:
        st.info(st.session_state.exec_summary)

    st.download_button(
        "Download conversation CSV",
        data=_export_query_log_csv(),
        file_name="query_log.csv",
        mime="text/csv",
        use_container_width=True,
    )

# Top cards
k1, k2, k3 = st.columns(3)
with k1:
    st.metric("Data sources", "Finance + company graph")
with k2:
    st.metric("Semantic tables", "5")
with k3:
    st.metric("Stage model", "semantic_model.yaml")

left, right = st.columns([1, 1.25], gap="large")

with left:
    st.subheader("Ask a question")
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    q = st.chat_input("Ask about unemployment, rates, retail sales, production, or company ownership")

    st.markdown("**Suggested questions**")
    sq_cols = st.columns(2)
    for i, prompt in enumerate(SUGGESTED):
        if sq_cols[i % 2].button(prompt, key=f"seed_{i}", use_container_width=True):
            st.session_state.pending_question = prompt
            st.rerun()

with right:
    st.subheader("Results")

    if st.session_state.last_result:
        rec = st.session_state.last_result
        st.caption(rec.get("headline", ""))
        _auto_chart(rec["df"])
        _confidence_ui(rec.get("confidence", 0.85))

        if rec.get("narrative"):
            st.info(f"* Insight: {rec['narrative']}")

        with st.expander("View generated SQL", expanded=False):
            st.code(rec.get("sql", "No SQL"), language="sql")

        with st.expander("View raw data table", expanded=False):
            st.dataframe(rec["df"], use_container_width=True)

        st.markdown("**Suggested follow-ups**")
        fcols = st.columns(3)
        for i, fq in enumerate(rec.get("followups", [])[:3]):
            if fcols[i].button(fq, key=f"followup_{i}_{len(st.session_state.records)}", use_container_width=True):
                st.session_state.pending_question = fq
                st.rerun()
    else:
        st.info("Ask a question to generate chart, confidence, narrative, SQL, and follow-up chips.")

# Input resolution
final_question = None
if st.session_state.pending_question:
    final_question = st.session_state.pending_question
    st.session_state.pending_question = None
elif q and q.strip():
    final_question = q.strip()

if final_question:
    final_question = f"{final_question} {TIME_RANGE_SUFFIX[range_choice]}".strip()
    st.session_state.messages.append({"role": "user", "content": final_question})

    with st.spinner("Thinking..."):
        response = call_cortex_analyst(final_question)

        if response.get("error"):
            err = str(response["error"])
            if response.get("raw"):
                err = f"{err}\n\nDetails: {json.dumps(response['raw'], ensure_ascii=True)}"
            st.session_state.messages.append({"role": "assistant", "content": err})
            st.session_state.query_log.append(
                {
                    "question": final_question,
                    "status": "fail",
                    "rows": 0,
                    "confidence": 0.0,
                }
            )
            st.rerun()

        confidence = _extract_confidence(response)
        sql = None
        interpretation = ""
        for block in response.get("message", {}).get("content", []):
            if block.get("type") == "sql":
                sql = block.get("statement") or block.get("sql")
            elif block.get("type") == "text":
                interpretation = block.get("text", "")

        if not sql:
            msg = interpretation or "No SQL generated for this question."
            st.session_state.messages.append({"role": "assistant", "content": msg})
            st.session_state.query_log.append(
                {
                    "question": final_question,
                    "status": "partial",
                    "rows": 0,
                    "confidence": confidence,
                }
            )
            st.rerun()

        df = run_sql(sql)
        if "Error" in df.columns and _wants_subsidiary_leaderboard(final_question):
            df = run_sql(SQL_FALLBACK_MOST_SUBSIDIARIES)
            sql = (
                SQL_FALLBACK_MOST_SUBSIDIARIES
                + "\n\n-- Note: Cortex Analyst SQL failed; ran verified subsidiary leaderboard query."
            )

        if df.empty or "Error" in df.columns:
            reason = "No rows returned." if df.empty else str(df.iloc[0, 0])
            st.session_state.messages.append({"role": "assistant", "content": reason})
            st.session_state.query_log.append(
                {
                    "question": final_question,
                    "status": "fail",
                    "rows": len(df),
                    "confidence": confidence,
                }
            )
            st.rerun()

        narrative = generate_narrative(final_question, df) or interpretation
        followups = _get_followups(final_question, df)
        rec = {
            "question": final_question,
            "sql": sql,
            "df": df,
            "confidence": confidence,
            "narrative": narrative,
            "followups": followups,
            "headline": _headline(df, final_question),
        }
        st.session_state.last_result = rec
        st.session_state.records.append(rec)
        st.session_state.messages.append(
            {
                "role": "assistant",
                "content": narrative or "Here are the results.",
            }
        )
        st.session_state.query_log.append(
            {
                "question": final_question,
                "status": "pass",
                "rows": len(df),
                "confidence": confidence,
            }
        )
        st.rerun()
