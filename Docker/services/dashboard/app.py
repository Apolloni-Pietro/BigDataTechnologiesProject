import os
import time
from typing import Any, cast
import requests
import pandas as pd
import streamlit as st
import plotly.express as px

# ── Configuration ─────────────────────────────────────────────────────────
# Questo è l'indirizzo del container dove gira FastAPI!
API_URL = os.getenv("API_URL", "http://api:8080")

# ── Metric presentation (friendly names + formatting) ─────────────────────
# Raw internal metric keys -> user-friendly labels. Used for table headers,
# KPI tiles and chart axes so naming stays consistent across every page.
METRIC_LABELS = {
    "repo_name":               "Repository",
    "health_score":            "Health Score",
    "risk_score":              "Risk Score",
    "bus_factor":              "Bus Factor (contributors)",
    "commit_freq_30d":         "Commits / Day (30d)",
    "active_actors":           "Active Participants (90d)",
    "active_contributors_90d": "Code Contributors (90d)",
    "event_count":             "Total Events (90d)",
    "pr_latency_p50":          "Median PR Merge Time (h)",
    "pr_abandon_rate":         "PR Abandon Rate",
    "stale_issue_ratio":       "Stale Issue Ratio",
    "days_since_last_commit":  "Days Since Last Commit",
    "days_since_last_release": "Days Since Last Release",
    "last_updated":            "Last Updated",
}

# Metrics that are true 0–1 ratios -> shown as percentages (e.g. 65.8%).
# NOTE: bus_factor is an integer head-count, NOT a ratio, so it stays a count.
PERCENT_METRICS = {"health_score", "risk_score", "pr_abandon_rate", "stale_issue_ratio"}

# Metrics that are whole numbers (counts / day deltas) -> render without decimals.
INTEGER_METRICS = {
    "bus_factor", "active_actors", "active_contributors_90d", "event_count",
    "days_since_last_commit", "days_since_last_release",
}


def fmt_metric(key: str, value) -> str:
    """Format a single metric value for display ('—' when missing)."""
    if value is None or value == "" or value == "—":
        return "—"
    if key == "last_updated":
        try:
            # Case A: If value is a numeric epoch time (or string representing one)
            epoch = float(value)
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch))
        except (TypeError, ValueError):
            # Case B: If value is an ISO 8601 string (e.g., "2026-06-23T15:30:00")
            try:
                return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(value)  # Fallback to raw string if parsing fails
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)  # non-numeric (e.g. repo_name)
    if key in PERCENT_METRICS:
        return f"{v * 100:.1f}%"
    if key in INTEGER_METRICS:
        return f"{int(round(v))}"
    return f"{v:.2f}"  # remaining floats (commit_freq_30d, pr_latency_p50)


def _parse_last_updated(value) -> str:
    """Convert an epoch float or ISO string to a human-friendly timestamp."""
    if value is None or value == "" or value != value:  # None / NaN
        return "—"
    try:
        return time.strftime("%-d %b %Y %H:%M", time.localtime(float(value)))
    except (TypeError, ValueError):
        pass
    try:
        return pd.to_datetime(value).strftime("%-d %b %Y %H:%M")
    except Exception:
        return str(value)


def friendly_df(df: pd.DataFrame) -> pd.DataFrame:
    """Scale percent metrics to 0–100, humanise last_updated, rename columns.

    Returns a display-only copy; the caller's raw df is left untouched.
    """
    out = df.copy()
    for col in PERCENT_METRICS & set(out.columns):
        out[col] = pd.to_numeric(out[col], errors="coerce") * 100
    if "last_updated" in out.columns:
        out["last_updated"] = out["last_updated"].apply(_parse_last_updated)
    return out.rename(columns=METRIC_LABELS)

# ── Page setup ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OSS Health Monitor",
    page_icon="🔍",
    layout="wide",
)

# ── API helpers (Comunicazione con FastAPI) ───────────────────────────────

@st.cache_data(ttl=30)
def fetch_all_repos(max_score: float = 1.0) -> pd.DataFrame:
    """Chiama l'endpoint GET /repos di FastAPI"""
    try:
        resp = requests.get(
            f"{API_URL}/repos",
            params={"limit": 1000, "max_score": max_score},
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        return pd.DataFrame(data.get("repos", []))
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to FastAPI. Is the `api` container running?")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"API error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=10)
def fetch_current_metrics(owner: str, name: str) -> dict:
    """Chiama l'endpoint GET /repos/{owner}/{name}/current di FastAPI (che legge da Redis)"""
    try:
        resp = requests.get(f"{API_URL}/repos/{owner}/{name}/current", timeout=5)
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e)}

_FETCH_HISTORY_ERROR: dict = {}

@st.cache_data(ttl=30)
def fetch_history(owner: str, name: str, days: int) -> pd.DataFrame:
    """Chiama l'endpoint GET /repos/{owner}/{name}/history di FastAPI (che legge da TimescaleDB/Redis)"""
    _FETCH_HISTORY_ERROR.clear()
    try:
        resp = requests.get(f"{API_URL}/repos/{owner}/{name}/history", params={"days": days}, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        points = data.get("points", [])
        if not points:
            return pd.DataFrame()
        df = pd.DataFrame(points)
        if "timestamp_ms" in df.columns:
            df["time"] = pd.to_datetime(df["timestamp_ms"], unit="ms")
            df = df.rename(columns={"value": "health_score"})
        elif "day" in df.columns:
            df["time"] = pd.to_datetime(df["day"])
            df = df.rename(columns={"health_score_avg": "health_score"})
        else:
            _FETCH_HISTORY_ERROR["msg"] = f"Unexpected API response format: {list(df.columns)}"
            return pd.DataFrame()
        return df[["time", "health_score"]].set_index("time")
    except Exception as e:
        _FETCH_HISTORY_ERROR["msg"] = str(e)
        return pd.DataFrame()

@st.cache_data(ttl=30)
def fetch_at_risk(threshold: float = 0.35) -> pd.DataFrame:
    """Chiama l'endpoint GET /at-risk di FastAPI"""
    try:
        resp = requests.get(f"{API_URL}/at-risk", params={"threshold": threshold, "limit": 500}, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        return pd.DataFrame(data.get("repos", []))
    except Exception:
        return pd.DataFrame()

# ── Layout ────────────────────────────────────────────────────────────────

st.title("🔍 OSS Ecosystem Health Monitor")
st.caption("Tracking the health and fragility of open-source projects in real-time.")

page = st.sidebar.radio("Navigate", ["📊 Overview", "🔎 Repository Detail", "⚠️ At-Risk Projects"])

# ── Page 1: Overview ──────────────────────────────────────────────────────
if page == "📊 Overview":
    st.header("Global Ecosystem Health")

    df = fetch_all_repos()

    if df.empty:
        st.info("No data yet. Waiting for FastAPI to return data...")
        st.stop()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Repos Tracked", len(df))
    c2.metric("Avg Health Score", fmt_metric("health_score", df["health_score"].mean()) if "health_score" in df.columns else "—")
    c3.metric("At-Risk Repos (<35%)", len(df[df["health_score"] < 0.35]) if "health_score" in df.columns else "—")
    c4.metric("Healthy Repos (>70%)", len(df[df["health_score"] >= 0.70]) if "health_score" in df.columns else "—")

    st.divider()

    st.subheader("Health Score vs Bus Factor Distribution")
    if "health_score" in df.columns and "bus_factor" in df.columns:
        fig_scatter = px.scatter(
            df,
            x="bus_factor",
            y="health_score",
            hover_data=["repo_name"],
            color="health_score",
            color_continuous_scale="RdYlGn",
            labels={
                "bus_factor": METRIC_LABELS["bus_factor"],
                "health_score": METRIC_LABELS["health_score"],
                "repo_name": METRIC_LABELS["repo_name"],
            },
            title="Project Overview (lower score = more red = more fragile)",
        )
        fig_scatter.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig_scatter, use_container_width=True)

    # Friendly column names; percent metrics are scaled to 0–100 by friendly_df,
    # so the colour thresholds below are on that same percentage scale.
    df_display = friendly_df(df)
    col_fmt: dict[str, str] = {}
    for k in PERCENT_METRICS:
        lbl = METRIC_LABELS[k]
        if lbl in df_display.columns:
            col_fmt[lbl] = "{:.1f}%"
    for k in INTEGER_METRICS:
        lbl = METRIC_LABELS[k]
        if lbl in df_display.columns:
            col_fmt[lbl] = "{:.0f}"
    for k in ("commit_freq_30d", "pr_latency_p50"):
        lbl = METRIC_LABELS[k]
        if lbl in df_display.columns:
            col_fmt[lbl] = "{:.2f}"
    health_col = METRIC_LABELS["health_score"]
    styler = cast(Any, df_display.style).format(col_fmt, na_rep="—")
    if health_col in df_display.columns:
        def colour_score(val):
            try:
                v = float(val)
                if v < 35: return "background-color: #ffcccc; color: black;"
                elif v < 70: return "background-color: #fff4cc; color: black;"
                else: return "background-color: #ccffcc; color: black;"
            except (TypeError, ValueError): return ""
        styler = styler.applymap(colour_score, subset=[health_col])
    st.dataframe(styler, use_container_width=True)

# ── Page 2: Repository Detail ─────────────────────────────────────────────
elif page == "🔎 Repository Detail":
    st.header("Deep Dive Analysis")

    df_all = fetch_all_repos()
    if df_all.empty:
        st.warning("No repositories available yet.")
        st.stop()

    repo_list = sorted(df_all["repo_name"].unique().tolist())
    repo_input = st.selectbox("Select a repository to analyze:", repo_list)

    if repo_input:
        parts = repo_input.strip().split("/", 1)
        owner, name = parts[0], parts[1]

        st.subheader(f"Current metrics — `{repo_input}`")
        data = fetch_current_metrics(owner, name)

        if "error" in data:
            st.error(f"API error: {data['error']}")
        elif not data or not data.get("metrics"):
            st.warning("No metrics found via FastAPI for this repo yet.")
        else:
            metrics = data["metrics"]
            # All available metrics, friendly-named and formatted, in a 4-wide grid.
            # Ordered with the headline health signals first; repo_name/internal
            # keys are skipped (the raw payload is in the expander below).
            display_order = [
                "health_score", "risk_score", "bus_factor", "commit_freq_30d",
                "active_actors", "active_contributors_90d", "event_count",
                "pr_latency_p50", "pr_abandon_rate", "stale_issue_ratio",
                "days_since_last_commit", "days_since_last_release",
            ]
            shown = [k for k in display_order if k in metrics]
            cols = st.columns(4)
            for i, key in enumerate(shown):
                cols[i % 4].metric(METRIC_LABELS.get(key, key), fmt_metric(key, metrics.get(key)))

            st.divider()

            st.subheader("Health Trend")
            days = st.slider("Time window (days)", min_value=1, max_value=90, value=7)
            df_history = fetch_history(owner, name, days)

            if df_history.empty:
                if _FETCH_HISTORY_ERROR.get("msg"):
                    st.warning(f"Could not load history: {_FETCH_HISTORY_ERROR['msg']}")
                else:
                    st.info("No historical data available for this time window yet.")
            else:
                df_history.reset_index(inplace=True)
                fig_line = px.line(df_history, x="time", y="health_score",
                                   title="Health Score Over Time",
                                   labels={"time": "Date", "health_score": METRIC_LABELS["health_score"]})
                fig_line.update_yaxes(range=[0, 1], tickformat=".0%")
                st.plotly_chart(fig_line, use_container_width=True)

            with st.expander("Raw API payload (From FastAPI)"):
                st.json(data["metrics"])

# ── Page 3: At-Risk Projects ──────────────────────────────────────────────
elif page == "⚠️ At-Risk Projects":
    st.header("Operational Risks & Deterioration")
    st.caption("Highlighting projects with a critical Health Score (< 0.35)")

    threshold = st.slider("Risk Threshold", min_value=0.1, max_value=0.6, value=0.35, step=0.05)
    df_risk = fetch_at_risk(threshold=threshold)

    if df_risk.empty:
        st.success("🎉 No projects found below this risk threshold!")
    else:
        st.error(f"Found {len(df_risk)} projects classified as high-risk.")
        
        fig_bar = px.bar(
            df_risk.sort_values("health_score", ascending=True).head(15),
            x="health_score",
            y="repo_name",
            orientation="h",
            color="health_score",
            color_continuous_scale="Reds_r",
            labels={
                "health_score": METRIC_LABELS["health_score"],
                "repo_name": METRIC_LABELS["repo_name"],
            },
            title="Top 15 Most Fragile Projects",
        )
        fig_bar.update_xaxes(tickformat=".0%")
        st.plotly_chart(fig_bar, use_container_width=True)

        df_risk_display = friendly_df(df_risk)
        risk_col_fmt: dict[str, str] = {}
        for k in PERCENT_METRICS:
            lbl = METRIC_LABELS[k]
            if lbl in df_risk_display.columns:
                risk_col_fmt[lbl] = "{:.1f}%"
        for k in INTEGER_METRICS:
            lbl = METRIC_LABELS[k]
            if lbl in df_risk_display.columns:
                risk_col_fmt[lbl] = "{:.0f}"
        for k in ("commit_freq_30d", "pr_latency_p50"):
            lbl = METRIC_LABELS[k]
            if lbl in df_risk_display.columns:
                risk_col_fmt[lbl] = "{:.2f}"
        st.dataframe(
            cast(Any, df_risk_display.style).format(risk_col_fmt, na_rep="—"),
            use_container_width=True,
        )