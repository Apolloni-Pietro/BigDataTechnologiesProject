import os
import time
from typing import Any, cast
import requests
import pandas as pd
import streamlit as st

# ── Configuration ─────────────────────────────────────────────────────────
API_URL = os.getenv("API_URL", "http://api:8080")
# All API calls go through FastAPI. The dashboard never talks to
# Redis or TimescaleDB directly.

# ── Page setup ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title = "OSS Health Monitor",
    page_icon  = "🔍",
    layout     = "wide",
)


# ── API helpers ───────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
# cache_data caches the return value of this function for 30 seconds.
# Without this, every Streamlit re-render (e.g. a slider moving) would
# fire a new HTTP request to FastAPI. With it, the same response is
# reused for 30 seconds, then refreshed.
def fetch_all_repos(max_score: float = 1.0, sort: str = "importance") -> pd.DataFrame:
    """Fetch all repos from the API and return as a DataFrame."""
    try:
        resp = requests.get(
            f"{API_URL}/repos",
            params  = {"limit": 100, "max_score": max_score, "sort": sort},
            timeout = 5,
        )
        resp.raise_for_status()
        data = resp.json()
        return pd.DataFrame(data.get("repos", []))
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to the API. Is the `api` container running?")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"API error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_current_metrics(owner: str, name: str) -> dict:
    """Fetch the latest metric values for a single repo."""
    try:
        resp = requests.get(
            f"{API_URL}/repos/{owner}/{name}/current",
            timeout=5,
        )
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=30)
def fetch_history(owner: str, name: str, days: int) -> pd.DataFrame:
    """Fetch the health score history for a single repo."""
    try:
        resp = requests.get(
            f"{API_URL}/repos/{owner}/{name}/history",
            params  = {"days": days},
            timeout = 5,
        )
        resp.raise_for_status()
        data   = resp.json()
        points = data.get("points", [])
        if not points:
            return pd.DataFrame()
        df = pd.DataFrame(points)
        # Normalise the time column regardless of whether it came from
        # Redis (timestamp_ms) or TimescaleDB (day string).
        if "timestamp_ms" in df.columns:
            df["time"] = pd.to_datetime(df["timestamp_ms"], unit="ms")
            df = df.rename(columns={"value": "health_score"})
        elif "day" in df.columns:
            df["time"] = pd.to_datetime(df["day"])
            df = df.rename(columns={"health_score_avg": "health_score"})
        return df[["time", "health_score"]].set_index("time")
    except Exception as e:
        return pd.DataFrame()


# ── Layout ────────────────────────────────────────────────────────────────

st.title("🔍 OSS Health Monitor")
st.caption("Real-time health tracking for open-source projects.")

# Sidebar navigation
page = st.sidebar.radio("Navigate", ["📊 Overview", "🔎 Repository Detail"])

# ── Page 1: Overview ──────────────────────────────────────────────────────

if page == "📊 Overview":
    st.header("Repository Health Overview")

    col1, col2 = st.columns([3, 1])
    with col2:
        sort_label = st.selectbox(
            "Sort by",
            ["Importance (activity)", "Health score", "Name"],
            index=0,
            help="Importance ranks by distinct active people (actors) in the rolling window — bot-resistant, unlike raw event volume.",
        )
        show_at_risk = st.checkbox("Show at-risk only", value=False)
        max_score    = 0.35 if show_at_risk else 1.0

    sort_key = {
        "Importance (activity)": "importance",
        "Health score":          "health_score",
        "Name":                  "name",
    }[sort_label]

    df = fetch_all_repos(max_score=max_score, sort=sort_key)

    if df.empty:
        st.info("No data yet. The pipeline is still processing GitHub events — check back shortly.")
        st.stop()

    # Summary metrics at the top of the page
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Repos tracked",     len(df))
    c2.metric("Avg health score",  f"{df['health_score'].mean():.2f}" if "health_score" in df.columns else "—")
    c3.metric("At-risk repos",     len(df[df["health_score"] < 0.35]) if "health_score" in df.columns else "—")
    c4.metric("Healthy repos",     len(df[df["health_score"] >= 0.70]) if "health_score" in df.columns else "—")

    st.divider()

    # Colour-code the health_score column in the table.
    # Streamlit's st.dataframe accepts a Styler object.
    if "health_score" in df.columns:
        def colour_score(val):
            try:
                v = float(val)
                if v < 0.35:
                    return "background-color: #ffcccc"   # red
                elif v < 0.70:
                    return "background-color: #fff4cc"   # yellow
                else:
                    return "background-color: #ccffcc"   # green
            except (TypeError, ValueError):
                return ""
        styled = cast(Any, df.style).applymap(colour_score, subset=["health_score"])
        st.dataframe(styled, use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)

    # Auto-refresh controls
    st.divider()
    auto_refresh = st.checkbox("Auto-refresh every 30 seconds")
    if auto_refresh:
        time.sleep(30)
        st.rerun()


# ── Page 2: Repository Detail ─────────────────────────────────────────────

elif page == "🔎 Repository Detail":
    st.header("Repository Detail")

    repo_input = st.text_input(
        "Enter a repository (owner/name)",
        placeholder="e.g. facebook/react",
    )

    if not repo_input or "/" not in repo_input:
        st.info("Enter a repository name in the format `owner/name` above.")
        st.stop()

    parts = repo_input.strip().split("/", 1)
    owner, name = parts[0], parts[1]

    # ── Current metrics ──────────────────────────────────────────
    st.subheader(f"Current metrics — `{repo_input}`")

    data = fetch_current_metrics(owner, name)

    if "error" in data:
        st.error(f"API error: {data['error']}")
    elif not data or not data.get("metrics"):
        st.warning(
            f"No data found for `{repo_input}`. "
            "It may not have appeared in the event stream yet. "
            "Wait a few minutes and try again."
        )
    else:
        metrics = data["metrics"]
        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Health score",
            f"{float(metrics.get('health_score', 0)):.2f}",
            help="Composite score from 0 (critical) to 1 (healthy)."
        )
        c2.metric(
            "Bus factor",
            metrics.get("bus_factor", "—"),
            help="Number of contributors who own >80% of commits."
        )
        c3.metric(
            "Commit freq (30d)",
            f"{float(metrics.get('commit_freq_30d', 0)):.1f}/day",
        )
        c4.metric(
            "Stale issue ratio",
            f"{float(metrics.get('stale_issue_ratio', 0)) * 100:.0f}%",
        )

    # ── Historical trend ──────────────────────────────────────────
    st.subheader("Health score over time")

    days = st.slider("Time window (days)", min_value=1, max_value=90, value=7)

    df_history = fetch_history(owner, name, days)

    if df_history.empty:
        st.info("No historical data available for this time window yet.")
    else:
        st.line_chart(df_history["health_score"])
        st.caption(
            f"Source: {'Redis' if days <= 7 else 'TimescaleDB'}  |  "
            f"{len(df_history)} data points"
        )

    # ── Raw metrics table ─────────────────────────────────────────
    with st.expander("Raw metric values"):
        if data and data.get("metrics"):
            st.json(data["metrics"])