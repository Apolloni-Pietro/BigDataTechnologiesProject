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

@st.cache_data(ttl=30)
def fetch_history(owner: str, name: str, days: int) -> pd.DataFrame:
    """Chiama l'endpoint GET /repos/{owner}/{name}/history di FastAPI (che legge da TimescaleDB/Redis)"""
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
        return df[["time", "health_score"]].set_index("time")
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=30)
def fetch_at_risk(threshold: float = 0.35) -> pd.DataFrame:
    """Chiama l'endpoint GET /at-risk di FastAPI"""
    try:
        resp = requests.get(f"{API_URL}/at-risk", params={"threshold": threshold, "limit": 50}, timeout=5)
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
    c2.metric("Avg Health Score", f"{df['health_score'].mean():.2f}" if "health_score" in df.columns else "—")
    c3.metric("At-Risk Repos (<0.35)", len(df[df["health_score"] < 0.35]) if "health_score" in df.columns else "—")
    c4.metric("Healthy Repos (>0.70)", len(df[df["health_score"] >= 0.70]) if "health_score" in df.columns else "—")

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
            title="Overview dei Progetti (Più è basso lo score, più è rosso)"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    if "health_score" in df.columns:
        def colour_score(val):
            try:
                v = float(val)
                if v < 0.35: return "background-color: #ffcccc; color: black;"
                elif v < 0.70: return "background-color: #fff4cc; color: black;"
                else: return "background-color: #ccffcc; color: black;"
            except (TypeError, ValueError): return ""
        styled = cast(Any, df.style).applymap(colour_score, subset=["health_score"])
        st.dataframe(styled, use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)

    auto_refresh = st.checkbox("Auto-refresh every 30 seconds")
    if auto_refresh:
        time.sleep(30)
        st.rerun()

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
            # ── Quadratini dei KPI Corretti con i dati reali del Consumer ──
            c1, c2, c3, c4 = st.columns(4)

            c1.metric("Health Score", f"{float(metrics.get('health_score', 0)):.2f}")
            c2.metric("Bus Factor", metrics.get("bus_factor", "—"))
            c3.metric("Commit Freq (30d)", f"{float(metrics.get('commit_freq_30d', 0)):.1f}/day")
            c4.metric("Days Since Last Release", metrics.get("days_since_last_release", "—"))

            st.divider()

            st.subheader("Health Trend")
            days = st.slider("Time window (days)", min_value=1, max_value=90, value=14)
            df_history = fetch_history(owner, name, days)

            if df_history.empty:
                st.info("No historical data available for this time window yet.")
            else:
                df_history.reset_index(inplace=True)
                fig_line = px.line(df_history, x="time", y="health_score", 
                                   title="Health Score Over Time",
                                   labels={"time": "Date", "health_score": "Score"})
                fig_line.update_yaxes(range=[0, 1])
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
            title="Top 15 Most Fragile Projects"
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        st.dataframe(df_risk, use_container_width=True)