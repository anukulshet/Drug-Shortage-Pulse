# app/app.py
import os
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(override=True)

DATABASE_URL = None

if "DATABASE_URL" in st.secrets:
    DATABASE_URL = st.secrets["DATABASE_URL"]
else:
    DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found. Set it in Streamlit Secrets or in a local .env file.")

engine = create_engine(DATABASE_URL, future=True)

st.set_page_config(page_title="Drug Shortage Pulse", layout="wide")
st.title("Drug Shortage Pulse")


#Data Loaders
@st.cache_data(ttl=300)
def load_last_refresh():
    df = pd.read_sql(
        "SELECT MAX(fetched_at) AS last_fetched_at, COUNT(*) AS pages FROM raw_shortages;",
        engine,
    )
    if df.empty:
        return None, 0
    ts = df.loc[0, "last_fetched_at"]
    pages = int(df.loc[0, "pages"] or 0)
    return ts, pages


@st.cache_data(ttl=300)
def load_kpis():
    df = pd.read_sql("SELECT * FROM daily_kpis ORDER BY kpi_date ASC;", engine)
    if not df.empty:
        df["kpi_date"] = pd.to_datetime(df["kpi_date"])
    return df


@st.cache_data(ttl=300)
def load_shortages(limit=20000):
    return pd.read_sql(
        f"SELECT * FROM shortages_clean ORDER BY ingested_at DESC LIMIT {int(limit)};",
        engine,
    )


@st.cache_data(ttl=300)
def load_available_change_dates(limit=60):
    df = pd.read_sql(
        f"SELECT DISTINCT change_date FROM shortage_changes ORDER BY change_date DESC LIMIT {int(limit)};",
        engine,
    )
    if df.empty:
        return []
    return [d for d in df["change_date"].tolist() if pd.notna(d)]


@st.cache_data(ttl=300)
def load_changes_for_date(change_date):
    q = text("""
        SELECT change_type, shortage_key, created_at
        FROM shortage_changes
        WHERE change_date = CAST(:d AS date)
        ORDER BY created_at DESC
        LIMIT 500;
    """)
    return pd.read_sql(q, engine, params={"d": str(change_date)})


#Header info
last_ts, pages = load_last_refresh()
if last_ts is not None:
    st.caption(f"Last refreshed: {last_ts} | Raw pages stored: {pages}")
else:
    st.caption("No raw ingests yet. Run the pipeline to populate the database.")


#KPIs
kpi = load_kpis()
latest = kpi.iloc[-1] if len(kpi) else None

c1, c2, c3, c4 = st.columns(4)

if latest is not None:
    c1.metric("New today", int(latest["new_shortages"]))
    c2.metric("Active now", int(latest["active_shortages"]))
    c3.metric("Resolved now", int(latest["resolved_shortages"]))
    avg_days = float(latest["avg_active_duration_days"] or 0)
    c4.metric("Avg active duration", f"{avg_days:,.1f} days  ({avg_days/365:.1f} yrs)")
else:
    c1.metric("New today", 0)
    c2.metric("Active now", 0)
    c3.metric("Resolved now", 0)
    c4.metric("Avg active duration", "0.0 days")


# Trends
st.subheader("Trends")

if not kpi.empty:
    fig = px.line(
        kpi,
        x="kpi_date",
        y=["new_shortages", "active_shortages", "resolved_shortages"],
        markers=True,
    )
    fig.update_xaxes(title="Date", type="date", tickformat="%b %d")
    fig.update_yaxes(title="Count")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No KPI history yet. Run the pipeline to populate daily_kpis.")


st.subheader("Changes")

change_dates = load_available_change_dates()
if not change_dates:
    st.info("No change history yet. Run the pipeline at least once, and again on a later day to see differences.")
else:
    default_date = change_dates[0]
    selected_date = st.date_input("Change date", value=default_date)

    changes = load_changes_for_date(selected_date)

    if changes.empty:
        st.write(f"No changes recorded for {selected_date}.")
    else:
        # Summary bar chart
        summary = changes["change_type"].value_counts().reset_index()
        summary.columns = ["change_type", "count"]
        figc = px.bar(summary, x="change_type", y="count")
        figc.update_xaxes(title="Change type")
        figc.update_yaxes(title="Count")
        st.plotly_chart(figc, use_container_width=True)

        # Detail table
        st.dataframe(changes, use_container_width=True)

# Explorer
st.subheader("Explorer")

df = load_shortages()

status_options = sorted(df["status"].dropna().unique().tolist()) if "status" in df.columns else []
dosage_options = sorted(df["dosage_form"].dropna().unique().tolist()) if "dosage_form" in df.columns else []

colA, colB, colC = st.columns([2, 2, 3])

with colA:
    status = st.multiselect("Status", status_options)
with colB:
    dosage_form = st.multiselect("Dosage form", dosage_options)
with colC:
    search = st.text_input("Search generic name", "")

f = df.copy()

if status and "status" in f.columns:
    f = f[f["status"].isin(status)]

if dosage_form and "dosage_form" in f.columns:
    f = f[f["dosage_form"].isin(dosage_form)]

if search and "generic_name" in f.columns:
    f = f[f["generic_name"].fillna("").str.contains(search, case=False, na=False)]

st.dataframe(f.head(500), use_container_width=True)

st.subheader("Status counts")
if "status" in f.columns and not f.empty:
    counts = f["status"].fillna("Unknown").value_counts().reset_index()
    counts.columns = ["status", "count"]
    fig2 = px.bar(counts, x="status", y="count")
    fig2.update_xaxes(title="Status")
    fig2.update_yaxes(title="Count")
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No rows match the current filters.")
