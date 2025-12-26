from __future__ import annotations

import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Drug Shortage Pulse", layout="wide")

DATABASE_URL = None
if "DATABASE_URL" in st.secrets:
    DATABASE_URL = st.secrets["DATABASE_URL"]
else:
    DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found. Set it in Streamlit Secrets (Cloud) or .env (local).")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def fmt_days_years(x):
    if x is None:
        return "—"
    x = float(x)
    return f"{x:,.1f} days ({x/365.25:.1f} yrs)"


@st.cache_data(ttl=300)
def fetch_one(sql: str):
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchone()


@st.cache_data(ttl=300)
def fetch_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# Header
st.title("Drug Shortage Pulse")

last = fetch_one("SELECT MAX(fetched_at) AS last_refreshed FROM raw_shortages;")
raw_pages = fetch_one("SELECT COUNT(*) AS n FROM raw_shortages;")

last_refreshed = last[0] if last else None
raw_pages_n = raw_pages[0] if raw_pages else 0

st.caption(f"Last refreshed: {last_refreshed} | Raw pages stored: {raw_pages_n}")

# KPIs
kpi = fetch_one("""
    SELECT kpi_date, new_shortages, active_shortages, resolved_shortages,
        avg_active_duration_days, median_active_duration_days
    FROM daily_kpis
    ORDER BY kpi_date DESC
    LIMIT 1;
""")

if not kpi:
    st.warning("No KPIs yet. Run the pipeline once to populate daily_kpis.")
    st.stop()

kpi_date, new_today, active_now, resolved_now, avg_days, median_days = kpi

# KPI cards
c1, c2, c3, c4 = st.columns(4)
c1.metric("New today", int(new_today))
c2.metric("Active now", int(active_now))
c3.metric("Resolved now", int(resolved_now))
c4.metric("Avg active duration", fmt_days_years(avg_days))

# Median card 
m1, m2 = st.columns(2)
m1.metric("Median active duration", fmt_days_years(median_days))
m2.caption("Duration is computed for CURRENT shortages as days since initial_posting_date.")

st.subheader("Trends")

series = fetch_df("""
    SELECT kpi_date, new_shortages, active_shortages, resolved_shortages
    FROM daily_kpis
    ORDER BY kpi_date;
""")
series["kpi_date"] = pd.to_datetime(series["kpi_date"])

melted = series.melt(id_vars=["kpi_date"], var_name="variable", value_name="value")
fig_trend = px.line(melted, x="kpi_date", y="value", color="variable", markers=True)
fig_trend.update_layout(xaxis_title="Date", yaxis_title="Count")
st.plotly_chart(fig_trend, use_container_width=True)

# Changes
st.subheader("Changes")

available_dates = fetch_df("""
    SELECT DISTINCT change_date
    FROM shortage_changes
    ORDER BY change_date DESC;
""")

if available_dates.empty:
    st.info("No change history yet. This becomes meaningful after at least 2 daily runs.")
else:
    available_dates["change_date"] = pd.to_datetime(available_dates["change_date"]).dt.date
    selected_date = st.selectbox("Change date", available_dates["change_date"].tolist())

    changes = fetch_df("""
        SELECT change_type, COUNT(*) AS count
        FROM shortage_changes
        WHERE change_date = :d
        GROUP BY change_type
        ORDER BY change_type;
    """, {"d": selected_date})

    fig_changes = px.bar(changes, x="change_type", y="count")
    fig_changes.update_layout(xaxis_title="Change type", yaxis_title="Count")
    st.plotly_chart(fig_changes, use_container_width=True)

# Explorer 
st.subheader("Explorer")

status_opts = fetch_df("SELECT DISTINCT status FROM shortages_clean ORDER BY status;")["status"].dropna().tolist()
dose_opts = fetch_df("SELECT DISTINCT dosage_form FROM shortages_clean ORDER BY dosage_form;")["dosage_form"].dropna().tolist()

f1, f2, f3 = st.columns([2, 2, 3])
sel_status = f1.multiselect("Status", status_opts, default=["Current"] if "Current" in status_opts else [])
sel_dose = f2.multiselect("Dosage form", dose_opts, default=[])
search_name = f3.text_input("Search generic name", value="")

# Build query dynamically 
where = []
params = {}

if sel_status:
    where.append("status = ANY(:statuses)")
    params["statuses"] = sel_status

if sel_dose:
    where.append("dosage_form = ANY(:doses)")
    params["doses"] = sel_dose

if search_name.strip():
    where.append("LOWER(generic_name) LIKE :q")
    params["q"] = f"%{search_name.strip().lower()}%"

where_sql = "WHERE " + " AND ".join(where) if where else ""

df = fetch_df(f"""
    SELECT shortage_key, generic_name, proprietary_name, dosage_form, status,
        therapeutic_category, initial_posting_date, update_date, change_date,
        discontinued_date, update_type, company_name, record_hash, ingested_at
    FROM shortages_clean
    {where_sql}
    ORDER BY ingested_at DESC
    LIMIT 500;
""", params)

# Convert date columns 
for col in ["initial_posting_date", "update_date", "change_date", "discontinued_date", "ingested_at"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

st.dataframe(df, use_container_width=True, height=320)

#Status counts
st.subheader("Status counts")

status_counts = fetch_df("""
    SELECT status, COUNT(*) AS count
    FROM shortages_clean
    GROUP BY status
    ORDER BY count DESC;
""")

fig_status = px.bar(status_counts, x="status", y="count")
fig_status.update_layout(xaxis_title="Status", yaxis_title="Count")
st.plotly_chart(fig_status, use_container_width=True)
