# Drug Shortage Pulse

Drug Shortage Pulse is a daily-updating analytics dashboard that tracks FDA drug shortages, detects what changed since the last run (NEW/UPDATED/RESOLVED), and summarizes trends and KPIs for quick decision-making.

**Use case:** help healthcare operations / pharmacy supply teams monitor shortages, spot new issues early, and understand whether the situation is improving or worsening over time.

---

## Demo

- Streamlit dashboard: **<ADD YOUR STREAMLIT CLOUD URL HERE>**

---

## What it does

### Data pipeline (daily)
1. Pulls the latest drug shortage data from the **openFDA Drug Shortages API**
2. Stores the full raw API payload (for traceability)
3. Normalizes JSON into clean relational tables for analytics
4. Creates a daily snapshot and computes deltas vs the previous run
5. Writes daily KPIs for fast trend charts

### Dashboard
- KPI cards: New today, Active now, Resolved now, Avg active duration
- Trend chart from daily KPIs
- “Changes” view (NEW/UPDATED/RESOLVED) by date
- Explorer with filters + searchable table
- Status distribution chart

---

## Architecture

