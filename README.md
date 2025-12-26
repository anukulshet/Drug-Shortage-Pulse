# Drug Shortage Pulse

A cloud-deployed healthcare analytics dashboard that tracks **FDA drug shortages**, updates daily, and highlights what changed since the last run.

**Live dashboard:** https://drug-shortage-pulse.streamlit.app

---

## What this project does (in simple words)

Every day, the FDA updates its drug shortage data. This project:
- downloads the latest shortage list from openFDA,
- stores the raw response (for traceability),
- cleans it into a structured table (easy to analyze),
- takes a daily snapshot so we can compare yesterday vs today,
- computes daily KPIs (new, active, resolved + durations),
- shows everything in an interactive Streamlit dashboard.

This is useful for anyone who needs to monitor supply risk (hospital ops, pharmacy teams, healthcare analysts).

---

## Features

### Automated pipeline (ETL)
- **Ingest**: fetch latest shortages from openFDA and save the full JSON payload
- **Transform**: normalize JSON into a clean relational table
- **Snapshot**: store daily snapshot of shortage keys + hashes
- **Change detection**: classify changes since previous snapshot as **NEW / UPDATED / RESOLVED**
- **KPIs**: store daily metrics in `daily_kpis` for trend charts and KPI cards

### Dashboard (Streamlit + Plotly)
- KPI cards: **New today**, **Active now**, **Resolved now**
- Duration insight: **Avg active duration** (optional median if enabled)
- Trends over time using `daily_kpis`
- Changes by date (NEW/UPDATED/RESOLVED)
- Explorer with filters + searchable table
- Status distribution chart

---

## Architecture

```text
openFDA API
   |
   v
Python pipeline (src/run_pipeline.py)
   |
   v
PostgreSQL (Neon in cloud / Docker locally)
   |
   v
Streamlit dashboard (app/app.py)
