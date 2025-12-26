from __future__ import annotations

from sqlalchemy import text
from .db import engine


def _safe_scalar(conn, sql: str, params: dict | None = None, default=0):
    val = conn.execute(text(sql), params or {}).scalar()
    return default if val is None else val


def compute_kpis_for_today() -> dict:
    """
    Computes today's KPIs and returns a dict ready for upsert into daily_kpis.

    Definitions:
    - active_shortages: count of shortages_clean where status = 'Current'
    - resolved_shortages: count of shortages_clean where status = 'Resolved'
    - new_shortages:
        preferred: count of shortage_changes today where change_type = 'NEW'
        fallback: count of shortages_clean ingested today
    - avg/median_active_duration_days:
        days since initial_posting_date for CURRENT shortages
    """
    with engine.begin() as conn:
        # Active/resolved counts
        active_now = _safe_scalar(
            conn,
            "SELECT COUNT(*) FROM shortages_clean WHERE status = 'Current';"
        )
        resolved_now = _safe_scalar(
            conn,
            "SELECT COUNT(*) FROM shortages_clean WHERE status = 'Resolved';"
        )

        # New shortages today 
        new_today = _safe_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM shortage_changes
            WHERE change_date = CURRENT_DATE
              AND change_type = 'NEW';
            """,
            default=None
        )

        # Fallback if shortage_changes is empty/not populated yet
        if new_today is None or new_today == 0:
            new_today = _safe_scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM shortages_clean
                WHERE ingested_at::date = CURRENT_DATE;
                """
            )

        # Avg + Median active duration in days (for CURRENT shortages)
        stats = conn.execute(text("""
            WITH active AS (
                SELECT (CURRENT_DATE - initial_posting_date)::int AS days
                FROM shortages_clean
                WHERE status = 'Current'
                  AND initial_posting_date IS NOT NULL
                  AND initial_posting_date <= CURRENT_DATE
            )
            SELECT
                AVG(days)::numeric AS avg_days,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY days)::numeric AS median_days
            FROM active;
        """)).fetchone()

        avg_days = stats[0] if stats else None
        median_days = stats[1] if stats else None

        return {
            "kpi_date": None,  # filled in SQL via CURRENT_DATE
            "new_shortages": int(new_today),
            "active_shortages": int(active_now),
            "resolved_shortages": int(resolved_now),
            "avg_active_duration_days": avg_days,
            "median_active_duration_days": median_days,
        }


def upsert_daily_kpis():
    """
    Upserts today's KPI row into daily_kpis.
    """
    payload = compute_kpis_for_today()

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO daily_kpis (
                    kpi_date,
                    new_shortages,
                    active_shortages,
                    resolved_shortages,
                    avg_active_duration_days,
                    median_active_duration_days
                )
                VALUES (
                    CURRENT_DATE,
                    :new_shortages,
                    :active_shortages,
                    :resolved_shortages,
                    :avg_active_duration_days,
                    :median_active_duration_days
                )
                ON CONFLICT (kpi_date) DO UPDATE SET
                    new_shortages = EXCLUDED.new_shortages,
                    active_shortages = EXCLUDED.active_shortages,
                    resolved_shortages = EXCLUDED.resolved_shortages,
                    avg_active_duration_days = EXCLUDED.avg_active_duration_days,
                    median_active_duration_days = EXCLUDED.median_active_duration_days,
                    created_at = NOW();
            """),
            payload
        )


def run_kpis():
    upsert_daily_kpis()
    return True
