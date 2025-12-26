from sqlalchemy import text
from .db import engine

def refresh_daily_kpis():
    sql = """
    INSERT INTO daily_kpis (kpi_date, new_shortages, active_shortages, resolved_shortages, avg_active_duration_days)
    SELECT
      CURRENT_DATE,
      (SELECT COUNT(*) FROM shortage_changes WHERE change_date = CURRENT_DATE AND change_type = 'NEW'),
      (SELECT COUNT(*) FROM shortages_clean WHERE status ILIKE '%current%' OR status ILIKE '%ongoing%' OR status ILIKE '%active%'),
      (SELECT COUNT(*) FROM shortages_clean WHERE status ILIKE '%resolved%'),
      (
        SELECT AVG((CURRENT_DATE - initial_posting_date))
          FROM shortages_clean
          WHERE initial_posting_date IS NOT NULL
          AND initial_posting_date >= DATE '2010-01-01'
            AND (status ILIKE '%current%' OR status ILIKE '%ongoing%' OR status ILIKE '%active%')
      )
    ON CONFLICT (kpi_date)
    DO UPDATE SET
      new_shortages = EXCLUDED.new_shortages,
      active_shortages = EXCLUDED.active_shortages,
      resolved_shortages = EXCLUDED.resolved_shortages,
      avg_active_duration_days = EXCLUDED.avg_active_duration_days,
      created_at = NOW();
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
