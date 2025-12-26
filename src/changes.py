from __future__ import annotations

from sqlalchemy import text
from .db import engine


def compute_changes() -> int:
    """
    Compare today's snapshot vs yesterday's snapshot and write rows into shortage_changes.
    Change types:
      - NEW: appears today but not yesterday
      - RESOLVED: status changed to resolved today
      - UPDATED: record hash changed (something about the record changed)
    """
    sql = """
    WITH today AS (
      SELECT shortage_key, record_hash, status
      FROM shortages_snapshot
      WHERE snapshot_date = CURRENT_DATE
    ),
    yday AS (
      SELECT shortage_key, record_hash, status
      FROM shortages_snapshot
      WHERE snapshot_date = CURRENT_DATE - 1
    )
    SELECT
      t.shortage_key,
      CASE
        WHEN y.shortage_key IS NULL THEN 'NEW'
        WHEN (y.status IS DISTINCT FROM t.status) AND (t.status ILIKE '%resolved%') THEN 'RESOLVED'
        WHEN y.record_hash <> t.record_hash THEN 'UPDATED'
        ELSE NULL
      END AS change_type
    FROM today t
    LEFT JOIN yday y USING (shortage_key)
    WHERE
      y.shortage_key IS NULL
      OR (y.status IS DISTINCT FROM t.status AND t.status ILIKE '%resolved%')
      OR (y.record_hash <> t.record_hash);
    """

    with engine.begin() as conn:
        rows = conn.execute(text(sql)).fetchall()

        # delete existing rows for today (if any)
        conn.execute(text("DELETE FROM shortage_changes WHERE change_date = CURRENT_DATE;"))

        # insert rows
        for shortage_key, change_type in rows:
            if change_type is None:
                continue
            conn.execute(
                text("""
                    INSERT INTO shortage_changes (change_date, shortage_key, change_type)
                    VALUES (CURRENT_DATE, :shortage_key, :change_type)
                """),
                {"shortage_key": shortage_key, "change_type": change_type},
            )

    return len(rows)


def run_changes() -> int:
    """Wrapper used by run_pipeline.py"""
    return compute_changes()
