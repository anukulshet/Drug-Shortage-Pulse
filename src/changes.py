from sqlalchemy import text
from .db import engine

def compute_changes():
    # Compare today vs yesterday snapshots
    sql = """
    WITH today AS (
      SELECT shortage_key, record_hash, status
      FROM shortages_snapshot
      WHERE snapshot_date = CURRENT_DATE
    ),
    yday AS (
      SELECT shortage_key, record_hash, status
      FROM shortages_snapshot
      WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day'
    )
    SELECT
      t.shortage_key,
      CASE
        WHEN y.shortage_key IS NULL THEN 'NEW'
        WHEN y.record_hash <> t.record_hash THEN 'UPDATED'
        WHEN y.status IS DISTINCT FROM t.status AND t.status ILIKE '%resolved%' THEN 'RESOLVED'
        ELSE NULL
      END AS change_type
    FROM today t
    LEFT JOIN yday y USING (shortage_key)
    WHERE y.shortage_key IS NULL OR y.record_hash <> t.record_hash
       OR (y.status IS DISTINCT FROM t.status AND t.status ILIKE '%resolved%');
    """

    with engine.begin() as conn:
        rows = conn.execute(text(sql)).fetchall()

        conn.execute(text("DELETE FROM shortage_changes WHERE change_date = CURRENT_DATE;"))

        for shortage_key, change_type in rows:
            conn.execute(
                text("""
                    INSERT INTO shortage_changes (change_date, shortage_key, change_type)
                    VALUES (CURRENT_DATE, :shortage_key, :change_type)
                """),
                {"shortage_key": shortage_key, "change_type": change_type},
            )

    return len(rows)
