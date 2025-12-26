from __future__ import annotations
from sqlalchemy import text
from .db import engine


def run_snapshot() -> int:
    """
    Creates/updates today's snapshot in shortages_snapshot.

    snapshot_date = CURRENT_DATE
    One row per shortage_key with its record_hash + status + update_date.

    Returns number of rows inserted/updated (approx; depends on DB driver).
    """
    sql = """
    INSERT INTO shortages_snapshot (
        snapshot_date,
        shortage_key,
        record_hash,
        status,
        update_date
    )
    SELECT
        CURRENT_DATE AS snapshot_date,
        shortage_key,
        record_hash,
        status,
        update_date
    FROM shortages_clean
    ON CONFLICT (snapshot_date, shortage_key)
    DO UPDATE SET
        record_hash = EXCLUDED.record_hash,
        status = EXCLUDED.status,
        update_date = EXCLUDED.update_date;
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        return int(result.rowcount) if result.rowcount is not None else 0
