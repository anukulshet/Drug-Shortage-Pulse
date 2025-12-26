from sqlalchemy import text
from .db import engine

def write_today_snapshot():
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO shortages_snapshot (snapshot_date, shortage_key, record_hash, status, update_date)
            SELECT CURRENT_DATE, shortage_key, record_hash, status, update_date
            FROM shortages_clean
            ON CONFLICT (snapshot_date, shortage_key) DO UPDATE SET
              record_hash = EXCLUDED.record_hash,
              status = EXCLUDED.status,
              update_date = EXCLUDED.update_date;
        """))
