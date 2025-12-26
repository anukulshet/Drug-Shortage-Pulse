import hashlib
import json
import pandas as pd
from sqlalchemy import text
from .db import engine

def _mk_key(r: dict) -> str:
    # Prefer a stable ID if present; otherwise hash a deterministic set of fields
    if r.get("package_ndc"):
        return f"ndc:{r['package_ndc']}"
    base = "|".join([
        str(r.get("generic_name", "")),
        str(r.get("dosage_form", "")),
        str(r.get("initial_posting_date", "")),
        str(r.get("company_name", "")),
    ]).strip().lower()
    return "hash:" + hashlib.sha1(base.encode("utf-8")).hexdigest()

def _hash_record(d: dict) -> str:
    stable = json.dumps(d, sort_keys=True, default=str)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()

def _to_date(x):
    """Convert API date strings to Python date objects (or None)."""
    if x is None or x == "":
        return None
    try:
        return pd.to_datetime(x, errors="coerce").date()
    except Exception:
        return None

def load_latest_raw_results() -> list[dict]:
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT payload
            FROM raw_shortages
            ORDER BY fetched_at DESC
            LIMIT 1
        """)).fetchone()

    if not row:
        return []

    payload = row[0]
    # SQLAlchemy may return jsonb as dict already; if not, decode
    if isinstance(payload, str):
        payload = json.loads(payload)

    return payload.get("results", []) or []

def normalize(results: list[dict]) -> pd.DataFrame:
    rows = []
    for r in results:
        record = {
            "shortage_key": _mk_key(r),
            "generic_name": r.get("generic_name"),
            "proprietary_name": r.get("proprietary_name"),
            "dosage_form": r.get("dosage_form"),
            "status": r.get("status"),
            "therapeutic_category": (
                (r.get("therapeutic_category") or [None])[0]
                if isinstance(r.get("therapeutic_category"), list)
                else r.get("therapeutic_category")
            ),
            # Convert to Python date objects (much safer than relying on Postgres parsing)
            "initial_posting_date": _to_date(r.get("initial_posting_date")),
            "update_date": _to_date(r.get("update_date")),
            "change_date": _to_date(r.get("change_date")),
            "discontinued_date": _to_date(r.get("discontinued_date")),
            "update_type": r.get("update_type"),
            "company_name": r.get("company_name"),
        }
        record["record_hash"] = _hash_record(record)
        rows.append(record)

    return pd.DataFrame(rows)

def upsert_clean(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    sql = """
    INSERT INTO shortages_clean (
      shortage_key, generic_name, proprietary_name, dosage_form, status, therapeutic_category,
      initial_posting_date, update_date, change_date, discontinued_date, update_type, company_name, record_hash
    )
    VALUES (
      :shortage_key, :generic_name, :proprietary_name, :dosage_form, :status, :therapeutic_category,
      CAST(:initial_posting_date AS date),
      CAST(:update_date AS date),
      CAST(:change_date AS date),
      CAST(:discontinued_date AS date),
      :update_type, :company_name, :record_hash
    )
    ON CONFLICT (shortage_key)
    DO UPDATE SET
      generic_name = EXCLUDED.generic_name,
      proprietary_name = EXCLUDED.proprietary_name,
      dosage_form = EXCLUDED.dosage_form,
      status = EXCLUDED.status,
      therapeutic_category = EXCLUDED.therapeutic_category,
      initial_posting_date = EXCLUDED.initial_posting_date,
      update_date = EXCLUDED.update_date,
      change_date = EXCLUDED.change_date,
      discontinued_date = EXCLUDED.discontinued_date,
      update_type = EXCLUDED.update_type,
      company_name = EXCLUDED.company_name,
      record_hash = EXCLUDED.record_hash,
      ingested_at = NOW();
    """

    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))

    return len(df)

def run_transform():
    results = load_latest_raw_results()
    df = normalize(results)
    return upsert_clean(df)
