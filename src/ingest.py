import json
import time
import requests
from sqlalchemy import text
from .db import engine
from .config import OPENFDA_API_KEY

BASE_URL = "https://api.fda.gov/drug/shortages.json"

def fetch_page(limit=100, skip=0):
    params = {"limit": limit, "skip": skip}
    if OPENFDA_API_KEY:
        params["api_key"] = OPENFDA_API_KEY
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def ingest_raw(max_records=500, sleep_s=0.15):
    limit = 100
    skip = 0
    inserted_pages = 0

    while skip < max_records:
        data = fetch_page(limit=limit, skip=skip)
        meta_last_updated = (data.get("meta", {}) or {}).get("last_updated")

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO raw_shortages(meta_last_updated, payload)
                    VALUES (CAST(:meta_last_updated AS date), CAST(:payload AS jsonb))
            """),
            {
                "meta_last_updated": meta_last_updated,
                "payload": json.dumps(data),
            },
            )


        inserted_pages += 1
        if not data.get("results"):
            break

        skip += limit
        time.sleep(sleep_s)

    return inserted_pages
