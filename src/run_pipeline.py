from src.ingest import ingest_raw
from src.transform import run_transform
from src.snapshot import write_today_snapshot
from src.changes import compute_changes
from src.kpis import refresh_daily_kpis

def main():
    ingest_raw(max_records=500)
    run_transform()
    write_today_snapshot()
    compute_changes()
    refresh_daily_kpis()
    print("✅ Pipeline complete")

if __name__ == "__main__":
    main()
