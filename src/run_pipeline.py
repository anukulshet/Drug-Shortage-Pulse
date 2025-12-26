from __future__ import annotations

from .ingest import ingest_raw
from .transform import run_transform
from .snapshot import run_snapshot
from .changes import run_changes
from .kpis import run_kpis


def main():
    ingest_raw(max_records=500)
    run_transform()
    run_snapshot()
    run_changes()
    run_kpis()
    print("Pipeline complete")


if __name__ == "__main__":
    main()
