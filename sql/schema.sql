CREATE TABLE IF NOT EXISTS raw_shortages (
  id BIGSERIAL PRIMARY KEY,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta_last_updated DATE,
  payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS shortages_clean (
  shortage_key TEXT PRIMARY KEY,
  generic_name TEXT,
  proprietary_name TEXT,
  dosage_form TEXT,
  status TEXT,
  therapeutic_category TEXT,
  initial_posting_date DATE,
  update_date DATE,
  change_date DATE,
  discontinued_date DATE,
  update_type TEXT,
  company_name TEXT,
  record_hash TEXT NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shortages_snapshot (
  snapshot_date DATE NOT NULL,
  shortage_key TEXT NOT NULL,
  record_hash TEXT NOT NULL,
  status TEXT,
  update_date DATE,
  PRIMARY KEY (snapshot_date, shortage_key)
);

CREATE TABLE IF NOT EXISTS shortage_changes (
  id BIGSERIAL PRIMARY KEY,
  change_date DATE NOT NULL,
  shortage_key TEXT NOT NULL,
  change_type TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_kpis (
  kpi_date DATE PRIMARY KEY,
  new_shortages INT NOT NULL,
  active_shortages INT NOT NULL,
  resolved_shortages INT NOT NULL,
  avg_active_duration_days NUMERIC,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
