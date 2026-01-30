CREATE TABLE IF NOT EXISTS sync_metadata (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_sync_time DATETIME,
  last_success_time DATETIME,
  status TEXT,
  sites_synced INTEGER DEFAULT 0,
  assets_synced INTEGER DEFAULT 0,
  readings_synced INTEGER DEFAULT 0,
  errors TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO sync_metadata (id, status) VALUES (1, 'never_run');
