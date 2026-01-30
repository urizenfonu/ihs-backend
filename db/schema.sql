-- Sites table
CREATE TABLE IF NOT EXISTS sites (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id INTEGER,
  name TEXT NOT NULL,
  region TEXT NOT NULL,
  zone TEXT,
  state TEXT,
  cluster_code TEXT,
  zone_external_id INTEGER,
  is_lagos BOOLEAN DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Assets table
CREATE TABLE IF NOT EXISTS assets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id INTEGER,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  site_id INTEGER NOT NULL,
  last_reading_timestamp DATETIME,
  tenant_channels TEXT,
  config TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (site_id) REFERENCES sites(id)
);

-- Composite Rules table (source of truth for thresholds)
CREATE TABLE IF NOT EXISTS composite_rules (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  severity TEXT NOT NULL,
  category TEXT NOT NULL,
  rule_type TEXT NOT NULL,
  enabled BOOLEAN DEFAULT 1,
  conditions TEXT NOT NULL,
  logical_operator TEXT DEFAULT 'AND',
  time_window_minutes INTEGER,
  aggregation_type TEXT,
  applies_to TEXT DEFAULT 'all',
  region_id TEXT,
  cluster_id TEXT,
  site_id TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Thresholds table (synced from composite_rules)
CREATE TABLE IF NOT EXISTS thresholds (
  id TEXT PRIMARY KEY,
  category TEXT NOT NULL,
  parameter TEXT NOT NULL,
  condition TEXT NOT NULL,
  value REAL NOT NULL,
  unit TEXT NOT NULL,
  severity TEXT NOT NULL,
  enabled BOOLEAN DEFAULT 1,
  description TEXT,
  sites TEXT DEFAULT '[]',
  trigger_count INTEGER DEFAULT 0,
  last_triggered DATETIME,
  applies_to TEXT DEFAULT 'all',
  region_id TEXT,
  cluster_id TEXT,
  site_id TEXT,
  location_name TEXT,
  conditions TEXT DEFAULT NULL,
  logic_operator TEXT DEFAULT 'AND',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Alarms table
CREATE TABLE IF NOT EXISTS alarms (
  id TEXT PRIMARY KEY,
  timestamp DATETIME NOT NULL,
  site TEXT NOT NULL,
  region TEXT NOT NULL,
  severity TEXT NOT NULL,
  category TEXT NOT NULL,
  message TEXT NOT NULL,
  status TEXT DEFAULT 'active',
  details TEXT,
  threshold_id TEXT,
  composite_rule_id TEXT,
  asset_id INTEGER,
  reading_id INTEGER,
  source TEXT DEFAULT 'excel',
  acknowledged_at DATETIME,
  acknowledged_by TEXT,
  resolved_at DATETIME,
  resolved_by TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (threshold_id) REFERENCES thresholds(id),
  FOREIGN KEY (composite_rule_id) REFERENCES composite_rules(id),
  FOREIGN KEY (asset_id) REFERENCES assets(id)
);

-- Readings table (cache latest readings for threshold evaluation)
CREATE TABLE IF NOT EXISTS readings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  asset_id INTEGER NOT NULL,
  reading_type TEXT NOT NULL,
  timestamp DATETIME NOT NULL,
  data TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (asset_id) REFERENCES assets(id)
);

-- Generated Reports table
CREATE TABLE IF NOT EXISTS generated_reports (
  id TEXT PRIMARY KEY,
  report_type TEXT NOT NULL,
  generated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  period_days INTEGER NOT NULL,
  filters TEXT,
  summary TEXT NOT NULL,
  data TEXT NOT NULL,
  file_url TEXT,
  file_size_kb INTEGER,
  status TEXT DEFAULT 'completed',
  created_by TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_alarms_status ON alarms(status);
CREATE INDEX IF NOT EXISTS idx_alarms_severity ON alarms(severity);
CREATE INDEX IF NOT EXISTS idx_alarms_category ON alarms(category);
CREATE INDEX IF NOT EXISTS idx_alarms_site ON alarms(site);
CREATE INDEX IF NOT EXISTS idx_alarms_timestamp ON alarms(timestamp);
CREATE INDEX IF NOT EXISTS idx_composite_rules_enabled ON composite_rules(enabled, category);
CREATE INDEX IF NOT EXISTS idx_thresholds_enabled ON thresholds(enabled);
CREATE INDEX IF NOT EXISTS idx_thresholds_category ON thresholds(category);
CREATE INDEX IF NOT EXISTS idx_readings_asset_id ON readings(asset_id);
CREATE INDEX IF NOT EXISTS idx_readings_timestamp ON readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_assets_site_id ON assets(site_id);
CREATE INDEX IF NOT EXISTS idx_assets_type ON assets(type);
CREATE INDEX IF NOT EXISTS idx_sites_name ON sites(name);
CREATE INDEX IF NOT EXISTS idx_reports_type ON generated_reports(report_type);
CREATE INDEX IF NOT EXISTS idx_reports_generated_at ON generated_reports(generated_at DESC);
