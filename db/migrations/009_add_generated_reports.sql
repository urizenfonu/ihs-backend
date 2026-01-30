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
CREATE INDEX IF NOT EXISTS idx_reports_type ON generated_reports(report_type);
CREATE INDEX IF NOT EXISTS idx_reports_generated_at ON generated_reports(generated_at DESC);
