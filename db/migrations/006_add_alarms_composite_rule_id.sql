-- Add composite_rule_id column to alarms table
ALTER TABLE alarms ADD COLUMN composite_rule_id TEXT DEFAULT NULL;

-- Add foreign key index for performance
CREATE INDEX IF NOT EXISTS idx_alarms_composite_rule_id ON alarms(composite_rule_id);
