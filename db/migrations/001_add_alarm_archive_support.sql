CREATE INDEX IF NOT EXISTS idx_alarms_threshold_id ON alarms(threshold_id);

CREATE VIEW IF NOT EXISTS threshold_alarm_counts AS
SELECT
    threshold_id,
    COUNT(*) as active_alarm_count
FROM alarms
WHERE status IN ('active', 'acknowledged')
  AND threshold_id IS NOT NULL
GROUP BY threshold_id;
