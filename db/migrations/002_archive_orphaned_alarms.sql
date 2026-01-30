UPDATE alarms
SET status = 'archived'
WHERE threshold_id IS NOT NULL
  AND threshold_id NOT IN (SELECT id FROM thresholds)
  AND status != 'resolved';
