-- Delete alarms tied to Excel-imported assets/sites
DELETE FROM alarms WHERE asset_id IN (
  SELECT id FROM assets WHERE site_id IN (
    SELECT id FROM sites WHERE is_lagos = 1 OR external_id IS NULL
  )
) OR site IN (
  SELECT name FROM sites WHERE is_lagos = 1 OR external_id IS NULL
);

-- Delete all readings associated with Excel-imported assets
DELETE FROM readings WHERE asset_id IN (
  SELECT id FROM assets WHERE site_id IN (
    SELECT id FROM sites WHERE is_lagos = 1 OR external_id IS NULL
  )
);

-- Delete all Excel-imported assets
DELETE FROM assets WHERE site_id IN (
  SELECT id FROM sites WHERE is_lagos = 1 OR external_id IS NULL
);

-- Delete Lagos placeholder site and any sites without external_id
DELETE FROM sites WHERE is_lagos = 1 OR external_id IS NULL;
