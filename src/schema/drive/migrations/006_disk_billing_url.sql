-- 006_disk_billing_url.sql

-- Step 1: Add the new column and migrate the data
ALTER TABLE disks ADD COLUMN billing_url TEXT;

UPDATE disks SET billing_url = endpoint;

-- Step 2: Drop the old column
ALTER TABLE disks DROP COLUMN endpoint;