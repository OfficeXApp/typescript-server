-- 001_rename_timestamp_ns.sql

-- This statement will only run successfully on databases that still have the `timestamp_ns` column.
-- It will gracefully fail on databases that have already been migrated, without crashing the process.
ALTER TABLE about_drive RENAME COLUMN timestamp_ns TO timestamp_ms;

-- Update existing data by converting nanoseconds to milliseconds and then to an INTEGER.
UPDATE about_drive SET timestamp_ms = CAST(CAST(timestamp_ms AS REAL) / 1000 AS INTEGER);