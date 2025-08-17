-- 005_disk_auto_expire_ms.sql

ALTER TABLE disks ADD COLUMN autoexpire_ms INTEGER;
ALTER TABLE about_drive ADD COLUMN bundled_default_disk TEXT;

