-- 002_contacts_secret_entropy.sql

ALTER TABLE contacts ADD COLUMN secret_entropy TEXT;
