-- 001_add_about_factory_table.sql 

CREATE TABLE IF NOT EXISTS about_factory (
    id TEXT PRIMARY KEY NOT NULL,
    version TEXT NOT NULL
);

-- Insert the current version. You should update this value with each new migration.
INSERT INTO about_factory (id, version) VALUES ('about_factory', '0.1.0');