-- 007_search_metadata_format.sql
-- Purpose: Ensure search_fts.metadata holds canonical path metadata
-- Format: /{disk_type}/{disk_id}/{file_id or folder_id}

-- Recreate Files triggers to populate metadata as desired
DROP TRIGGER IF EXISTS files_ai;
CREATE TRIGGER files_ai AFTER INSERT ON files BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (
    new.name || ' ' || new.full_directory_path || ' ' || new.id,
    new.name,
    new.full_directory_path,
    new.id,
    'FILES',
    '/' || new.disk_type || '/' || new.disk_id || '/' || new.id,
    new.created_at,
    new.last_updated_date_ms
  );
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

DROP TRIGGER IF EXISTS files_au;
CREATE TRIGGER files_au AFTER UPDATE ON files BEGIN
  UPDATE search_fts SET
    searchable_string = new.name || ' ' || new.full_directory_path || ' ' || new.id,
    title = new.name,
    preview = new.full_directory_path,
    updated_at = new.last_updated_date_ms,
    metadata = '/' || new.disk_type || '/' || new.disk_id || '/' || new.id
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

-- Recreate Folders triggers to populate metadata as desired
DROP TRIGGER IF EXISTS folders_ai;
CREATE TRIGGER folders_ai AFTER INSERT ON folders BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (
    new.name || ' ' || new.full_directory_path || ' ' || new.id,
    new.name,
    new.full_directory_path,
    new.id,
    'FOLDERS',
    '/' || new.disk_type || '/' || new.disk_id || '/' || new.id,
    new.created_at,
    new.last_updated_date_ms
  );
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

DROP TRIGGER IF EXISTS folders_au;
CREATE TRIGGER folders_au AFTER UPDATE ON folders BEGIN
  UPDATE search_fts SET
    searchable_string = new.name || ' ' || new.full_directory_path || ' ' || new.id,
    title = new.name,
    preview = new.full_directory_path,
    updated_at = new.last_updated_date_ms,
    metadata = '/' || new.disk_type || '/' || new.disk_id || '/' || new.id
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

-- Backfill existing FTS rows for FILES and FOLDERS
-- FILES
UPDATE search_fts
SET metadata = (
  SELECT '/' || f.disk_type || '/' || f.disk_id || '/' || f.id
  FROM files f
  WHERE f.id = search_fts.resource_id
)
WHERE category = 'FILES';

-- FOLDERS
UPDATE search_fts
SET metadata = (
  SELECT '/' || fo.disk_type || '/' || fo.disk_id || '/' || fo.id
  FROM folders fo
  WHERE fo.id = search_fts.resource_id
)
WHERE category = 'FOLDERS';
