-- DDL for SQLite based on Rust Structures (v2)

-- =============================================
-- Main Data Tables
-- =============================================


CREATE TABLE IF NOT EXISTS _migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- src/schema/drive/schema_drive.sql

-- Table: about_drive
-- Source: src/core/state/drives/state.rs -> thread_local statics
-- Description: Stores essential, largely immutable, information about the drive itself.
CREATE TABLE about_drive (
    drive_id TEXT PRIMARY KEY NOT NULL,
    drive_name TEXT NOT NULL,
    canister_id TEXT NOT NULL UNIQUE,          -- Corresponds to CANISTER_ID (PublicKeyICP)
    version TEXT NOT NULL,                     -- Corresponds to VERSION
    drive_state_checksum TEXT NOT NULL,        -- Corresponds to DRIVE_STATE_CHECKSUM
    timestamp_ns INTEGER NOT NULL,    -- Corresponds to DRIVE_STATE_TIMESTAMP_NS (BigInt as string)
    owner_id TEXT NOT NULL,                    -- Corresponds to OWNER_ID
    host_url TEXT NOT NULL,                -- Corresponds to URL_ENDPOINT
    transfer_owner_id TEXT NOT NULL,           -- Corresponds to TRANSFER_OWNER_ID
    spawn_redeem_code TEXT NOT NULL,           -- Corresponds to SPAWN_REDEEM_CODE
    spawn_note TEXT NOT NULL,                  -- Corresponds to SPAWN_NOTE
    nonce_uuid_generated INTEGER NOT NULL,     -- Corresponds to NONCE_UUID_GENERATED
    default_everyone_group_id TEXT,            -- Corresponds to DEFAULT_EVERYONE_GROUP_ID group
    external_id TEXT,
    external_payload TEXT
);

-- Table: contacts
-- Source: src/core/state/contacts/types.rs -> Contact
-- Description: Stores contact information.
-- contacts is an optional table, because user_id is actually cryptographic public keys as userids, so userid can exist completely outside of SQL known database
CREATE TABLE contacts (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to UserID
    name TEXT NOT NULL,
    avatar TEXT,
    email TEXT,
    notifications_url TEXT,
    public_note TEXT,
    private_note TEXT,
    secret_entropy TEXT,
    evm_public_address TEXT NOT NULL,
    icp_principal TEXT NOT NULL UNIQUE,
    seed_phrase TEXT,
    from_placeholder_user_id TEXT,
    redeem_code TEXT,
    created_at INTEGER NOT NULL,
    last_online_ms INTEGER NOT NULL,
    external_id TEXT,
    external_payload TEXT
);

-- Table: api_keys
-- Source: src/core/state/api_keys/types.rs -> ApiKey
-- Description: Stores API keys associated with contacts.
CREATE TABLE api_keys (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to ApiKeyID
    value TEXT NOT NULL UNIQUE, -- Corresponds to ApiKeyValue
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    private_note TEXT,
    created_at INTEGER NOT NULL,
    begins_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    is_revoked INTEGER NOT NULL DEFAULT 0,
    external_id TEXT,
    external_payload TEXT,
    FOREIGN KEY(user_id) REFERENCES contacts(id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Table: drives
-- Source: src/core/state/drives/types.rs -> Drive
-- Description: Stores information about different drives.
CREATE TABLE drives (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to DriveID
    name TEXT NOT NULL,
    icp_principal TEXT NOT NULL,
    public_note TEXT,
    private_note TEXT,
    host_url TEXT NOT NULL,
    last_indexed_ms INTEGER,
    created_at INTEGER NOT NULL,
    external_id TEXT,
    external_payload TEXT
);

-- Table: disks
-- Source: src/core/state/disks/types.rs -> Disk
-- Description: Stores information about storage disks.
CREATE TABLE disks (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to DiskID
    name TEXT NOT NULL,
    disk_type TEXT NOT NULL, -- e.g., 'ICP_CANISTER', 'AWS_BUCKET'
    private_note TEXT,
    public_note TEXT,
    auth_json TEXT, -- Stores credentials, e.g., for AWS S3
    created_at INTEGER NOT NULL,
    root_folder TEXT, -- only null briefly during creation
    trash_folder TEXT, -- only null briefly during creation
    external_id TEXT,
    external_payload TEXT,
    endpoint TEXT
);


-- Table: folders
-- Source: src/core/state/directory/types.rs -> FolderRecord
-- Description: Stores metadata for folders in the directory.
CREATE TABLE folders (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to FolderID
    name TEXT NOT NULL,
    parent_folder_id TEXT,
    full_directory_path TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    subfolder_uuids TEXT[],
    file_uuids TEXT[],
    last_updated_date_ms INTEGER NOT NULL,
    last_updated_by TEXT NOT NULL,
    disk_id TEXT NOT NULL,
    disk_type TEXT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0,
    expires_at INTEGER NOT NULL,
    drive_id TEXT NOT NULL,
    restore_trash_prior_folder_uuid TEXT,
    has_sovereign_permissions INTEGER NOT NULL DEFAULT 0,
    shortcut_to TEXT,
    notes TEXT,
    external_id TEXT,
    external_payload TEXT,
    FOREIGN KEY(parent_folder_id) REFERENCES folders(id) ON DELETE SET NULL,
    FOREIGN KEY(disk_id) REFERENCES disks(id) ON DELETE CASCADE,
    FOREIGN KEY(drive_id) REFERENCES drives(id),
    FOREIGN KEY(shortcut_to) REFERENCES folders(id) ON DELETE SET NULL
);

-- Table: files
-- Source: src/core/state/directory/types.rs -> FileRecord
-- Description: Stores metadata for files in the directory. This table holds the current version.
CREATE TABLE files (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to FileID
    name TEXT NOT NULL,
    parent_folder_id TEXT NOT NULL,
    version_id TEXT NOT NULL UNIQUE, -- Points to the current version in file_versions
    extension TEXT NOT NULL,
    full_directory_path TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    disk_id TEXT NOT NULL,
    disk_type TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    raw_url TEXT NOT NULL,
    last_updated_date_ms INTEGER NOT NULL,
    last_updated_by TEXT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0,
    drive_id TEXT NOT NULL,
    upload_status TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    restore_trash_prior_folder_uuid TEXT,
    has_sovereign_permissions INTEGER NOT NULL DEFAULT 0,
    shortcut_to TEXT,
    notes TEXT,
    external_id TEXT,
    external_payload TEXT,
    FOREIGN KEY(parent_folder_id) REFERENCES folders(id),
    FOREIGN KEY(disk_id) REFERENCES disks(id) ON DELETE CASCADE,
    FOREIGN KEY(drive_id) REFERENCES drives(id),
    FOREIGN KEY(shortcut_to) REFERENCES files(id) ON DELETE SET NULL
);

-- Table: file_versions
-- Source: Derived from FileRecord versioning fields
-- Description: Stores historical versions of files.
CREATE TABLE file_versions (
    version_id TEXT PRIMARY KEY NOT NULL, -- Corresponds to FileVersionID
    file_id TEXT NOT NULL, -- The logical file this is a version of
    name TEXT NOT NULL,
    file_version INTEGER NOT NULL,
    prior_version_id TEXT,
    next_version_id TEXT,
    extension TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    disk_id TEXT NOT NULL,
    disk_type TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    raw_url TEXT NOT NULL,
    notes TEXT,
    external_id TEXT,
    external_payload TEXT,
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
    FOREIGN KEY(prior_version_id) REFERENCES file_versions(version_id) ON DELETE SET NULL,
    UNIQUE (file_id, file_version)
);

-- Table: groups
-- Source: src/core/state/groups/types.rs -> Group
-- Description: Stores user groups.
CREATE TABLE groups (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to GroupID
    name TEXT NOT NULL,
    owner TEXT NOT NULL,
    avatar TEXT,
    public_note TEXT,
    private_note TEXT,
    created_at INTEGER NOT NULL,
    last_modified_at INTEGER NOT NULL,
    drive_id TEXT NOT NULL,
    host_url TEXT NOT NULL,
    external_id TEXT,
    external_payload TEXT,
    FOREIGN KEY(drive_id) REFERENCES drives(id)
);

-- Table: group_invites
-- Source: src/core/state/group_invites/types.rs -> GroupInvite
-- Description: Stores invitations for users to join groups.
CREATE TABLE group_invites (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to GroupInviteID
    group_id TEXT NOT NULL,
    inviter_id TEXT NOT NULL,
    invitee_type TEXT NOT NULL, -- 'USER', 'PLACEHOLDER', 'PUBLIC'
    invitee_id TEXT, -- UserID or PlaceholderID, NULL if public
    role TEXT NOT NULL, -- 'ADMIN', 'MEMBER'
    note TEXT NOT NULL,
    active_from INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    last_modified_at INTEGER NOT NULL,
    redeem_code TEXT,
    from_placeholder_invitee TEXT,
    external_id TEXT,
    external_payload TEXT,
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
);

-- Table: labels
-- Source: src/core/state/labels/types.rs -> Label
-- Description: Defines available labels (tags) that can be applied to resources.
CREATE TABLE labels (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to LabelID
    value TEXT NOT NULL UNIQUE, -- The actual label text, e.g., 'important'
    public_note TEXT,
    private_note TEXT,
    color TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    last_updated_date_ms INTEGER NOT NULL,
    external_id TEXT,
    external_payload TEXT
);

-- Table: permissions_directory
-- Source: src/core/state/permissions/types.rs -> DirectoryPermission
-- Description: Stores permissions for directory resources (files/folders).
CREATE TABLE permissions_directory (
    id TEXT PRIMARY KEY NOT NULL,
    resource_type TEXT NOT NULL, -- 'File' or 'Folder'
    resource_id TEXT NOT NULL,
    resource_path TEXT NOT NULL,
    grantee_type TEXT NOT NULL, -- 'Public', 'User', 'Group', 'Placeholder'
    grantee_id TEXT, -- UserID, GroupID, or PlaceholderID; NULL if public
    granted_by TEXT NOT NULL,
    begin_date_ms INTEGER NOT NULL,
    expiry_date_ms INTEGER NOT NULL,
    inheritable INTEGER NOT NULL,
    note TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    last_modified_at INTEGER NOT NULL,
    redeem_code TEXT,
    from_placeholder_grantee TEXT,
    metadata_type TEXT, -- 'Labels', 'DirectoryPassword'
    metadata_content TEXT,
    external_id TEXT,
    external_payload TEXT
);

-- Table: permissions_system
-- Source: src/core/state/permissions/types.rs -> SystemPermission
-- Description: Stores permissions for system-level resources (tables/records).
CREATE TABLE permissions_system (
    id TEXT PRIMARY KEY NOT NULL,
    resource_type TEXT NOT NULL, -- 'Table' or 'Record'
    resource_identifier TEXT NOT NULL, -- e.g., 'DRIVES' or 'DriveID_xyz'
    grantee_type TEXT NOT NULL, -- 'Public', 'User', 'Group', 'Placeholder'
    grantee_id TEXT, -- UserID, GroupID, or PlaceholderID; NULL if public
    granted_by TEXT NOT NULL,
    begin_date_ms INTEGER NOT NULL,
    expiry_date_ms INTEGER NOT NULL,
    note TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    last_modified_at INTEGER NOT NULL,
    redeem_code TEXT,
    from_placeholder_grantee TEXT,
    metadata_type TEXT,
    metadata_content TEXT,
    external_id TEXT,
    external_payload TEXT
);

-- Table: webhooks
-- Source: src/core/state/webhooks/types.rs -> Webhook
-- Description: Stores webhook configurations.
CREATE TABLE webhooks (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    alt_index TEXT NOT NULL,
    event TEXT NOT NULL, -- e.g., 'file.created'
    signature TEXT NOT NULL,
    note TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    filters TEXT, -- JSON formatted string of filters
    external_id TEXT,
    external_payload TEXT,
    created_at INTEGER NOT NULL
);

-- Table: purchases
-- Description: Stores information about purchases.
CREATE TABLE purchases (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to PurchaseID
    template_id TEXT,
    vendor_name TEXT NOT NULL,
    vendor_id TEXT NOT NULL, -- Corresponds to UserID
    status TEXT NOT NULL, -- Corresponds to PurchaseStatus enum
    description TEXT,
    about_url TEXT,
    run_url TEXT,
    billing_url TEXT,
    support_url TEXT,
    delivery_url TEXT,
    verification_url TEXT,
    installation_url TEXT,
    title TEXT NOT NULL,
    subtitle TEXT,
    pricing TEXT,
    next_delivery_date INTEGER,
    vendor_notes TEXT,
    notes TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_updated_at INTEGER NOT NULL,
    tracer TEXT,
    external_id TEXT,
    external_payload TEXT
);


-- Table: shortlinks
-- Description: Stores information about shortlinks.
CREATE TABLE shortlinks (
    id TEXT PRIMARY KEY NOT NULL, -- Corresponds to slug
    url TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    created_by TEXT
);


-- Table: contact_id_superswap_history
-- Source: src/core/state/drives/state.rs -> HISTORY_SUPERSWAP_USERID
-- Description: Tracks the history of contact ID changes.
CREATE TABLE contact_id_superswap_history (
    old_user_id TEXT NOT NULL,
    new_user_id TEXT NOT NULL,
    swapped_at INTEGER NOT NULL,
    PRIMARY KEY (old_user_id, new_user_id)
);


-- =============================================
-- Junction Tables for Many-to-Many Relationships
-- =============================================


-- Junction Table for `Contact.past_user_ids`
CREATE TABLE contact_past_ids (
    user_id TEXT NOT NULL,
    past_user_id TEXT NOT NULL,
    PRIMARY KEY (user_id, past_user_id)
);

-- Junction Table for `DirectoryPermission.permission_types`
CREATE TABLE permissions_directory_types (
    permission_id TEXT NOT NULL,
    permission_type TEXT NOT NULL,
    PRIMARY KEY (permission_id, permission_type),
    FOREIGN KEY(permission_id) REFERENCES permissions_directory(id) ON DELETE CASCADE
);

-- Junction Table for `SystemPermission.permission_types`
CREATE TABLE permissions_system_types (
    permission_id TEXT NOT NULL,
    permission_type TEXT NOT NULL,
    PRIMARY KEY (permission_id, permission_type),
    FOREIGN KEY(permission_id) REFERENCES permissions_system(id) ON DELETE CASCADE
);

-- Junction tables for `labels: Vec<LabelStringValue>` on various resources.
CREATE TABLE api_key_labels (
    api_key_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (api_key_id, label_id),
    FOREIGN KEY(api_key_id) REFERENCES api_keys(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE contact_labels (
    user_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (user_id, label_id),
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE file_labels (
    file_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (file_id, label_id),
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE folder_labels (
    folder_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (folder_id, label_id),
    FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE disk_labels (
    disk_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (disk_id, label_id),
    FOREIGN KEY(disk_id) REFERENCES disks(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE drive_labels (
    drive_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (drive_id, label_id),
    FOREIGN KEY(drive_id) REFERENCES drives(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE group_labels (
    group_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (group_id, label_id),
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE group_invite_labels (
    invite_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (invite_id, label_id),
    FOREIGN KEY(invite_id) REFERENCES group_invites(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE permission_directory_labels (
    permission_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (permission_id, label_id),
    FOREIGN KEY(permission_id) REFERENCES permissions_directory(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE permission_system_labels (
    permission_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (permission_id, label_id),
    FOREIGN KEY(permission_id) REFERENCES permissions_system(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE webhook_labels (
    webhook_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (webhook_id, label_id),
    FOREIGN KEY(webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE label_labels (
    parent_label_id TEXT NOT NULL,
    child_label_id TEXT NOT NULL,
    PRIMARY KEY (parent_label_id, child_label_id),
    FOREIGN KEY(parent_label_id) REFERENCES labels(id) ON DELETE CASCADE,
    FOREIGN KEY(child_label_id) REFERENCES labels(id) ON DELETE CASCADE
);

CREATE TABLE purchase_labels (
    purchase_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (purchase_id, label_id),
    FOREIGN KEY(purchase_id) REFERENCES purchases(id) ON DELETE CASCADE,
    FOREIGN KEY(label_id) REFERENCES labels(id) ON DELETE CASCADE
);


-- Table: external_id_mappings
-- Description: Stores mappings from an ExternalID to a list of internal IDs (e.g., DriveID, FileID, FolderID, etc.)
CREATE TABLE external_id_mappings (
    external_id TEXT PRIMARY KEY NOT NULL,
    internal_ids TEXT NOT NULL
);

CREATE TABLE uuid_claimed (
    uuid TEXT PRIMARY KEY NOT NULL,
    claimed INTEGER NOT NULL DEFAULT 1
);



-- FTS5 Virtual Table for fuzzy text searching
CREATE VIRTUAL TABLE search_fts USING fts5(
  searchable_string,
  title,
  preview,
  resource_id,
  category,
  metadata,
  created_at,
  updated_at,
  tokenize='trigram'
);

-- Shadow table for fast lookups between resource_id and FTS rowid
CREATE TABLE fts_lookup (
    resource_id TEXT PRIMARY KEY,
    fts_rowid INTEGER
);


-- Files Triggers
CREATE TRIGGER files_ai AFTER INSERT ON files BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (new.name || ' ' || new.full_directory_path || ' ' || new.id, new.name, new.full_directory_path, new.id, 'FILES', new.external_payload, new.created_at, new.last_updated_date_ms);
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

CREATE TRIGGER files_au AFTER UPDATE ON files BEGIN
  UPDATE search_fts SET
    searchable_string = new.name || ' ' || new.full_directory_path || ' ' || new.id,
    title = new.name,
    preview = new.full_directory_path,
    updated_at = new.last_updated_date_ms,
    metadata = new.external_payload
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

CREATE TRIGGER files_ad AFTER DELETE ON files BEGIN
  DELETE FROM search_fts WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = old.id);
  DELETE FROM fts_lookup WHERE resource_id = old.id;
END;

-- Folders Triggers
CREATE TRIGGER folders_ai AFTER INSERT ON folders BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (new.name || ' ' || new.full_directory_path || ' ' || new.id, new.name, new.full_directory_path, new.id, 'FOLDERS', new.external_payload, new.created_at, new.last_updated_date_ms);
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

CREATE TRIGGER folders_au AFTER UPDATE ON folders BEGIN
  UPDATE search_fts SET
    searchable_string = new.name || ' ' || new.full_directory_path || '' || new.id,
    title = new.name,
    preview = new.full_directory_path,
    updated_at = new.last_updated_date_ms,
    metadata = new.external_payload
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

CREATE TRIGGER folders_ad AFTER DELETE ON folders BEGIN
  DELETE FROM search_fts WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = old.id);
  DELETE FROM fts_lookup WHERE resource_id = old.id;
END;

-- Contacts Triggers
CREATE TRIGGER contacts_ai AFTER INSERT ON contacts BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (new.name || ' ' || new.email || ' ' || new.id || ' ' || new.evm_public_address, new.name, new.email, new.id, 'CONTACTS', new.external_payload, new.created_at, new.last_online_ms);
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

CREATE TRIGGER contacts_au AFTER UPDATE ON contacts BEGIN
  UPDATE search_fts SET
    searchable_string = new.name || ' ' || new.email || ' ' || new.id || ' ' || new.evm_public_address,
    title = new.name,
    preview = new.email,
    updated_at = new.last_online_ms,
    metadata = new.external_payload
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

CREATE TRIGGER contacts_ad AFTER DELETE ON contacts BEGIN
  DELETE FROM search_fts WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = old.id);
  DELETE FROM fts_lookup WHERE resource_id = old.id;
END;


-- Disks Triggers
CREATE TRIGGER disks_ai AFTER INSERT ON disks BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (new.name || ' ' || new.disk_type || ' ' || new.id, new.name, new.disk_type, new.id, 'DISKS', new.external_payload, new.created_at, new.created_at);
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

CREATE TRIGGER disks_au AFTER UPDATE ON disks BEGIN
  UPDATE search_fts SET
    searchable_string = new.name || ' ' || new.disk_type || ' ' || new.id,
    title = new.name,
    preview = new.disk_type,
    updated_at = new.created_at,
    metadata = new.external_payload
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

CREATE TRIGGER disks_ad AFTER DELETE ON disks BEGIN
  DELETE FROM search_fts WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = old.id);
  DELETE FROM fts_lookup WHERE resource_id = old.id;
END;


-- Drives Triggers
CREATE TRIGGER drives_ai AFTER INSERT ON drives BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (new.name || ' ' || new.id || ' ' || new.host_url, new.name, new.host_url, new.id, 'DRIVES', new.external_payload, new.created_at, new.created_at);
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

CREATE TRIGGER drives_au AFTER UPDATE ON drives BEGIN
  UPDATE search_fts SET
    searchable_string = new.name || ' ' || new.id || ' ' || new.host_url,
    title = new.name,
    preview = new.host_url,
    updated_at = new.created_at,
    metadata = new.external_payload
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

CREATE TRIGGER drives_ad AFTER DELETE ON drives BEGIN
  DELETE FROM search_fts WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = old.id);
  DELETE FROM fts_lookup WHERE resource_id = old.id;
END;

-- Groups Triggers
CREATE TRIGGER groups_ai AFTER INSERT ON groups BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (new.name || ' ' || new.id || ' ' || new.host_url, new.name, new.host_url, new.id, 'GROUPS', new.external_payload, new.created_at, new.last_modified_at);
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

CREATE TRIGGER groups_au AFTER UPDATE ON groups BEGIN
  UPDATE search_fts SET
    searchable_string = new.name || ' ' || new.id || ' ' || new.host_url,
    title = new.name,
    preview = new.host_url,
    updated_at = new.last_modified_at,
    metadata = new.external_payload
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

CREATE TRIGGER groups_ad AFTER DELETE ON groups BEGIN
  DELETE FROM search_fts WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = old.id);
  DELETE FROM fts_lookup WHERE resource_id = old.id;
END;

-- Purchases Triggers
CREATE TRIGGER purchases_ai AFTER INSERT ON purchases BEGIN
  INSERT INTO search_fts(searchable_string, title, preview, resource_id, category, metadata, created_at, updated_at)
  VALUES (new.title || ' ' || new.subtitle || ' ' || new.vendor_name || ' ' || new.id, new.title, new.vendor_name, new.id, 'PURCHASES', new.external_payload, new.created_at, new.last_updated_at);
  INSERT INTO fts_lookup (resource_id, fts_rowid) VALUES (new.id, last_insert_rowid());
END;

CREATE TRIGGER purchases_au AFTER UPDATE ON purchases BEGIN
  UPDATE search_fts SET
    searchable_string = new.title || ' ' || new.subtitle || ' ' || new.vendor_name || ' ' || new.id,
    title = new.title,
    preview = new.vendor_name,
    updated_at = new.last_updated_at,
    metadata = new.external_payload
  WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = new.id);
END;

CREATE TRIGGER purchases_ad AFTER DELETE ON purchases BEGIN
  DELETE FROM search_fts WHERE rowid = (SELECT fts_rowid FROM fts_lookup WHERE resource_id = old.id);
  DELETE FROM fts_lookup WHERE resource_id = old.id;
END;

-- =============================================
-- Indexes for Performance
-- =============================================

CREATE INDEX idx_contacts_icp_principal ON contacts(icp_principal);
CREATE INDEX idx_contacts_created_at ON contacts(created_at);

CREATE INDEX idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX idx_api_keys_value ON api_keys(value);

CREATE INDEX idx_folders_parent_folder_id ON folders(parent_folder_id);
CREATE INDEX idx_folders_drive_id ON folders(drive_id);
CREATE INDEX idx_folders_disk_id ON folders(disk_id);

CREATE INDEX idx_files_parent_folder_id ON files(parent_folder_id);
CREATE INDEX idx_files_drive_id ON files(drive_id);
CREATE INDEX idx_files_disk_id ON files(disk_id);

CREATE INDEX idx_group_invites_group_id ON group_invites(group_id);
CREATE INDEX idx_group_invites_invitee_id ON group_invites(invitee_id);

CREATE INDEX idx_permissions_directory_resource ON permissions_directory(resource_type, resource_id);
CREATE INDEX idx_permissions_directory_grantee ON permissions_directory(grantee_type, grantee_id);

CREATE INDEX idx_permissions_system_resource ON permissions_system(resource_type, resource_identifier);
CREATE INDEX idx_permissions_system_grantee ON permissions_system(grantee_type, grantee_id);

CREATE INDEX idx_webhooks_alt_index ON webhooks(alt_index);
CREATE INDEX idx_webhooks_event ON webhooks(event);

CREATE INDEX idx_external_id_mappings ON external_id_mappings(external_id);
CREATE INDEX idx_uuid_claimed ON uuid_claimed(uuid);

CREATE INDEX idx_purchases_vendor_id ON purchases(vendor_id);
CREATE INDEX idx_purchases_status ON purchases(status);
CREATE INDEX idx_purchases_created_at ON purchases(created_at);


CREATE INDEX idx_shortlinks_url ON shortlinks(url);