-- src/schema/factory/schema_factory.sql

CREATE TABLE IF NOT EXISTS _migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Table: factory_admins
-- Description: Logs administrative notes and the user ID of the administrator.
CREATE TABLE factory_admins (
    admin_user_id TEXT NOT NULL,          -- The ID of the administrator user
    note TEXT                              -- A note or description from the administrator
);

-- Table: factory_api_keys
-- Source: src/core/state/api_keys/types.rs -> ApiKey
-- Description: Stores API keys used to authenticate with the factory canister.
CREATE TABLE factory_api_keys (
    id TEXT PRIMARY KEY NOT NULL,         -- Corresponds to ApiKey.id (ApiKeyID)
    value TEXT NOT NULL UNIQUE,           -- Corresponds to ApiKey.value (ApiKeyValue)
    user_id TEXT NOT NULL,                -- Corresponds to ApiKey.user_id (UserID)
    name TEXT NOT NULL,                   -- Corresponds to ApiKey.name
    created_at INTEGER NOT NULL,          -- Corresponds to ApiKey.created_at (Unix timestamp in ms)
    expires_at INTEGER NOT NULL,          -- Corresponds to ApiKey.expires_at (-1 for no expiry)
    is_revoked INTEGER NOT NULL DEFAULT 0 -- Corresponds to ApiKey.is_revoked
);

-- Index for fast lookup by API key value
CREATE INDEX idx_factory_api_keys_value ON factory_api_keys(value);
-- Index for finding all keys belonging to a user
CREATE INDEX idx_factory_api_keys_user_id ON factory_api_keys(user_id);

-- Table: giftcard_spawn_orgs
-- Source: src/core/state/drives/types.rs -> GiftcardSpawnOrg
-- Description: Stores "gift cards" that authorize the creation (spawning) of a new drive.
CREATE TABLE giftcard_spawn_orgs (
    id TEXT PRIMARY KEY NOT NULL,          -- Corresponds to GiftcardSpawnOrg.id (GiftcardSpawnOrgID)
    usd_revenue_cents INTEGER NOT NULL,    -- Corresponds to GiftcardSpawnOrg.usd_revenue_cents
    note TEXT,                             -- Corresponds to GiftcardSpawnOrg.note
    gas_cycles_included INTEGER NOT NULL,  -- Corresponds to GiftcardSpawnOrg.gas_cycles_included
    timestamp_ms INTEGER NOT NULL,         -- Corresponds to GiftcardSpawnOrg.timestamp_ms
    external_id TEXT UNIQUE,               -- Corresponds to GiftcardSpawnOrg.external_id (e.g., Stripe charge ID)
    redeemed INTEGER NOT NULL DEFAULT 0,   -- Corresponds to GiftcardSpawnOrg.redeemed
    bundled_default_disk TEXT                    -- Corresponds to GiftcardSpawnOrg.bundled_default_disk
);

-- Table: factory_spawn_history
-- Source: src/core/state/drives/types.rs -> IFactorySpawnHistoryRecord
-- Description: Logs the event of a new drive being spawned using a gift card.
CREATE TABLE factory_spawn_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id TEXT NOT NULL,                -- Corresponds to IFactorySpawnHistoryRecord.owner_id
    drive_id TEXT NOT NULL,                -- Corresponds to IFactorySpawnHistoryRecord.drive_id
    host TEXT NOT NULL,                -- Corresponds to IFactorySpawnHistoryRecord.host
    version TEXT NOT NULL,                 -- Corresponds to IFactorySpawnHistoryRecord.version
    note TEXT,                             -- Corresponds to IFactorySpawnHistoryRecord.note
    giftcard_id TEXT NOT NULL UNIQUE,      -- Corresponds to IFactorySpawnHistoryRecord.giftcard_id
    gas_cycles_included INTEGER NOT NULL,  -- Corresponds to IFactorySpawnHistoryRecord.gas_cycles_included
    timestamp_ms INTEGER NOT NULL,         -- Corresponds to IFactorySpawnHistoryRecord.timestamp_ms
    FOREIGN KEY(giftcard_id) REFERENCES giftcard_spawn_orgs(id)
);

-- Junction Table: user_giftcard_spawn_orgs
-- Source: src/core/state/drives/state.rs -> USER_TO_GIFTCARDS_SPAWNORG_HASHTABLE
-- Description: Maps users to the spawn gift cards they are associated with.
CREATE TABLE user_giftcard_spawn_orgs (
    user_id TEXT NOT NULL,
    giftcard_id TEXT NOT NULL,
    PRIMARY KEY (user_id, giftcard_id),
    FOREIGN KEY(giftcard_id) REFERENCES giftcard_spawn_orgs(id) ON DELETE CASCADE
);

-- Table: giftcard_refuels
-- Source: src/core/state/drives/types.rs -> GiftcardRefuel
-- Description: Stores "gift cards" that authorize adding cycles to an existing drive.
CREATE TABLE giftcard_refuels (
    id TEXT PRIMARY KEY NOT NULL,          -- Corresponds to GiftcardRefuel.id (GiftcardRefuelID)
    usd_revenue_cents INTEGER NOT NULL,    -- Corresponds to GiftcardRefuel.usd_revenue_cents
    note TEXT,                             -- Corresponds to GiftcardRefuel.note
    gas_cycles_included INTEGER NOT NULL,  -- Corresponds to GiftcardRefuel.gas_cycles_included
    timestamp_ms INTEGER NOT NULL,         -- Corresponds to GiftcardRefuel.timestamp_ms
    external_id TEXT UNIQUE,               -- Corresponds to GiftcardRefuel.external_id
    redeemed INTEGER NOT NULL DEFAULT 0    -- Corresponds to GiftcardRefuel.redeemed
);

-- Table: factory_refuel_history
-- Source: src/core/state/drives/types.rs -> IFactoryRefuelHistoryRecord
-- Description: Logs the event of a drive being refueled using a gift card.
CREATE TABLE factory_refuel_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    note TEXT,                             -- Corresponds to IFactoryRefuelHistoryRecord.note
    giftcard_id TEXT NOT NULL UNIQUE,      -- Corresponds to IFactoryRefuelHistoryRecord.giftcard_id
    gas_cycles_included INTEGER NOT NULL,  -- Corresponds to IFactoryRefuelHistoryRecord.gas_cycles_included
    timestamp_ms INTEGER NOT NULL,         -- Corresponds to IFactoryRefuelHistoryRecord.timestamp_ms
    icp_principal TEXT NOT NULL,           -- Corresponds to IFactoryRefuelHistoryRecord.icp_principal
    FOREIGN KEY(giftcard_id) REFERENCES giftcard_refuels(id)
);

-- Junction Table: user_giftcard_refuels
-- Source: src/core/state/drives/state.rs -> USER_TO_GIFTCARDS_REFUEL_HASHTABLE
-- Description: Maps users to the refuel gift cards they are associated with.
CREATE TABLE user_giftcard_refuels (
    user_id TEXT NOT NULL,
    giftcard_id TEXT NOT NULL,
    PRIMARY KEY (user_id, giftcard_id),
    FOREIGN KEY(giftcard_id) REFERENCES giftcard_refuels(id) ON DELETE CASCADE
);

