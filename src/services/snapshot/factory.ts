// src/services/snapshot/index.ts

import {
  ApiKey,
  ApiKeyID,
  ApiKeyValue,
  DriveID,
  GiftcardSpawnOrgID,
  UserID,
  GiftcardSpawnOrg,
  ICPPrincipalString,
  HostURL,
  FactoryApiKey, // Using FactoryApiKey as per your types, assuming factory_api_keys table maps to it
} from "@officexapp/types";
import { db } from "../database"; // Corrected: Use 'db' for query methods

/**
 * FactoryStateSnapshot interface - matches the Rust FactoryStateSnapshot structure.
 * This is duplicated here for clarity but should ideally be in a shared types file.
 */
export interface FactoryStateSnapshot {
  // System info (factory-wide or primary drive info if this is the main drive for the factory)
  canister_id: ICPPrincipalString; // Corresponds to PublicKeyICP in Rust
  version: string;
  owner_id: UserID;
  host_url: HostURL; // Corresponds to DriveRESTHostURL in Rust

  // API keys state
  apikeys_by_value: Record<ApiKeyValue, ApiKeyID>;
  apikeys_by_id: Record<ApiKeyID, ApiKey>;
  users_apikeys: Record<UserID, ApiKeyID[]>;
  apikeys_history: ApiKeyID[];

  // GiftcardSpawnOrg state
  deployments_by_giftcard_id: Record<GiftcardSpawnOrgID, GiftcardSpawnOrg>;
  historical_giftcards: GiftcardSpawnOrgID[];
  drive_to_giftcard_hashtable: Record<DriveID, GiftcardSpawnOrgID>; // This mapping will be conceptual
  user_to_giftcards_hashtable: Record<UserID, GiftcardSpawnOrgID[]>;
  giftcard_by_id: Record<GiftcardSpawnOrgID, GiftcardSpawnOrg>;

  // Timestamp
  timestamp_ms: string; // Corresponds to u64 in Rust (nanoseconds)
}

interface AboutFactoryRecord {
  canister_id: ICPPrincipalString;
  version: string;
  owner_id: UserID;
  host_url: HostURL;
}

/**
 * Helper function to get system info for the Factory from the `about_factory` table.
 * This aligns with the Rust source where these are global factory properties.
 */
async function getSystemInfoForFactory(
  endpoint: string
): Promise<
  Pick<
    FactoryStateSnapshot,
    "canister_id" | "version" | "owner_id" | "host_url"
  >
> {
  // query factory_admins for owner_id
  const owner_id = (
    await db.queryFactory(`SELECT admin_user_id FROM factory_admins;`)
  )[0].admin_user_id;
  return {
    canister_id: "DEFAULT_FACTORY_CANISTER_ID" as ICPPrincipalString,
    version: "OfficeX.NodeJS.Alpha.0.0.1",
    owner_id: owner_id,
    host_url: endpoint as HostURL,
  };
}

/**
 * Helper function to get all API keys related state from the factory database.
 * The Rust source `APIKEYS_BY_HISTORY` and `APIKEYS_BY_ID_HASHTABLE` imply a global collection
 * of API keys managed by the factory.
 *
 * TODO: The Rust implementation for API key snapshot iterates over global hashtables.
 * The current `factory_api_keys` table in `schema_factory.sql` holds these.
 * This function correctly queries the *factory* database for all API keys, matching the Rust logic.
 */
async function getApiKeysState(): Promise<{
  apikeys_by_value: Record<ApiKeyValue, ApiKeyID>;
  apikeys_by_id: Record<ApiKeyID, ApiKey>;
  users_apikeys: Record<UserID, ApiKeyID[]>;
  apikeys_history: ApiKeyID[];
}> {
  // Querying the factory database for all API keys.
  const apiKeysResult = await db.queryFactory(
    `SELECT
        id,
        value,
        user_id,
        name,
        created_at,
        expires_at,
        is_revoked
      FROM factory_api_keys
      ORDER BY created_at ASC;` // Ordering for history
  );

  const apikeys_by_value: Record<ApiKeyValue, ApiKeyID> = {};
  const apikeys_by_id: Record<ApiKeyID, ApiKey> = {};
  const users_apikeys: Record<UserID, ApiKeyID[]> = {};
  const apikeys_history: ApiKeyID[] = [];

  for (const row of apiKeysResult) {
    const apiKey: ApiKey = {
      id: row.id as ApiKeyID,
      value: row.value as ApiKeyValue,
      user_id: row.user_id as UserID,
      name: row.name as string,
      created_at: row.created_at as number,
      expires_at: row.expires_at as number,
      is_revoked: Boolean(row.is_revoked),
      private_note: undefined, // Not in SQL schema, assuming optional
      begins_at: 0, // Not in SQL schema, assuming default
      labels: [], // Not in SQL schema, assuming default
      external_id: undefined, // Not in SQL schema, assuming optional
      external_payload: undefined, // Not in SQL schema, assuming optional
    };

    apikeys_by_value[apiKey.value] = apiKey.id;
    apikeys_by_id[apiKey.id] = apiKey;

    if (!users_apikeys[apiKey.user_id]) {
      users_apikeys[apiKey.user_id] = [];
    }
    users_apikeys[apiKey.user_id].push(apiKey.id);

    apikeys_history.push(apiKey.id);
  }

  return {
    apikeys_by_value,
    apikeys_by_id,
    users_apikeys,
    apikeys_history,
  };
}

/**
 * Helper function to get GiftcardSpawnOrg related state from the factory database.
 * This directly maps to the tables defined in `schema_factory.sql`.
 *
 * The Rust source uses `DEPLOYMENTS_BY_GIFTCARD_SPAWNORG_ID`, `HISTORICAL_GIFTCARDS_SPAWNORGS`,
 * `DRIVE_TO_GIFTCARD_SPAWNORG_HASHTABLE`, `USER_TO_GIFTCARDS_SPAWNORG_HASHTABLE`,
 * `GIFTCARD_SPAWNORG_BY_ID`.
 *
 * My interpretation of the SQL schema:
 * - `giftcard_spawn_orgs` maps to `GiftcardSpawnOrg` and contributes to `giftcard_by_id`, `historical_giftcards`.
 * - `factory_spawn_history` could represent `deployments_by_giftcard_id` if each deployment is linked to a giftcard.
 * - `user_giftcard_spawn_orgs` directly maps to `user_to_giftcards_hashtable`.
 * - `drive_to_giftcard_hashtable` is not explicitly a table in `schema_factory.sql`. It might be derived or
 * implicitly handled by `factory_spawn_history` if `drive_id` and `giftcard_id` are a unique mapping there.
 * I will derive this from `factory_spawn_history`.
 */
async function getGiftcardSpawnOrgState(): Promise<
  Pick<
    FactoryStateSnapshot,
    | "deployments_by_giftcard_id"
    | "historical_giftcards"
    | "drive_to_giftcard_hashtable"
    | "user_to_giftcards_hashtable"
    | "giftcard_by_id"
  >
> {
  // Fetch all GiftcardSpawnOrg records
  const giftcardsResult = await db.queryFactory(
    `SELECT
        id,
        usd_revenue_cents,
        note,
        gas_cycles_included,
        timestamp_ms,
        external_id,
        redeemed,
        disk_auth_json
      FROM giftcard_spawn_orgs
      ORDER BY timestamp_ms ASC;`
  );

  const giftcard_by_id: Record<GiftcardSpawnOrgID, GiftcardSpawnOrg> = {};
  const historical_giftcards: GiftcardSpawnOrgID[] = [];

  for (const row of giftcardsResult) {
    const giftcard: GiftcardSpawnOrg = {
      id: row.id as GiftcardSpawnOrgID,
      usd_revenue_cents: row.usd_revenue_cents as number,
      note: row.note as string,
      gas_cycles_included: row.gas_cycles_included as number,
      timestamp_ms: row.timestamp_ms as number,
      external_id: row.external_id as string,
      redeemed: Boolean(row.redeemed),
      disk_auth_json: row.disk_auth_json as string | undefined,
    };
    giftcard_by_id[giftcard.id] = giftcard;
    historical_giftcards.push(giftcard.id);
  }

  // Fetch factory_spawn_history to populate deployments_by_giftcard_id and drive_to_giftcard_hashtable
  const spawnHistoryResult = await db.queryFactory(
    `SELECT
        owner_id,
        drive_id,
        host,
        version,
        note,
        giftcard_id,
        gas_cycles_included,
        timestamp_ms
      FROM factory_spawn_history;`
  );

  const deployments_by_giftcard_id: Record<
    GiftcardSpawnOrgID,
    GiftcardSpawnOrg
  > = {};
  const drive_to_giftcard_hashtable: Record<DriveID, GiftcardSpawnOrgID> = {};

  for (const row of spawnHistoryResult) {
    const giftcardId = row.giftcard_id as GiftcardSpawnOrgID;
    const driveId = row.drive_id as DriveID;

    // Use the giftcard details fetched earlier from giftcard_spawn_orgs
    const associatedGiftcard = giftcard_by_id[giftcardId];
    if (associatedGiftcard) {
      deployments_by_giftcard_id[giftcardId] = associatedGiftcard;
      drive_to_giftcard_hashtable[driveId] = giftcardId;
    }
  }

  // Fetch user_giftcard_spawn_orgs for user_to_giftcards_hashtable
  const userGiftcardMappingsResult = await db.queryFactory(
    `SELECT user_id, giftcard_id FROM user_giftcard_spawn_orgs;`
  );

  const user_to_giftcards_hashtable: Record<UserID, GiftcardSpawnOrgID[]> = {};
  for (const row of userGiftcardMappingsResult) {
    const userId = row.user_id as UserID;
    const giftcardId = row.giftcard_id as GiftcardSpawnOrgID;
    if (!user_to_giftcards_hashtable[userId]) {
      user_to_giftcards_hashtable[userId] = [];
    }
    user_to_giftcards_hashtable[userId].push(giftcardId);
  }

  return {
    deployments_by_giftcard_id,
    historical_giftcards,
    drive_to_giftcard_hashtable,
    user_to_giftcards_hashtable,
    giftcard_by_id,
  };
}

/**
 * Generates a complete snapshot of the factory's state.
 * This function encapsulates all data retrieval logic for the snapshot.
 *
 * @param driveId The ID of the drive associated with the request (used for system info if drive-specific).
 * @returns A Promise that resolves to the FactoryStateSnapshot object.
 */
export async function getFactorySnapshot(
  endpoint: string
): Promise<FactoryStateSnapshot> {
  // Authorization check for local environment is skipped, as per Rust code's `if !is_local_environment()`
  // Assuming `authenticateRequest` and the caller of `getFactorySnapshot` handles local environment bypass.

  // Fetch system info for the *specified drive*.
  // This assumes the `about_drive` table exists in the drive's database.
  const factorySystemInfo = await getSystemInfoForFactory(endpoint);

  // Fetch API keys state (these are assumed to be factory-wide based on Rust's global hashtables)
  const { apikeys_by_value, apikeys_by_id, users_apikeys, apikeys_history } =
    await getApiKeysState(); // No driveId here, as API keys are factory-global in Rust source

  // Fetch GiftcardSpawnOrg state (these are assumed to be factory-wide based on Rust's global hashtables)
  const {
    deployments_by_giftcard_id,
    historical_giftcards,
    drive_to_giftcard_hashtable,
    user_to_giftcards_hashtable,
    giftcard_by_id,
  } = await getGiftcardSpawnOrgState(); // No driveId here, as gift cards are factory-global in Rust source

  // Create a snapshot of the entire state
  const stateSnapshot: FactoryStateSnapshot = {
    // System info (from the specific drive)
    canister_id: factorySystemInfo.canister_id,
    version: factorySystemInfo.version,
    owner_id: factorySystemInfo.owner_id,
    host_url: factorySystemInfo.host_url,

    // API keys state (from the factory database)
    apikeys_by_value: apikeys_by_value,
    apikeys_by_id: apikeys_by_id,
    users_apikeys: users_apikeys,
    apikeys_history: apikeys_history,

    // GiftcardSpawnOrg state (from the factory database)
    deployments_by_giftcard_id: deployments_by_giftcard_id,
    historical_giftcards: historical_giftcards,
    drive_to_giftcard_hashtable: drive_to_giftcard_hashtable,
    user_to_giftcards_hashtable: user_to_giftcards_hashtable,
    giftcard_by_id: giftcard_by_id,

    // Timestamp - Rust uses `ic_cdk::api::time()` which is nanoseconds.
    // Date.now() returns milliseconds, so convert to nanoseconds.
    timestamp_ms: (BigInt(Date.now()) * 1_000_000n).toString(),
  };

  return stateSnapshot;
}
