// src/services/snapshot/drive.ts

import {
  DriveID,
  ICPPrincipalString,
  URLEndpoint,
  UserID,
  ExternalID,
  ApiKeyID,
  ApiKeyValue,
  ApiKey,
  Contact,
  FileID,
  FileRecord,
  FolderID,
  FolderRecord,
  DiskID,
  Disk,
  Drive,
  DirectoryPermissionID,
  DirectoryResourceID,
  DirectoryPermission,
  SystemPermissionID,
  SystemResourceID,
  SystemPermission,
  GroupInviteID,
  GroupInvite,
  GroupID,
  Group,
  WebhookID,
  Webhook,
  LabelValue,
  IFactorySpawnHistoryRecord,
  GranteeID,
  GroupInviteeID, // Import GroupInviteeID for more specific typing
} from "@officexapp/types";
import { dbHelpers } from "../database"; // Assuming dbHelpers is correctly implemented
import { SystemTableValueEnum, SystemRecordIDEnum } from "@officexapp/types"; // Adjust path if needed

/**
 * Interface representing the entire state snapshot of a Drive, mirroring the Rust `EntireState` struct.
 * Note: `HashMap` in Rust often maps to `Record<string, T>` or `Map<string, T>` in TypeScript.
 * `Vec<T>` in Rust maps to `T[]` in TypeScript.
 * `u64`, `u128` are `bigint` in TypeScript for large numbers, or `number` if within safe integer limits.
 * We'll represent them as `string` for consistency with how IDs are handled.
 */
export interface DriveStateSnapshot {
  // About
  DRIVE_ID: DriveID;
  CANISTER_ID: ICPPrincipalString;
  VERSION: string;
  OWNER_ID: UserID;
  URL_ENDPOINT: URLEndpoint;
  DRIVE_STATE_TIMESTAMP_NS: string; // Rust's u64 is bigint, represent as string
  EXTERNAL_ID_MAPPINGS: Record<ExternalID, string[]>; // Maps ExternalID to Vec<String>
  RECENT_DEPLOYMENTS: IFactorySpawnHistoryRecord[]; // Vec<IFactorySpawnHistoryRecord>
  SPAWN_REDEEM_CODE: string; // SpawnRedeemCode(string)
  SPAWN_NOTE: string;
  NONCE_UUID_GENERATED: string; // u128, represent as string
  UUID_CLAIMED: Record<string, boolean>; // HashMap<String, bool>

  // Api Keys
  APIKEYS_BY_VALUE_HASHTABLE: Record<ApiKeyValue, ApiKeyID>;
  APIKEYS_BY_ID_HASHTABLE: Record<ApiKeyID, ApiKey>;
  USERS_APIKEYS_HASHTABLE: Record<UserID, ApiKeyID[]>; // ApiKeyIDList { keys: Vec<ApiKeyID> }

  // Contacts
  CONTACTS_BY_ID_HASHTABLE: Record<UserID, Contact>;
  CONTACTS_BY_ICP_PRINCIPAL_HASHTABLE: Record<ICPPrincipalString, UserID>;
  CONTACTS_BY_TIME_LIST: UserID[];
  HISTORY_SUPERSWAP_USERID: Record<UserID, UserID>;

  // Directory
  folder_uuid_to_metadata: Record<FolderID, FolderRecord>;
  file_uuid_to_metadata: Record<FileID, FileRecord>;
  full_folder_path_to_uuid: Record<string, FolderID>; // DriveFullFilePath (string) to FolderID
  full_file_path_to_uuid: Record<string, FileID>; // DriveFullFilePath (string) to FileID

  // Disks
  DISKS_BY_ID_HASHTABLE: Record<DiskID, Disk>;
  DISKS_BY_TIME_LIST: DiskID[];

  // Drives
  DRIVES_BY_ID_HASHTABLE: Record<DriveID, Drive>;
  DRIVES_BY_TIME_LIST: DriveID[];

  // Permissions
  DIRECTORY_PERMISSIONS_BY_ID_HASHTABLE: Record<
    DirectoryPermissionID,
    DirectoryPermission
  >;
  DIRECTORY_PERMISSIONS_BY_RESOURCE_HASHTABLE: Record<
    DirectoryResourceID,
    DirectoryPermissionID[]
  >;
  DIRECTORY_GRANTEE_PERMISSIONS_HASHTABLE: Record<
    GranteeID,
    DirectoryPermissionID[]
  >;
  DIRECTORY_PERMISSIONS_BY_TIME_LIST: DirectoryPermissionID[];
  SYSTEM_PERMISSIONS_BY_ID_HASHTABLE: Record<
    SystemPermissionID,
    SystemPermission
  >;
  SYSTEM_PERMISSIONS_BY_RESOURCE_HASHTABLE: Record<
    SystemResourceID,
    SystemPermissionID[]
  >;
  SYSTEM_GRANTEE_PERMISSIONS_HASHTABLE: Record<GranteeID, SystemPermissionID[]>;
  SYSTEM_PERMISSIONS_BY_TIME_LIST: SystemPermissionID[];

  // Group Invites
  INVITES_BY_ID_HASHTABLE: Record<GroupInviteID, GroupInvite>;
  // This now needs to use GroupInviteeID as key, not just UserID,
  // to correctly mirror the Rust HashMap<GroupInviteeID, Vec<GroupInviteID>>
  USERS_INVITES_LIST_HASHTABLE: Record<GroupInviteeID, GroupInviteID[]>;

  // Groups
  GROUPS_BY_ID_HASHTABLE: Record<GroupID, Group>;
  GROUPS_BY_TIME_LIST: GroupID[];

  // Webhooks
  WEBHOOKS_BY_ALT_INDEX_HASHTABLE: Record<string, WebhookID[]>; // WebhookAltIndexID (string) to Vec<WebhookID>
  WEBHOOKS_BY_ID_HASHTABLE: Record<WebhookID, Webhook>;
  WEBHOOKS_BY_TIME_LIST: WebhookID[];
}

/**
 * Retrieves a complete snapshot of the drive's state from the database.
 * This function mirrors the `snapshot_entire_state` function from the Rust codebase,
 * fetching data from SQLite and transforming it into the `DriveStateSnapshot` structure.
 * @param driveId The ID of the drive to snapshot.
 * @returns A promise that resolves to the `DriveStateSnapshot` object.
 */
export async function getDriveSnapshot(
  driveId: DriveID
): Promise<DriveStateSnapshot> {
  // Use dbHelpers.withDrive to ensure the correct database is used and managed.
  return dbHelpers.withDrive(driveId, (database) => {
    // Helper function to fetch all rows and convert to a Map/Record
    const fetchAllToRecord = <T>(
      tableName: string,
      idColumn: string
    ): Record<string, T> => {
      const stmt = database.prepare(`SELECT * FROM ${tableName}`);
      const rows = stmt.all() as T[];
      return rows.reduce(
        (acc: Record<string, T>, row: T) => {
          // Ensure that the ID column exists and is a string
          const id = (row as any)[idColumn];
          if (typeof id === "string") {
            acc[id] = row;
          } else {
            // Handle cases where the ID might need conversion (e.g., from Buffer/Blob to string)
            acc[String(id)] = row;
          }
          return acc;
        },
        {} as Record<string, T>
      );
    };

    // Helper function to fetch a single row by ID and return it
    const fetchOneById = <T>(
      tableName: string,
      idColumn: string,
      idValue: string
    ): T | undefined => {
      const stmt = database.prepare(
        `SELECT * FROM ${tableName} WHERE ${idColumn} = ?`
      );
      return stmt.get(idValue) as T | undefined;
    };

    // Helper function to fetch a single value from a table
    const fetchSingleValue = (tableName: string, columnName: string): any => {
      const stmt = database.prepare(
        `SELECT ${columnName} FROM ${tableName} LIMIT 1`
      );
      const result = stmt.get() as Record<string, any>;
      return result ? result[columnName] : undefined;
    };

    // Helper function to fetch a list of IDs from a junction table or a single-column table
    const fetchIdList = (tableName: string, idColumn: string): string[] => {
      const stmt = database.prepare(`SELECT ${idColumn} FROM ${tableName}`);
      return (stmt.all() as any[]).map((row) => row[idColumn].toString());
    };

    // --- About Section ---
    // These values are typically stored in the `about_drive` table in your schema.
    const aboutDrive = fetchOneById<{
      drive_id: DriveID;
      drive_name: string;
      canister_id: ICPPrincipalString;
      version: string;
      drive_state_checksum: string;
      timestamp_ns: string;
      owner_id: UserID;
      url_endpoint: URLEndpoint;
      transfer_owner_id: UserID;
      spawn_redeem_code: string;
      spawn_note: string;
      nonce_uuid_generated: number;
    }>("about_drive", "drive_id", driveId);

    if (!aboutDrive) {
      throw new Error(`Drive with ID ${driveId} not found.`);
    }

    // `EXTERNAL_ID_MAPPINGS` (HashMap<ExternalID, Vec<String>>) from `external_id_mappings` table
    const externalIdMappingsRows = database
      .prepare(`SELECT external_id, internal_ids FROM external_id_mappings`)
      .all() as { external_id: string; internal_ids: string }[]; // internal_ids is JSON string
    const EXTERNAL_ID_MAPPINGS: Record<ExternalID, string[]> =
      externalIdMappingsRows.reduce(
        (
          acc: Record<ExternalID, string[]>,
          row: { external_id: string; internal_ids: string }
        ) => {
          try {
            // Assuming internal_ids is a JSON string of a string array, e.g., '["id1", "id2"]'
            acc[row.external_id] = JSON.parse(row.internal_ids);
          } catch (e) {
            console.error(
              `Failed to parse internal_ids for ${row.external_id}:`,
              e
            );
            acc[row.external_id] = [];
          }
          return acc;
        },
        {} as Record<ExternalID, string[]>
      );

    // `RECENT_DEPLOYMENTS` (Vec<IFactorySpawnHistoryRecord>)
    // This isn't directly in the provided SQL schema, it's a Rust `StableVec`.
    // In TS, if it's stored, it would likely be another table or a JSON blob.
    // For now, mocking as an empty array or you'd need a `factory_spawn_history` table.
    const RECENT_DEPLOYMENTS: IFactorySpawnHistoryRecord[] = []; // TODO: Implement fetching from database if a table is created for it.

    // `UUID_CLAIMED` (HashMap<String, bool>) from `uuid_claimed` table
    const uuidClaimedRows = database
      .prepare(`SELECT uuid, claimed FROM uuid_claimed`)
      .all() as { uuid: string; claimed: number }[];
    const UUID_CLAIMED: Record<string, boolean> = uuidClaimedRows.reduce(
      (
        acc: Record<string, boolean>,
        row: { uuid: string; claimed: number }
      ) => {
        // Explicitly type acc and row
        acc[row.uuid] = row.claimed === 1;
        return acc;
      },
      {} as Record<string, boolean>
    );

    // --- API Keys ---
    const APIKEYS_BY_ID_HASHTABLE = fetchAllToRecord<ApiKey>("api_keys", "id");
    const APIKEYS_BY_VALUE_HASHTABLE: Record<ApiKeyValue, ApiKeyID> =
      Object.values(APIKEYS_BY_ID_HASHTABLE).reduce(
        (acc: Record<ApiKeyValue, ApiKeyID>, apiKey: ApiKey) => {
          acc[apiKey.value] = apiKey.id;
          return acc;
        },
        {} as Record<ApiKeyValue, ApiKeyID>
      );

    // `USERS_APIKEYS_HASHTABLE` (HashMap<UserID, ApiKeyIDList>)
    // This is a mapping of UserID to a list of ApiKeyIDs.
    // In SQL, this is typically represented via a foreign key in `api_keys` back to `contacts` (users).
    // We need to group API keys by user_id.
    const USERS_APIKEYS_HASHTABLE: Record<UserID, ApiKeyID[]> = Object.values(
      APIKEYS_BY_ID_HASHTABLE
    ).reduce(
      (acc: Record<UserID, ApiKeyID[]>, apiKey: ApiKey) => {
        if (!acc[apiKey.user_id]) {
          acc[apiKey.user_id] = [];
        }
        acc[apiKey.user_id].push(apiKey.id);
        return acc;
      },
      {} as Record<UserID, ApiKeyID[]>
    );

    // --- Contacts ---
    const CONTACTS_BY_ID_HASHTABLE = fetchAllToRecord<Contact>(
      "contacts",
      "id"
    );
    const CONTACTS_BY_ICP_PRINCIPAL_HASHTABLE: Record<
      ICPPrincipalString,
      UserID
    > = Object.values(CONTACTS_BY_ID_HASHTABLE).reduce(
      (acc: Record<ICPPrincipalString, UserID>, contact: Contact) => {
        acc[contact.icp_principal] = contact.id;
        return acc;
      },
      {} as Record<ICPPrincipalString, UserID>
    );

    const CONTACTS_BY_TIME_LIST = fetchIdList(
      "contacts ORDER BY created_at ASC",
      "id"
    );

    // `HISTORY_SUPERSWAP_USERID` (HashMap<UserID, UserID>) from `contact_id_superswap_history` table
    const historySuperswapRows = database
      .prepare(
        `SELECT old_user_id, new_user_id FROM contact_id_superswap_history`
      )
      .all() as { old_user_id: string; new_user_id: string }[];
    const HISTORY_SUPERSWAP_USERID: Record<UserID, UserID> =
      historySuperswapRows.reduce(
        (
          acc: Record<UserID, UserID>,
          row: { old_user_id: string; new_user_id: string }
        ) => {
          // Explicitly type acc and row
          acc[row.old_user_id] = row.new_user_id;
          return acc;
        },
        {} as Record<UserID, UserID>
      );

    // --- Directory ---
    const folder_uuid_to_metadata = fetchAllToRecord<FolderRecord>(
      "folders",
      "id"
    );
    const file_uuid_to_metadata = fetchAllToRecord<FileRecord>("files", "id");

    const full_folder_path_to_uuid = database
      .prepare(`SELECT full_directory_path, id FROM folders`)
      .all()
      .reduce(
        (
          acc: any, // Record<string, FolderID>,
          row: any // { full_directory_path: string; id: FolderID }
        ) => {
          acc[row.full_directory_path] = row.id;
          return acc;
        },
        {} as Record<string, FolderID>
      ) as Record<string, FolderID>;

    const full_file_path_to_uuid = database
      .prepare(`SELECT full_directory_path, id FROM files`)
      .all()
      .reduce(
        (
          acc: any, // Record<string, FileID>,
          row: any // { full_directory_path: string; id: FileID }
        ) => {
          acc[row.full_directory_path] = row.id;
          return acc;
        },
        {} as Record<string, FileID>
      ) as Record<string, FileID>;

    // --- Disks ---
    const DISKS_BY_ID_HASHTABLE = fetchAllToRecord<Disk>("disks", "id");
    const DISKS_BY_TIME_LIST = fetchIdList(
      "disks ORDER BY created_at ASC",
      "id"
    );

    // --- Drives ---
    const DRIVES_BY_ID_HASHTABLE = fetchAllToRecord<Drive>("drives", "id");
    const DRIVES_BY_TIME_LIST = fetchIdList(
      "drives ORDER BY created_at ASC",
      "id"
    );

    // --- Permissions ---
    const DIRECTORY_PERMISSIONS_BY_ID_HASHTABLE =
      fetchAllToRecord<DirectoryPermission>("permissions_directory", "id");
    const SYSTEM_PERMISSIONS_BY_ID_HASHTABLE =
      fetchAllToRecord<SystemPermission>("permissions_system", "id");

    // DIRECTORY_PERMISSIONS_BY_RESOURCE_HASHTABLE (HashMap<DirectoryResourceID, Vec<DirectoryPermissionID>>)
    const DIRECTORY_PERMISSIONS_BY_RESOURCE_HASHTABLE: Record<
      DirectoryResourceID,
      DirectoryPermissionID[]
    > = Object.values(DIRECTORY_PERMISSIONS_BY_ID_HASHTABLE).reduce(
      (
        acc: Record<DirectoryResourceID, DirectoryPermissionID[]>,
        perm: DirectoryPermission
      ) => {
        if (!acc[perm.resource_id]) {
          acc[perm.resource_id] = [];
        }
        acc[perm.resource_id].push(perm.id);
        return acc;
      },
      {} as Record<DirectoryResourceID, DirectoryPermissionID[]>
    );

    // DIRECTORY_GRANTEE_PERMISSIONS_HASHTABLE (HashMap<GranteeID, Vec<DirectoryPermissionID>>)
    const DIRECTORY_GRANTEE_PERMISSIONS_HASHTABLE: Record<
      GranteeID,
      DirectoryPermissionID[]
    > = Object.values(DIRECTORY_PERMISSIONS_BY_ID_HASHTABLE).reduce(
      (
        acc: Record<GranteeID, DirectoryPermissionID[]>,
        perm: DirectoryPermission
      ) => {
        // In Rust, GranteeID is an enum, which might serialize into a more complex string.
        // Assuming `granted_to` directly maps to `GranteeID` string representation.
        const granteeId = perm.granted_to; // `granted_to` is already GranteeID type from your types
        if (!acc[granteeId]) {
          acc[granteeId] = [];
        }
        acc[granteeId].push(perm.id);
        return acc;
      },
      {} as Record<GranteeID, DirectoryPermissionID[]>
    );

    const DIRECTORY_PERMISSIONS_BY_TIME_LIST = fetchIdList(
      "permissions_directory ORDER BY created_at ASC",
      "id"
    );

    // SYSTEM_PERMISSIONS_BY_RESOURCE_HASHTABLE (HashMap<SystemResourceID, Vec<SystemPermissionID>>)
    const SYSTEM_PERMISSIONS_BY_RESOURCE_HASHTABLE: Record<
      SystemResourceID,
      SystemPermissionID[]
    > = Object.values(SYSTEM_PERMISSIONS_BY_ID_HASHTABLE).reduce(
      (
        acc: Record<SystemResourceID, SystemPermissionID[]>,
        perm: SystemPermission
      ) => {
        // Rust's SystemResourceID is an enum (Table or Record). We need to reconstruct the string representation.
        let resourceKey: SystemResourceID;
        // The `resource_id` property on `SystemPermission` is already `SystemResourceID`
        // which is a union type `SystemTableResource | SystemRecordResource`.
        // So direct assignment is fine if the types are consistent.
        resourceKey = perm.resource_id;

        if (!acc[resourceKey]) {
          acc[resourceKey] = [];
        }
        acc[resourceKey].push(perm.id);
        return acc;
      },
      {} as Record<SystemResourceID, SystemPermissionID[]>
    );

    // SYSTEM_GRANTEE_PERMISSIONS_HASHTABLE (HashMap<GranteeID, Vec<SystemPermissionID>>)
    const SYSTEM_GRANTEE_PERMISSIONS_HASHTABLE: Record<
      GranteeID,
      SystemPermissionID[]
    > = Object.values(SYSTEM_PERMISSIONS_BY_ID_HASHTABLE).reduce(
      (
        acc: Record<GranteeID, SystemPermissionID[]>,
        perm: SystemPermission
      ) => {
        const granteeId = perm.granted_to; // `granted_to` is already GranteeID type
        if (!acc[granteeId]) {
          acc[granteeId] = [];
        }
        acc[granteeId].push(perm.id);
        return acc;
      },
      {} as Record<GranteeID, SystemPermissionID[]>
    );

    const SYSTEM_PERMISSIONS_BY_TIME_LIST = fetchIdList(
      "permissions_system ORDER BY created_at ASC",
      "id"
    );

    // --- Group Invites ---
    // For Group Invites, we need to fetch the `invitee_type` explicitly from the DB.
    // The `GroupInvite` type in TS does not have `invitee_type`, but the SQL schema does.
    interface GroupInviteWithInviteeType extends GroupInvite {
      invitee_type: "USER" | "PLACEHOLDER_GROUP_INVITEE" | "PUBLIC";
    }

    const INVITES_BY_ID_HASHTABLE =
      fetchAllToRecord<GroupInviteWithInviteeType>("group_invites", "id");

    // USERS_INVITES_LIST_HASHTABLE (HashMap<GroupInviteeID, Vec<GroupInviteID>>)
    const USERS_INVITES_LIST_HASHTABLE: Record<
      GroupInviteeID,
      GroupInviteID[]
    > = Object.values(INVITES_BY_ID_HASHTABLE).reduce(
      (
        acc: Record<GroupInviteeID, GroupInviteID[]>,
        invite: GroupInviteWithInviteeType
      ) => {
        let inviteeKey: GroupInviteeID;

        // Determine the correct key based on `invitee_type` from the DB row.
        if (invite.invitee_type === "PUBLIC") {
          inviteeKey = "PUBLIC";
        } else if (invite.invitee_type === "USER") {
          inviteeKey = invite.invitee_id; // This will be a UserID
        } else if (invite.invitee_type === "PLACEHOLDER_GROUP_INVITEE") {
          inviteeKey = invite.invitee_id; // This will be a PlaceholderGroupInviteeID
        } else {
          // Fallback for any unexpected invitee_type, though ideally it should match the enum.
          console.warn(
            `Unexpected invitee_type for invite ${invite.id}: ${invite.invitee_type}`
          );
          inviteeKey = invite.invitee_id; // Use invitee_id as a fallback
        }

        if (!acc[inviteeKey]) {
          acc[inviteeKey] = [];
        }
        acc[inviteeKey].push(invite.id);
        return acc;
      },
      {} as Record<GroupInviteeID, GroupInviteID[]>
    );

    // --- Groups ---
    const GROUPS_BY_ID_HASHTABLE = fetchAllToRecord<Group>("groups", "id");
    const GROUPS_BY_TIME_LIST = fetchIdList(
      "groups ORDER BY created_at ASC",
      "id"
    );

    // --- Webhooks ---
    const WEBHOOKS_BY_ID_HASHTABLE = fetchAllToRecord<Webhook>(
      "webhooks",
      "id"
    );

    // WEBHOOKS_BY_ALT_INDEX_HASHTABLE (HashMap<WebhookAltIndexID, Vec<WebhookID>>)
    // Rust had `WebhookAltIndexID(string)` and `value.webhooks` (Vec<WebhookID>).
    // SQL has `webhooks.alt_index`.
    const WEBHOOKS_BY_ALT_INDEX_HASHTABLE: Record<string, WebhookID[]> =
      Object.values(WEBHOOKS_BY_ID_HASHTABLE).reduce(
        (acc: Record<string, WebhookID[]>, webhook: Webhook) => {
          const altIndex = webhook.alt_index;
          if (!acc[altIndex]) {
            acc[altIndex] = [];
          }
          acc[altIndex].push(webhook.id);
          return acc;
        },
        {} as Record<string, WebhookID[]>
      );

    const WEBHOOKS_BY_TIME_LIST = fetchIdList(
      "webhooks ORDER BY created_at ASC",
      "id"
    );

    return {
      DRIVE_ID: aboutDrive.drive_id,
      CANISTER_ID: aboutDrive.canister_id,
      VERSION: aboutDrive.version,
      OWNER_ID: aboutDrive.owner_id,
      URL_ENDPOINT: aboutDrive.url_endpoint,
      DRIVE_STATE_TIMESTAMP_NS: aboutDrive.timestamp_ns,
      EXTERNAL_ID_MAPPINGS,
      RECENT_DEPLOYMENTS, // MOCKED
      SPAWN_REDEEM_CODE: aboutDrive.spawn_redeem_code,
      SPAWN_NOTE: aboutDrive.spawn_note,
      NONCE_UUID_GENERATED: String(aboutDrive.nonce_uuid_generated), // u128 to string
      UUID_CLAIMED,

      APIKEYS_BY_VALUE_HASHTABLE,
      APIKEYS_BY_ID_HASHTABLE,
      USERS_APIKEYS_HASHTABLE,

      CONTACTS_BY_ID_HASHTABLE,
      CONTACTS_BY_ICP_PRINCIPAL_HASHTABLE,
      CONTACTS_BY_TIME_LIST,
      HISTORY_SUPERSWAP_USERID,

      folder_uuid_to_metadata,
      file_uuid_to_metadata,
      full_folder_path_to_uuid,
      full_file_path_to_uuid,

      DISKS_BY_ID_HASHTABLE,
      DISKS_BY_TIME_LIST,

      DRIVES_BY_ID_HASHTABLE,
      DRIVES_BY_TIME_LIST,

      DIRECTORY_PERMISSIONS_BY_ID_HASHTABLE,
      DIRECTORY_PERMISSIONS_BY_RESOURCE_HASHTABLE,
      DIRECTORY_GRANTEE_PERMISSIONS_HASHTABLE,
      DIRECTORY_PERMISSIONS_BY_TIME_LIST,
      SYSTEM_PERMISSIONS_BY_ID_HASHTABLE,
      SYSTEM_PERMISSIONS_BY_RESOURCE_HASHTABLE,
      SYSTEM_GRANTEE_PERMISSIONS_HASHTABLE,
      SYSTEM_PERMISSIONS_BY_TIME_LIST,

      INVITES_BY_ID_HASHTABLE,
      USERS_INVITES_LIST_HASHTABLE,

      GROUPS_BY_ID_HASHTABLE,
      GROUPS_BY_TIME_LIST,

      WEBHOOKS_BY_ALT_INDEX_HASHTABLE,
      WEBHOOKS_BY_ID_HASHTABLE,
      WEBHOOKS_BY_TIME_LIST,
    };
  });
}

// TODO: The following functions correspond to the Rust `convert_state_to_serializable` and `snapshot_entire_state`
// they are extracted from the Rust logic to ensure that `snapshot_entire_state` only concerns
// itself with fetching the entire snapshot data and the handler formats it.
// The Rust `snapshot_entire_state` actually does the data fetching and transformation.
// The TypeScript `getDriveSnapshot` combines these for simplicity, as `convert_state_to_serializable`
// in Rust was primarily for JSON serialization differences between Rust's internal types and output.
// In TypeScript, we fetch and directly map to the desired JSON structure.

// Below are the placeholder helper functions for various data categories.
// They mimic the structure of the Rust state accessors and `convert_state_to_serializable`.
// For simplicity and directness with SQLite, many of these will now be combined or handled within `getDriveSnapshot`.
// However, if your application architecture dictates, you can separate these into distinct service functions.

// Example of a helper function for 'About' section, if needed outside `getDriveSnapshot`
export async function getDriveAbout(driveId: DriveID): Promise<{
  drive_id: DriveID;
  drive_name: string;
  canister_id: ICPPrincipalString;
  version: string;
  drive_state_checksum: string;
  timestamp_ns: string;
  owner_id: UserID;
  url_endpoint: URLEndpoint;
  transfer_owner_id: UserID;
  spawn_redeem_code: string;
  spawn_note: string;
  nonce_uuid_generated: number;
}> {
  return dbHelpers.withDrive(driveId, (database) => {
    const stmt = database.prepare(`
                SELECT
                    drive_id,
                    drive_name,
                    canister_id,
                    version,
                    drive_state_checksum,
                    timestamp_ns,
                    owner_id,
                    url_endpoint,
                    transfer_owner_id,
                    spawn_redeem_code,
                    spawn_note,
                    nonce_uuid_generated
                FROM about_drive
                WHERE drive_id = ?
            `);
    const result = stmt.get(driveId) as {
      drive_id: DriveID;
      drive_name: string;
      canister_id: ICPPrincipalString;
      version: string;
      drive_state_checksum: string;
      timestamp_ns: string;
      owner_id: UserID;
      url_endpoint: URLEndpoint;
      transfer_owner_id: UserID;
      spawn_redeem_code: string;
      spawn_note: string;
      nonce_uuid_generated: number;
    };
    if (!result) {
      throw new Error(`About data for drive ${driveId} not found.`);
    }
    return result;
  });
}

// TODO: Implement other specific getters if they are needed as public service functions
// outside of the full snapshot (e.g., if other handlers need just contacts, or just files)

export async function getDriveExternalIdMappings(
  driveId: DriveID
): Promise<Record<ExternalID, string[]>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database
      .prepare(`SELECT external_id, internal_ids FROM external_id_mappings`)
      .all() as { external_id: string; internal_ids: string }[];
    return rows.reduce(
      (
        acc: Record<ExternalID, string[]>,
        row: { external_id: string; internal_ids: string }
      ) => {
        try {
          acc[row.external_id] = JSON.parse(row.internal_ids);
        } catch (e) {
          console.error(
            `Failed to parse internal_ids for ${row.external_id}:`,
            e
          );
          acc[row.external_id] = [];
        }
        return acc;
      },
      {} as Record<ExternalID, string[]>
    );
  });
}

export async function getDriveRecentDeployments(
  driveId: DriveID
): Promise<IFactorySpawnHistoryRecord[]> {
  // This data is not present in the provided SQL schema.
  // If it were in SQLite, you'd fetch it from a dedicated table.
  // For now, return an empty array or implement based on actual storage.
  console.warn(
    `getDriveRecentDeployments for drive ${driveId} is a TODO: Data not in SQL schema.`
  );
  return []; // Placeholder
}

export async function getDriveClaimedUuids(
  driveId: DriveID
): Promise<Record<string, boolean>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database
      .prepare(`SELECT uuid, claimed FROM uuid_claimed`)
      .all() as { uuid: string; claimed: number }[];
    return rows.reduce(
      (
        acc: Record<string, boolean>,
        row: { uuid: string; claimed: number }
      ) => {
        acc[row.uuid] = row.claimed === 1;
        return acc;
      },
      {} as Record<string, boolean>
    );
  });
}

export async function getDriveUsersApiKeys(
  driveId: DriveID
): Promise<Record<UserID, ApiKeyID[]>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database.prepare(`SELECT user_id, id FROM api_keys`).all() as {
      user_id: string;
      id: string;
    }[];
    return rows.reduce(
      (
        acc: Record<UserID, ApiKeyID[]>,
        row: { user_id: string; id: string }
      ) => {
        if (!acc[row.user_id]) {
          acc[row.user_id] = [];
        }
        acc[row.user_id].push(row.id);
        return acc;
      },
      {} as Record<UserID, ApiKeyID[]>
    );
  });
}

export async function getDriveContacts(
  driveId: DriveID
): Promise<Record<UserID, Contact>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database.prepare(`SELECT * FROM contacts`).all() as Contact[];
    return rows.reduce(
      (acc: Record<UserID, Contact>, contact: Contact) => {
        acc[contact.id] = contact;
        return acc;
      },
      {} as Record<UserID, Contact>
    );
  });
}

export async function getDriveSuperswapHistory(
  driveId: DriveID
): Promise<Record<UserID, UserID>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database
      .prepare(
        `SELECT old_user_id, new_user_id FROM contact_id_superswap_history`
      )
      .all() as { old_user_id: string; new_user_id: string }[];
    return rows.reduce(
      (
        acc: Record<UserID, UserID>,
        row: { old_user_id: string; new_user_id: string }
      ) => {
        acc[row.old_user_id] = row.new_user_id;
        return acc;
      },
      {} as Record<UserID, UserID>
    );
  });
}

export async function getDriveFolders(
  driveId: DriveID
): Promise<Record<FolderID, FolderRecord>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database
      .prepare(`SELECT * FROM folders`)
      .all() as FolderRecord[];
    return rows.reduce(
      (acc: Record<FolderID, FolderRecord>, folder: FolderRecord) => {
        acc[folder.id] = folder;
        return acc;
      },
      {} as Record<FolderID, FolderRecord>
    );
  });
}

export async function getDriveFiles(
  driveId: DriveID
): Promise<Record<FileID, FileRecord>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database.prepare(`SELECT * FROM files`).all() as FileRecord[];
    return rows.reduce(
      (acc: Record<FileID, FileRecord>, file: FileRecord) => {
        acc[file.id] = file;
        return acc;
      },
      {} as Record<FileID, FileRecord>
    );
  });
}

export async function getDriveDisks(
  driveId: DriveID
): Promise<Record<DiskID, Disk>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database.prepare(`SELECT * FROM disks`).all() as Disk[];
    return rows.reduce(
      (acc: Record<DiskID, Disk>, disk: Disk) => {
        acc[disk.id] = disk;
        return acc;
      },
      {} as Record<DiskID, Disk>
    );
  });
}

export async function getDriveDrives(
  driveId: DriveID
): Promise<Record<DriveID, Drive>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database.prepare(`SELECT * FROM drives`).all() as Drive[];
    return rows.reduce(
      (acc: Record<DriveID, Drive>, drive: Drive) => {
        acc[drive.id] = drive;
        return acc;
      },
      {} as Record<DriveID, Drive>
    );
  });
}

export async function getDriveDirectoryPermissions(
  driveId: DriveID
): Promise<Record<DirectoryPermissionID, DirectoryPermission>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database
      .prepare(`SELECT * FROM permissions_directory`)
      .all() as DirectoryPermission[];
    return rows.reduce(
      (
        acc: Record<DirectoryPermissionID, DirectoryPermission>,
        perm: DirectoryPermission
      ) => {
        acc[perm.id] = perm;
        return acc;
      },
      {} as Record<DirectoryPermissionID, DirectoryPermission>
    );
  });
}

export async function getDriveSystemPermissions(
  driveId: DriveID
): Promise<Record<SystemPermissionID, SystemPermission>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database
      .prepare(`SELECT * FROM permissions_system`)
      .all() as SystemPermission[];
    return rows.reduce(
      (
        acc: Record<SystemPermissionID, SystemPermission>,
        perm: SystemPermission
      ) => {
        acc[perm.id] = perm;
        return acc;
      },
      {} as Record<SystemPermissionID, SystemPermission>
    );
  });
}

export async function getDriveUsersInvites(
  driveId: DriveID
): Promise<Record<GroupInviteeID, GroupInviteID[]>> {
  // Changed return type to GroupInviteeID
  return dbHelpers.withDrive(driveId, (database) => {
    // Select `invitee_id` AND `invitee_type` to correctly determine the key.
    const rows = database
      .prepare(`SELECT invitee_id, invitee_type, id FROM group_invites`)
      .all() as {
      invitee_id: GroupInviteeID;
      invitee_type: "USER" | "PLACEHOLDER" | "PUBLIC";
      id: GroupInviteID;
    }[];

    return rows.reduce(
      (acc: Record<GroupInviteeID, GroupInviteID[]>, row) => {
        let inviteeKey: GroupInviteeID;

        // Determine the correct key based on `invitee_type` from the DB row.
        if (row.invitee_type === "PUBLIC") {
          inviteeKey = "PUBLIC";
        } else if (row.invitee_type === "USER") {
          inviteeKey = row.invitee_id; // This will be a UserID
        } else if (row.invitee_type === "PLACEHOLDER") {
          // Assuming "PLACEHOLDER_GROUP_INVITEE" is stored as "PLACEHOLDER"
          inviteeKey = row.invitee_id; // This will be a PlaceholderGroupInviteeID
        } else {
          // Fallback for any unexpected invitee_type, though ideally it should match the enum.
          console.warn(
            `Unexpected invitee_type for invite ${row.id}: ${row.invitee_type}`
          );
          inviteeKey = row.invitee_id; // Use invitee_id as a fallback
        }

        if (!acc[inviteeKey]) {
          acc[inviteeKey] = [];
        }
        acc[inviteeKey].push(row.id);
        return acc;
      },
      {} as Record<GroupInviteeID, GroupInviteID[]> // Explicitly type the initial accumulator
    );
  });
}

export async function getDriveGroups(
  driveId: DriveID
): Promise<Record<GroupID, Group>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database.prepare(`SELECT * FROM groups`).all() as Group[];
    return rows.reduce(
      (acc: Record<GroupID, Group>, group: Group) => {
        acc[group.id] = group;
        return acc;
      },
      {} as Record<GroupID, Group>
    );
  });
}

export async function getDriveWebhooks(
  driveId: DriveID
): Promise<Record<WebhookID, Webhook>> {
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database.prepare(`SELECT * FROM webhooks`).all() as Webhook[];
    return rows.reduce(
      (acc: Record<WebhookID, Webhook>, webhook: Webhook) => {
        acc[webhook.id] = webhook;
        return acc;
      },
      {} as Record<WebhookID, Webhook>
    );
  });
}

export async function getDriveLabels(
  driveId: DriveID
): Promise<Record<string, LabelValue[]>> {
  // This data structure is tricky. Rust's `LabelValue` is a string.
  // The `labels` table stores `id` and `value`.
  // The junction tables (`api_key_labels`, `contact_labels`, etc.) link resources to label IDs.
  // The Rust snapshot converts these to a `Vec<LabelValue>`, which implies a list of label values directly on the resource.
  // We need to fetch labels for each resource. This is usually done when fetching the resource itself.
  // The `EntireState` structure has `labels: LabelValue[]` for individual resources like `FileRecord`, `FolderRecord`, etc.
  // The top-level `labels` object in `EntireState` is not explicitly shown in Rust, but it is in the `convert_state_to_serializable`
  // function which iterates over `state.LABELS_BY_ID_HASHTABLE` (not present in the provided Rust state).
  // Assuming `getDriveSnapshot` will pull `labels` data and attach it to the respective records.
  // For `convert_state_to_serializable` and `snapshot_entire_state` in Rust, `labels` seem to refer to the Label structs themselves, not just their values.
  // If `Labels` is meant to be a hashmap of all available labels:
  return dbHelpers.withDrive(driveId, (database) => {
    const rows = database.prepare(`SELECT id, value FROM labels`).all() as {
      id: string;
      value: string;
    }[];
    return rows.reduce(
      (
        acc: Record<string, LabelValue[]>,
        row: { id: string; value: string }
      ) => {
        // The Rust `EntireState` doesn't have a top-level `LABELS_BY_ID_HASHTABLE` but `convert_state_to_serializable` suggests iterating it.
        // We'll mimic the output of `convert_state_to_serializable` which produced `HashMap<String, Value>` for label content.
        // So here, we map label ID to its value.
        acc[row.id] = [row.value]; // Wrap in an array to match `LabelValue[]`
        return acc;
      },
      {} as Record<string, LabelValue[]>
    );
  });
}
