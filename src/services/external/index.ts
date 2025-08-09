// src/services/externalIdService.ts
import { ExternalID, DriveID } from "@officexapp/types";
import { dbHelpers } from "../database";

/**
 * Utility to convert string[] to JSON string.
 */
function internalIdsToJson(internalIds: string[]): string {
  return JSON.stringify(internalIds);
}

/**
 * Utility to parse JSON string back to string[].
 */
function jsonToInternalIds(jsonString: string): string[] {
  try {
    const items = JSON.parse(jsonString);
    if (
      Array.isArray(items) &&
      items.every((item) => typeof item === "string")
    ) {
      return items;
    }
    console.error("Parsed JSON is not an array of strings:", items);
    return [];
  } catch (error) {
    console.error("Failed to parse JSON to string[]:", error);
    return [];
  }
}

/**
 * Manages the mapping between external IDs and internal drive resource IDs.
 * Corresponds to the Rust `update_external_id_mapping` function and `EXTERNAL_ID_MAPPINGS` StableBTreeMap.
 *
 * @param driveId The ID of the drive to which these mappings belong.
 * @param oldExternalId The old external ID to remove mapping for (optional).
 * @param newExternalId The new external ID to add mapping for (optional).
 * @param internalId The internal ID (e.g., DriveID, FileID, FolderID, etc.) to map (required if old or new ID is present).
 */
export async function updateExternalIDMapping(
  driveId: DriveID,
  oldExternalId: ExternalID | undefined,
  newExternalId: ExternalID | undefined,
  internalId: string | undefined // This should be a general string as it can be any internal ID
): Promise<void> {
  if (!internalId) {
    console.warn("Cannot update external ID mapping without an internalId.");
    return;
  }

  await dbHelpers.transaction("drive", driveId, (db) => {
    // 1. Handle removal of old external ID mapping
    if (oldExternalId) {
      const selectOldSql = `SELECT internal_ids FROM external_id_mappings WHERE external_id = ?;`;
      const oldMapping = db.prepare(selectOldSql).get(oldExternalId) as {
        internal_ids: string;
      };

      if (oldMapping) {
        let currentInternalIds: string[] = jsonToInternalIds(
          oldMapping.internal_ids
        );
        currentInternalIds = currentInternalIds.filter(
          (id) => id !== internalId
        );

        if (currentInternalIds.length === 0) {
          // If the list is empty, remove the mapping entirely
          const deleteSql = `DELETE FROM external_id_mappings WHERE external_id = ?;`;
          db.prepare(deleteSql).run(oldExternalId);
        } else {
          // Update the existing mapping
          const updateSql = `UPDATE external_id_mappings SET internal_ids = ? WHERE external_id = ?;`;
          db.prepare(updateSql).run(
            internalIdsToJson(currentInternalIds),
            oldExternalId
          );
        }
      }
    }

    // 2. Handle adding new external ID mapping
    if (newExternalId) {
      const selectNewSql = `SELECT internal_ids FROM external_id_mappings WHERE external_id = ?;`;
      const newMapping = db.prepare(selectNewSql).get(newExternalId) as {
        internal_ids: string;
      };

      let newInternalIds: string[];
      if (newMapping) {
        newInternalIds = jsonToInternalIds(newMapping.internal_ids);
      } else {
        newInternalIds = [];
      }

      if (!newInternalIds.includes(internalId)) {
        newInternalIds.push(internalId);
        const insertOrUpdateSql = `INSERT INTO external_id_mappings (external_id, internal_ids) VALUES (?, ?)
                                    ON CONFLICT(external_id) DO UPDATE SET internal_ids = excluded.internal_ids;`;
        db.prepare(insertOrUpdateSql).run(
          newExternalId,
          internalIdsToJson(newInternalIds)
        );
        console.log(
          `Added/Updated external ID mapping for ${newExternalId} with ${internalId}`
        );
      } else {
        console.log(
          `Internal ID ${internalId} already exists for external ID ${newExternalId}. No update needed.`
        );
      }
    }
  });
}

/**
 * Retrieves internal IDs associated with a given external ID.
 * Corresponds to `EXTERNAL_ID_MAPPINGS.get()` operation.
 * @param driveId The ID of the drive.
 * @param externalId The external ID to look up.
 * @returns A string[] (list of internal IDs) or undefined if no mapping exists.
 */
export async function getInternalIDsForExternalID(
  driveId: DriveID,
  externalId: ExternalID
): Promise<string[] | undefined> {
  return dbHelpers.withDrive(driveId, (db) => {
    const sql = `SELECT internal_ids FROM external_id_mappings WHERE external_id = ?;`;
    const result = db.prepare(sql).get(externalId) as { internal_ids: string };
    return result ? jsonToInternalIds(result.internal_ids) : undefined;
  });
}

/**
 * Claims a UUID, marking it as used.
 * Corresponds to the Rust `UUID_CLAIMED` StableBTreeMap.
 * This is a simplified implementation assuming uniqueness is handled at a higher level
 * or that conflicts are rare for UUIDs. For true UUID uniqueness guarantees across a distributed system,
 * more robust mechanisms (e.g., a central UUID generation service) would be needed.
 * For local SQLite, this table prevents reuse.
 * @param driveId The ID of the drive.
 * @param uuid The UUID string to claim.
 * @returns True if the UUID was successfully claimed, false if it was already claimed.
 */
export async function claimUUID(
  driveId: DriveID,
  uuid: string
): Promise<boolean> {
  return dbHelpers.withDrive(driveId, (db) => {
    try {
      const sql = `INSERT INTO uuid_claimed (uuid, claimed) VALUES (?, 1);`;
      const info = db.prepare(sql).run(uuid);
      return info.changes > 0; // If changes > 0, it means a new row was inserted (claimed)
    } catch (error: any) {
      // Check if it's a unique constraint violation (SQLITE_CONSTRAINT_PRIMARYKEY = 19)
      if (error.code === "SQLITE_CONSTRAINT_PRIMARYKEY") {
        console.warn(`UUID '${uuid}' is already claimed.`);
        return false;
      }
      throw error; // Re-throw other errors
    }
  });
}

/**
 * Checks if a UUID has been claimed.
 * @param driveId The ID of the drive.
 * @param uuid The UUID string to check.
 * @returns True if the UUID is claimed, false otherwise.
 */
export async function isUUIDClaimed(
  driveId: DriveID,
  uuid: string
): Promise<boolean> {
  return dbHelpers.withDrive(driveId, (db) => {
    const sql = `SELECT claimed FROM uuid_claimed WHERE uuid = ?;`;
    const result = db.prepare(sql).get(uuid) as { claimed: number };
    return result ? result.claimed === 1 : false;
  });
}
