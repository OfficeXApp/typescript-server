// src/services/externalIdService.ts
import { ExternalID, DriveID } from "@officexapp/types";
import { dbHelpers } from "../database";
import { Database } from "better-sqlite3";

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
export function claimUUID(database: Database, uuid: string): boolean {
  try {
    const sql = `INSERT INTO uuid_claimed (uuid, claimed) VALUES (?, 1);`;
    const info = database.prepare(sql).run(uuid);
    return info.changes > 0; // If changes > 0, it means a new row was inserted (claimed)
  } catch (error: any) {
    // Check if it's a unique constraint violation (SQLITE_CONSTRAINT_PRIMARYKEY = 19)
    if (error.code === "SQLITE_CONSTRAINT_PRIMARYKEY") {
      throw new Error(`UUID '${uuid}' is already claimed.`);
    }
    throw error; // Re-throw other errors
  }
}

/**
 * Checks if a UUID has been claimed.
 * @param driveId The ID of the drive.
 * @param uuid The UUID string to check.
 * @returns True if the UUID is claimed, false otherwise.
 */
export async function isUUIDClaimed(
  uuid: string,
  driveId: DriveID
): Promise<boolean> {
  return dbHelpers.withDrive(driveId, (db) => {
    const sql = `SELECT claimed FROM uuid_claimed WHERE uuid = ?;`;
    const result = db.prepare(sql).get(uuid) as { claimed: number };
    return result ? result.claimed === 1 : false;
  });
}
