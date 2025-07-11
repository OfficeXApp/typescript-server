import { ApiResponse, DriveID, UserID } from "@officexapp/types";
import { db } from "../../services/database";

export interface OrgIdParams {
  org_id: string;
}

// Generic API Response Helper
export function createApiResponse<T>(
  data?: T,
  error?: { code: number; message: string }
): ApiResponse<T> {
  return {
    status: error ? "error" : "success",
    data,
    error,
    timestamp: Date.now(),
  };
}

// TODO: Replace with actual function to get the drive owner's UserID from the database
// This might involve querying the `about_drive` table for the current `DriveID`'s owner.
export async function getDriveOwnerId(orgId: DriveID): Promise<UserID> {
  try {
    const result = await db.queryDrive(
      orgId,
      "SELECT owner_id FROM about_drive LIMIT 1"
    );
    if (result.length > 0 && result[0].owner_id) {
      return result[0].owner_id as UserID;
    }
  } catch (error) {
    console.error("Error fetching drive owner ID:", error);
  }
  // Return a sensible placeholder or throw an error if owner ID cannot be determined.
  // For now, an educated guess is a default user ID.
  return "UserID_PLACEHOLDER_DRIVE_OWNER"; // TODO: Replace with actual owner ID from DB
}
