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
  // throw an error if none
  throw new Error("Owner ID not found for drive");
}
