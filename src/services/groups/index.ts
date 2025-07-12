// src/services/groups.ts

import {
  UserID,
  GroupID,
  Group, // Assuming this is your TypeScript interface for Group
  GroupInvite, // Assuming this is your TypeScript interface for GroupInvite
  GroupInviteeID, // Assuming this is your TypeScript type for GroupInviteeID
  IDPrefixEnum,
  URLEndpoint,
  IResponseValidateGroupMember,
} from "@officexapp/types"; // Adjust this path to your actual types
import { db } from "../../services/database"; // Adjust this path to your database service
import { USER_ID_PREFIX } from "../permissions/directory"; // Import constants from permissions/directory

/**
 * Represents a Group record as stored in the SQLite database.
 * This should match the `groups` table schema.
 */
interface GroupDbRow {
  id: string; // GroupID
  name: string;
  owner_user_id: string; // UserID
  avatar?: string;
  private_note?: string;
  public_note?: string;
  created_at: number;
  last_modified_at: number;
  drive_id: string; // DriveID
  endpoint_url: string; // URLEndpoint
  external_id?: string;
  external_payload?: string;
  // Note: admin_invites and member_invites are typically stored in a separate join table or derived
  // For simplicity, we'll fetch them as needed or assume a denormalized JSON column if you added one.
}

/**
 * Represents a GroupInvite record as stored in the SQLite database.
 * This should match the `group_invites` table schema.
 */
interface GroupInviteDbRow {
  id: string; // GroupInviteID
  group_id: string; // GroupID
  inviter_user_id: string; // UserID
  invitee_type: string; // 'USER', 'PLACEHOLDER', 'PUBLIC'
  invitee_id?: string; // UserID or PlaceholderID, NULL if public
  role: string; // 'ADMIN', 'MEMBER'
  note: string;
  active_from: number;
  expires_at: number;
  created_at: number;
  last_modified_at: number;
  redeem_code?: string;
  from_placeholder_invitee?: string;
  external_id?: string;
  external_payload?: string;
}

/**
 * Retrieves a group by its ID.
 * @param groupId The ID of the group.
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns The Group object if found, otherwise undefined.
 */
export async function getGroupById(
  groupId: GroupID,
  orgId: string
): Promise<Group | undefined> {
  const query = `
      SELECT
        id, name, owner_user_id, avatar, private_note, public_note,
        created_at, last_modified_at, drive_id, endpoint_url,
        external_id, external_payload
      FROM groups
      WHERE id = ?;
    `;
  const rows = await db.queryDrive(orgId, query, [groupId]);

  if (rows.length === 0) {
    return undefined;
  }

  const row = rows[0] as GroupDbRow;

  // For `admin_invites` and `member_invites`, you'd typically fetch from `group_invites` table
  // and filter by group_id and role. For this example, we'll keep it simple as empty arrays
  // or add a more complete fetching if needed.
  // In a real scenario, you might have `GROUP_CONCAT` for invite IDs or separate queries.
  const adminInvites: string[] = []; // Placeholder
  const memberInvites: string[] = []; // Placeholder

  return {
    id: row.id,
    name: row.name,
    owner: row.owner_user_id,
    avatar: row.avatar || "",
    private_note: row.private_note,
    public_note: row.public_note,
    admin_invites: adminInvites, // This would need actual data fetching if needed for the Group object itself
    member_invites: memberInvites, // This would need actual data fetching if needed for the Group object itself
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    drive_id: row.drive_id,
    endpoint_url: row.endpoint_url as URLEndpoint,
    labels: [], // Labels are handled via a join table, fetch separately if needed
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}

/**
 * Checks if a user is an admin of a specific group.
 * This function largely mirrors the `is_group_admin` in Rust.
 * @param userId The ID of the user.
 * @param groupId The ID of the group.
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns True if the user is an admin, false otherwise.
 */
export async function isGroupAdmin(
  userId: UserID,
  groupId: GroupID,
  orgId: string
): Promise<boolean> {
  const group = await getGroupById(groupId, orgId);
  if (!group) {
    return false; // Group not found
  }

  // 1. Check if user is the owner of the group
  if (group.owner === userId) {
    return true;
  }

  // 2. Check admin invites
  const currentTime = Date.now();
  const adminInviteQuery = `
      SELECT
        gi.id, gi.group_id, gi.inviter_user_id, gi.invitee_type, gi.invitee_id,
        gi.role, gi.note, gi.active_from, gi.expires_at,
        gi.created_at, gi.last_modified_at, gi.redeem_code,
        gi.from_placeholder_invitee, gi.external_id, gi.external_payload
      FROM group_invites gi
      WHERE gi.group_id = ?
        AND gi.role = 'ADMIN'
        AND gi.invitee_type = 'USER'
        AND gi.invitee_id = ?;
    `;
  const adminInviteRows = await db.queryDrive(orgId, adminInviteQuery, [
    groupId,
    userId.replace(USER_ID_PREFIX, ""), // Extract raw user ID if prefixed for DB storage
  ]);

  for (const row of adminInviteRows) {
    const invite = row as GroupInviteDbRow;
    if (
      invite.active_from <= currentTime &&
      (invite.expires_at <= 0 || invite.expires_at > currentTime)
    ) {
      return true;
    }
  }

  return false;
}

/**
 * Checks if a user is a member of a local group. This includes both admins and regular members.
 * This function largely mirrors the `is_user_on_local_group` in Rust.
 *
 * IMPORTANT: This function assumes the group is "local" (i.e., on the same drive).
 * It does NOT make HTTP calls to external drives.
 * @param userId The ID of the user.
 * @param group The Group object to check membership against.
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns True if the user is a member of the group, false otherwise.
 */
export async function isUserOnLocalGroup(
  userId: UserID,
  group: Group,
  orgId: string
): Promise<boolean> {
  // 1. Check if user is the owner
  if (group.owner === userId) {
    return true;
  }

  // 2. Check all member invites (which include admin invites in Rust)
  const currentTime = Date.now();

  // Fetch all invites for this user that are active and associated with this group
  // Rust: `USERS_INVITES_LIST_HASHTABLE` maps `GroupInviteeID` to `GroupInviteIDList`
  // and `INVITES_BY_ID_HASHTABLE` for invite details.
  // We need to query `group_invites` table.
  const userInvitesQuery = `
      SELECT
        gi.id, gi.group_id, gi.inviter_user_id, gi.invitee_type, gi.invitee_id,
        gi.role, gi.note, gi.active_from, gi.expires_at,
        gi.created_at, gi.last_modified_at, gi.redeem_code,
        gi.from_placeholder_invitee, gi.external_id, gi.external_payload
      FROM group_invites gi
      WHERE gi.group_id = ?
        AND gi.invitee_type = 'USER'
        AND gi.invitee_id = ?;
    `;

  const inviteRows = await db.queryDrive(orgId, userInvitesQuery, [
    group.id,
    userId.replace(USER_ID_PREFIX, ""), // Extract raw user ID if prefixed for DB storage
  ]);

  for (const row of inviteRows) {
    const invite = row as GroupInviteDbRow;
    if (
      invite.active_from <= currentTime &&
      (invite.expires_at <= 0 || invite.expires_at > currentTime)
    ) {
      return true;
    }
  }

  return false;
}

/**
 * Checks if a user is a member of any group, including local and potentially external groups.
 * This function largely mirrors the `is_user_on_group` in Rust.
 * @param userId The ID of the user.
 * @param groupId The ID of the group.
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns True if the user is a member of the group, false otherwise.
 */
export async function isUserInGroup(
  userId: UserID,
  groupId: GroupID,
  orgId: string
): Promise<boolean> {
  const group = await getGroupById(groupId, orgId);
  if (!group) {
    return false; // Group not found
  }

  // Rust's `URL_ENDPOINT.with(|url| url.borrow().get().clone())` is your current drive's endpoint.
  // You'll need to pass or retrieve your local drive's URL endpoint to compare.
  // For simplicity, let's assume getDriveOwnerId provides enough info for local checks,
  // or you have a way to get the local drive's URL.
  // For this example, I'll mock `getLocalDriveEndpointUrl`. In your real app,
  // this should come from your `about_drive` table or a configuration.
  // Or even better, if `orgId` *is* the local drive ID, then any group fetched from `db.queryDrive(orgId, ...)`
  // is inherently a "local" group.
  const localDriveInfo = await db.queryDrive(
    orgId,
    `SELECT url_endpoint FROM about_drive LIMIT 1;`
  );
  const localDriveEndpoint =
    localDriveInfo.length > 0 ? localDriveInfo[0].url_endpoint : "";

  if (group.endpoint_url === localDriveEndpoint) {
    // If it's our own drive's group, use local validation
    return isUserOnLocalGroup(userId, group, orgId);
  } else {
    // It's an external group, make HTTP call to their validate endpoint
    // This part requires an HTTP client and handling external API calls.
    // The Rust code uses `ic_cdk::api::management_canister::http_request`.
    // In Node.js, you'd use `axios` or `fetch`.

    const validationUrl = `${group.endpoint_url.replace(/\/+$/, "")}/groups/validate`; // Remove trailing slash

    try {
      const response = await fetch(validationUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          group_id: groupId, // Rust sends groupId.0, assuming raw string
          user_id: userId, // Rust sends userId.0, assuming raw string
        }),
      });

      if (!response.ok) {
        console.error(
          `External group validation failed with status: ${response.status}`
        );
        return false;
      }

      const result =
        (await response.json()) as unknown as IResponseValidateGroupMember;
      // Assuming ValidateGroupResponseData from Rust which has `is_member: boolean`
      return result.ok.data.is_member === true; // Ensure it's explicitly true
    } catch (e) {
      console.error(`External group validation request failed: ${e}`);
      return false;
    }
  }
}

// Helper to extract plain ID from prefixed UserID string for DB queries
function extractPlainUserId(prefixedUserId: UserID): string {
  if (prefixedUserId.startsWith(IDPrefixEnum.User)) {
    return prefixedUserId.substring(IDPrefixEnum.User.length);
  }
  return prefixedUserId; // Return as is if no prefix, or handle error
}
